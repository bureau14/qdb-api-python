/*
 *
 * Official Python API
 *
 * Copyright (c) 2009-2023, quasardb SAS. All rights reserved.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *    * Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above copyright
 *      notice, this list of conditions and the following disclaimer in the
 *      documentation and/or other materials provided with the distribution.
 *    * Neither the name of quasardb nor the names of its contributors may
 *      be used to endorse or promote products derived from this software
 *      without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY QUASARDB AND CONTRIBUTORS ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE REGENTS AND CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
#pragma once

#include "../concepts.hpp"
#include "../error.hpp"
#include "unicode.hpp"
#include "util.hpp"
#include <range/v3/algorithm/all_of.hpp>
#include <range/v3/algorithm/copy.hpp>
#include <range/v3/algorithm/find.hpp>
#include <range/v3/algorithm/for_each.hpp>
#include <range/v3/algorithm/max_element.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/view/chunk.hpp>
#include <range/v3/view/counted.hpp>
#include <range/v3/view/remove_if.hpp>
#include <range/v3/view/stride.hpp>
#include <range/v3/view/transform.hpp>

namespace qdb::convert::detail
{

namespace py = pybind11;

template <concepts::dtype DType>
requires(concepts::variable_width_dtype<DType>) struct clean_stride
{
    using stride_type = typename DType::stride_type;      // e.g. std::u32string
    using value_type  = typename stride_type::value_type; // e.g. wchar_t

    static constexpr value_type const null_value_ = DType::null_value();

    template <concepts::input_range_t<value_type> InputRange>
    requires(ranges::sized_range<InputRange>) inline decltype(auto) operator()(
        InputRange && stride) const noexcept
    {

        auto first = ranges::begin(stride);
        auto last  = ranges::find(stride, null_value_);

        using iterator_type = decltype(first);
        using sentinel_type = decltype(last);

        // There were some template deduction issues in this function for the
        // subrange<> call below, these static assertions are here to make this a bit
        // easier to deal with

        static_assert(ranges::input_iterator<iterator_type>);
        static_assert(ranges::sized_sentinel_for<sentinel_type, iterator_type>);

        return ranges::subrange<iterator_type, sentinel_type, ranges::subrange_kind::sized>(
            first, last);
    };
};

template <concepts::dtype DType>
requires(concepts::fixed_width_dtype<DType>) inline decltype(auto) to_range(py::array const & xs)
{
    // Lowest-level codepoint representation inside numpy, e.g. wchar_t for unicode
    // or short for int16.
    using value_type       = typename DType::value_type;
    value_type const * xs_ = xs.unchecked<value_type>().data();

    // Numpy can sometimes use larger strides, e.g. pack int64 in a container with
    // 128-byte strides. In these case, we need to increase the size of our step.
    //
    // Related ticket: SC-11057
    py::ssize_t stride_size{0};
    switch (xs.ndim())
    {
        // This can happen in case an array contains only a single number, then it will
        // not have any dimensions. In this case, it's best to just use the itemsize as
        // the stride size, because we'll not have to forward the iterator anyway.
        [[unlikely]] case 0 : stride_size = xs.itemsize();
        break;

        // Default case: use stride size of the first (and only) dimension. Most of the
        //               time this will be identical to the itemsize.
        [[likely]] case 1 : stride_size = xs.strides(0);
        break;
    default:
        throw qdb::incompatible_type_exception{
            "Multi-dimensional arrays are not supported. Eexpected 0 or 1 dimensions, got: "
            + std::to_string(xs.ndim())};
    };

    assert(stride_size > 0);

    py::ssize_t item_size = xs.itemsize();

    // Sanity check; stride_size is number of bytes per item for a whole "step", item_size
    // is the number of bytes per item. As such, stride_size should always be a multiple
    // of item_size.
    assert(stride_size % item_size == 0);

    // The number of "steps" of <value_type> we need to take per iteration.
    py::ssize_t step_size = stride_size / item_size;

    return ranges::views::stride(ranges::views::counted(xs_, (xs.size() * step_size)), step_size);
};

// Variable length encoding: split into chunks of <itemsize() / codepoint_size>
template <concepts::dtype DType>
requires(concepts::variable_width_dtype<DType>) inline decltype(auto) to_range(py::array const & xs)
{
    using stride_type = typename DType::stride_type;
    using value_type  = typename stride_type::value_type;

    // (Note: a "code point" is a single character as defined by unicode. For
    //  example, in UTF-16, each code point is 2 bytes).
    //
    // For numpy's variable-length encoding, numpy arrays use the following values:
    //
    // xs.size     -> the total number of items
    // xs.itemsize -> the number of bytes of each item
    //
    // Numpy encodes, say, unicode as a single continuous range of wchar_t. For
    // example, given the three unicode words "yes", "no", and "wombat", the encoding
    // may look as follows (X -> string value, 0 -> zero padding)
    //
    // longest_word:   6           (wombat)
    // stride_type:    u32string_t
    // stride_size:    4           (sizeof(stride_type::value_type))
    // => itemsize:    24          (longest_word * stride_size)
    // => size:        3           (amount of items)
    // => stride_size: 6           (amount of characters in longest word)
    //
    // Encoded, this will then look like this:
    //
    // [XXX000XX0000XXXXXX]
    //  yes   no    wombat
    //
    // What we will do below is to convert this single, continuous range of data
    // to a range of strings. We do that by:
    //
    //  1. Converting the continuous numpy array to a continuous range
    //  2. Split this up in strides of `stride_size`; our range thus becomes
    //     a range of subranges that all point to the start/stop of each stride
    //  3. Clear any null padding bytes on the right.
    //
    // A "stride" is defined as just a range, i.e. a pair of start/end iterator.
    //
    // No memory is copied, the emitted data still refers to the same numpy array
    // data under-the-hood.
    py::ssize_t stride_size = DType::stride_size(xs.itemsize());

    // First. let's gather a view of "all" bytes in the dataset: xs.size() repres
    auto all_bytes = ranges::views::counted(xs.unchecked<value_type>().data(), xs.size() * stride_size);

    // Now, "split" these in strides
    auto strides = ranges::chunk_view(all_bytes, stride_size);

    return ranges::views::transform(strides, clean_stride<DType>{});
};

/**
 * Converts range R to np.ndarray of dtype DType. Copies underlying data.
 */
template <concepts::dtype DType, ranges::input_range R>
requires(concepts::fixed_width_dtype<DType>) inline py::array to_array(R && xs)
{
    using value_type = typename DType::value_type;

    py::ssize_t size = ranges::size(xs);

    py::array ret(DType::dtype(), size);

    ranges::copy(xs, ret.mutable_unchecked<value_type>().mutable_data());

    return ret;
};

/**
 * Converts range R to np.ndarray of dtype DType. Copies underlying data.
 */
template <concepts::dtype Dtype, ranges::input_range R>
requires(concepts::variable_width_dtype<Dtype>) inline py::array to_array(R && xs)
{
    using out_char_type = typename Dtype::value_type;

    //
    // Our input range is a view of words with varying lengths, which may
    // have a complicated range view pipeline which does unicode conversions
    // and whatnot.
    //
    // In order to encode this data as a numpy contiguous, variable width
    // array (i.e. of dtype('U') or dtype('S')), we need to:
    //
    ////
    //  1. know the length of the longest word:
    //
    std::size_t stride_size = largest_word_length(xs);

    ////
    //  2. know the size (in bytes) of a single character;
    //
    py::array::StridesContainer strides{Dtype::itemsize(stride_size)};

    ////
    //  3. know the total number of words;
    //
    py::array::ShapeContainer shape{ranges::size(xs)};

    ////
    //  4. allocate a single, contiguous array of [1] * [2] * [3] bytes;
    //
    py::array arr{Dtype::dtype(stride_size), shape, strides};
    assert(Dtype::stride_size(arr.itemsize()) == stride_size);

    ////
    //  5. expose this contiguous array as chunks of `stride_size`
    //
    out_char_type * ptr = arr.mutable_unchecked<out_char_type>().mutable_data();
    auto dst =
        ranges::views::counted(ptr, stride_size * arr.size()) | ranges::views::chunk(stride_size);
    assert(ranges::size(dst) == ranges::size(xs));

    ////
    // 6. zip in and out ranges
    //
    // This will align each input stride (input range of length [0...stride_size>) next to
    // the output stride (output range of fixed size `stride_size`.
    //
    auto inout = ranges::zip_view(std::move(xs), std::move(dst));

    ////
    // 7. skip null values
    //
    // It's now safe to skip any null values
    auto inout_ = ranges::views::remove_if(
        std::move(inout), [](auto const & x) -> bool { return ranges::empty(std::get<0>(x)); });

    ////
    // 7. write the results into each stride.
    //
    ranges::for_each(inout_, [stride_size](auto && x) -> void {
        auto && [in, out] = x;

        static_assert(ranges::input_range<decltype(in)>);
        static_assert(ranges::output_range<decltype(out), out_char_type>);

        // assert(ranges::size(out) == stride_size);
        // assert(ranges::size(in) <= ranges::size(out));
        // assert(ranges::empty(in) == false);

        ranges::copy(in, ranges::begin(out));
    });

    return arr;
};

}; // namespace qdb::convert::detail
