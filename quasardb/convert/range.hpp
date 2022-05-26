/*
 *
 * Official Python API
 *
 * Copyright (c) 2009-2022, quasardb SAS. All rights reserved.
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
#include <range/v3/all.hpp>

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
    using value_type = typename DType::value_type;

    value_type const * xs_ = xs.unchecked<value_type>().data();

    return ranges::views::counted(xs_, xs.size());
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
requires(concepts::fixed_width_dtype<DType>) inline py::array to_array(R const & xs)
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
template <concepts::dtype DType, ranges::input_range R>
requires(concepts::variable_width_dtype<DType>) inline py::array to_array(R const & xs)
{
    assert(ranges::size(xs) > 0);

    // Use the first element to determine the stride size
    using value_type = typename DType::value_type;
    auto head        = *(ranges::cbegin(xs));

    // Ensure we have e.g. a range of u32char for unicode, u8char for bytestring, etc.
    static_assert(concepts::input_range_t<decltype(head), value_type>);

    std::size_t codepoints_per_item = ranges::size(head);
    assert(codepoints_per_item > 0);

    // We're playing a bit of a trick here: rather than iterating over the range,
    // we just steal the pointer of the first item (which is also the beginning of
    // the entire array), and we're not evaluating anything else.
    //
    // Because of range views' lazy behavior, this means that remaining view
    // adapters / transform in the pipeline are not realized.
    //
    // As such, this check right here is not just for show, it ensures
    // that we're actually _consuming_ the whole range.
    //
    // But it's also just a good check that ensures all our strides are actually
    // the same size. :)

    bool all_equal = true;
    for (auto x : xs)
    {
        // Branchless, because we expect absolutely no `false` here ever, and
        // this means it can be vectorized.
        all_equal = all_equal && (ranges::size(x) == codepoints_per_item);
    };

    if (all_equal == false) [[unlikely]]
    {
        throw qdb::internal_local_exception{
            "Internal error: array strides are not of equal lengths: codepoints_per_item: "
            + std::to_string(codepoints_per_item)};
    };

    py::array::ShapeContainer shape{ranges::size(xs)};
    py::array::StridesContainer strides{DType::itemsize(codepoints_per_item)};
    //    py::dtype dt("<U18");
    return py::array{DType::dtype(codepoints_per_item), shape, strides, ranges::cdata(head)};
};

}; // namespace qdb::convert::detail
