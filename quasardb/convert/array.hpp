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
#include "../masked_array.hpp"
#include "../numpy.hpp"
#include "../traits.hpp"
#include "../utils.hpp"
#include "range.hpp"
#include "util.hpp"
#include "value.hpp"
#include <qdb/ts.h>
#include <pybind11/pybind11.h>
#include <range/v3/algorithm/max_element.hpp>
#include <range/v3/range/conversion.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/view/common.hpp>
#include <range/v3/view/transform.hpp>
#include <range/v3/view/zip.hpp>
#include <cstring>
#include <utf8.h> // utf-cpp

namespace qdb::convert::detail
{

namespace py  = pybind11;
namespace utf = utf8::unchecked;

////////////////////////////////////////////////////////////////////////////////
//
// ARRAY CONVERTERS
//
///////////////////
//
// These converters operate on arrays of any type. For simplicity sake, their
// interface assumes the destination is a pre-allocated pointer. Their purpose
// is to make use of the fastest numpy API available to handle high-perf
// array conversion.
//
////////////////////////////////////////////////////////////////////////////////

template <typename From, typename To>
struct convert_array;

/////
//
//  numpy->qdb
// "Regular" transforms
//
// Input:  range of length N, dtype: any fixed-width dtype
// Output: range of length N, type:  qdb_primitive
//
/////
template <typename From, typename To>
requires(concepts::dtype<From> && !concepts::delegate_dtype<From> && concepts::qdb_primitive<To>) struct
    convert_array<From, To>
{
    using value_type = typename From::value_type;
    static constexpr value_converter<From, To> const xform_{};

    [[nodiscard]] constexpr inline auto operator()() const noexcept
    {
        return ranges::views::transform(xform_);
    };
};

/////
//
// numpy->qdb
// "Delegate" transforms
//
// In some cases numpy's representations are more rich than what Quasardb's primitives
// support, e.g. int32. In this case, we first transform this to the "delegate type",
// int64, which can then figure out the rest.
//
// Input: range of length N, type: fixed-width ("regular") numpy data
// Output: range of length N, type: qdb primitives
//
/////
template <typename From, typename To>
requires(concepts::delegate_dtype<From> && concepts::qdb_primitive<To>) struct convert_array<From, To>
{
    // Source value_type, e.g. std::int32_t
    using value_type = typename From::value_type;

    // Delegate dtype, e.g. traits::int64_dtype
    using Delegate = typename From::delegate_type;

    // Delegate value_type, e.g. std::int64_t
    using delegate_value_type = typename Delegate::value_type;

    static constexpr convert_array<Delegate, To> const delegate{};

    [[nodiscard]] constexpr inline auto operator()() const noexcept
    {
        auto xform = [](value_type const & x) -> delegate_value_type {
            if (From::is_null(x))
            {
                return Delegate::null_value();
            }
            else
            {
                return static_cast<delegate_value_type>(x);
            }
        };
        return ranges::views::transform(xform) | delegate();
    };
};

/////
//
// qdb->numpy
// "Regular" transforms
//
// Input:  range of length N, type: qdb_primitive
// Output: range of length N, dtype: To
//
/////
template <typename From, typename To>
requires(
    concepts::qdb_primitive<From> &&
        concepts::fixed_width_dtype<To> && !concepts::delegate_dtype<To>) struct convert_array<From, To>
{
    static constexpr value_converter<From, To> const xform_{};

    template <typename Rng>
    [[nodiscard]] inline auto operator()(Rng && xs) const noexcept
    {
        assert(ranges::empty(xs) == false);
        return xs | ranges::views::transform(xform_);
    };
};

/////
//
// qdb->numpy
// Variable-width transforms
//
// Returns a range that is backed by a single, large memory arena and chopped
// in pieces.
//
// Input:  range of length N, type: qdb_string or qdb_blob
// Output: range of length N, dtype: To
//
/////

template <typename From, typename To>
requires(
    concepts::qdb_primitive<From> && concepts::variable_width_dtype<To>) struct convert_array<From, To>
{
    using in_char_type  = qdb_char_type;
    using out_char_type = std::u32string::value_type;

    /**
     * range<qdb_string_t> -> range<qdb_string_view>
     */
    template <concepts::input_range_t<qdb_string_t> Rng>
    [[nodiscard]] inline auto operator()(Rng && xs) const noexcept
    {
        static constexpr value_converter<From, qdb_string_view> const xform_{};
        return operator()(ranges::views::transform(std::move(xs), xform_));
    }

    /**
     * range<qdb_string_view> -> range<chunk_view<out_char_type>>
     */
    template <concepts::input_range_t<qdb_string_view> Rng>
    [[nodiscard]] inline auto operator()(Rng && xs) const noexcept
    {
        assert(ranges::empty(xs) == false);

        // What we have as input data is an array of just QuasarDB native objects, i.e.
        // qdb_string_t or qdb_blob_t.
        //
        // We must transform this into a single, contiguous buffer, and convert it
        // into a different representation in the process.
        //
        // The flow of the code below is as follows:
        //  - determine the size & allocate the destination buffer in one big swoop;
        //  - expose this buffer as a range with subranges of strides;
        //  - merge the input qdb_string_t into this output range, while transcoding
        //    it from UTF8 to UTF32.

        ////
        // Step 1: allocate one big bad buffer
        ////
        std::size_t stride_size{_largest_word_length(xs)};
        py::array::ShapeContainer shape{ranges::size(xs)};
        py::array::StridesContainer strides{To::itemsize(stride_size)};

        py::array arr{To::dtype(stride_size), shape, strides};

        ////
        // Step 2: expose this buffer as a range
        ////
        auto output = _stride_array_view(arr);
        assert(ranges::size(output) == ranges::size(xs));
        //

        ////
        // Step 3: transcode + copy into output
        //
        // The approach we take here is:
        //  1. zip the input and output ranges, so that we have the source qdb_primitive
        //    right next to the memory area it needs to be written into
        //  2. feed all this through a transform function
        //  3. profit
        ////
        auto xform_and_store = [=, *this](auto && x) {
            auto in  = ranges::views::common(std::get<0>(x));
            auto out = std::get<1>(x);

            utf::utf8to32(ranges::begin(in), ranges::end(in), ranges::begin(out));
            return out;
        };

        // Create a view that aligns our input qdb_string_t next to the data it needs
        // to write into.
        auto output_ = ranges::zip_view(std::move(xs), std::move(output))
                       | ranges::views::transform(std::move(xform_and_store));

        // And last but not least: allow piggybacking our `py::array` onto the range
        // so that we can access it again later.
        return qdb::convert::detail::passenger_view(std::move(output_), std::move(arr));
    };

private:
    inline auto _stride_array_view(py::array xs) const noexcept
    {
        py::ssize_t stride_size = To::stride_size(xs.itemsize());
        out_char_type * ptr     = xs.mutable_unchecked<out_char_type>().mutable_data();
        return ranges::chunk_view(ranges::views::counted(ptr, stride_size * xs.size()), stride_size);
    }

    /**
     * Returns the length of the largest word in a range. Length is in bytes,
     * not codepoints.
     *
     * Smallest value returned is 1.
     */
    template <typename R>
    inline std::size_t _largest_word_length(R const & xs) const noexcept
    {

        // Transform into a range of sizes
        auto xs_ = xs | ranges::views::transform([](auto const & x) -> std::size_t {
            return ranges::size(x);
        });

        // Return the element with the largest size
        auto iter = ranges::max_element(xs_);

        return std::max(*iter, std::size_t(1));
    };
};

}; // namespace qdb::convert::detail

namespace qdb::convert
{

namespace py = pybind11;

////////////////////////////////////////////////////////////////////////////////
//
// PUBLIC API
//
///////////////////
//
// Functions below define the public API. Their intent to handle boilerplate and
// and easier-to-use interface than the lower-level converters defined above.
//
////////////////////////////////////////////////////////////////////////////////

// numpy -> qdb
// input:  np.ndarray
// output: OutputIterator
template <concepts::dtype From, concepts::qdb_primitive To, ranges::output_iterator<To> OutputIterator>
static inline constexpr void array(py::array const & xs, OutputIterator dst)
{
    if (xs.size() == 0) [[unlikely]]
    {
        return;
    };
    ranges::copy(detail::to_range<From>(xs) | detail::convert_array<From, To>{}(), dst);
}

// numpy -> qdb
// input:  np.ndarray
// output: OutputRange
template <concepts::dtype From, concepts::qdb_primitive To>
static inline constexpr void array(py::array const & xs, std::vector<To> & dst)
{
    dst.resize(xs.size()); // <- important!
    array<From, To>(xs, ranges::begin(dst));
}

// numpy -> qdb
// input:  np.ndarray
// returns: vector
template <concepts::dtype From, concepts::qdb_primitive To>
static inline constexpr std::vector<To> array(py::array const & xs)
{
    if (xs.size() == 0)
    {
        return {};
    };

    return ranges::to<std::vector>(detail::to_range<From>(xs) | detail::convert_array<From, To>{}());
}

// numpy -> qdb
template <concepts::dtype From, concepts::qdb_primitive To>
static inline constexpr void masked_array(qdb::masked_array const & xs, std::vector<To> & dst)
{
    array<From, To>(xs.filled<From>(), dst);
}

// numpy -> qdb
template <concepts::dtype From, concepts::qdb_primitive To>
static inline constexpr std::vector<To> masked_array(qdb::masked_array const & xs)
{
    return array<From, To>(xs.filled<From>());
}

}; // namespace qdb::convert
