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

namespace qdb::convert::detail
{

namespace py = pybind11;

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
requires(concepts::qdb_primitive<From> && !concepts::delegate_dtype<To>) struct convert_array<From, To>
{
    static constexpr value_converter<From, To> const xform_{};

    [[nodiscard]] inline auto operator()() const noexcept
    {
        return ranges::views::transform(xform_);
    };
};

/////
//
// qdb->numpy
// "Delegate" transforms
//
// In some cases numpy's representations are more rich than what Quasardb's primitives
// support, e.g. int32. In this case, we first transform this to the "delegate type",
// int64, which can then figure out the rest.
//
// Input:  range of length N, type: qdb_primitive
// Output: range of length N, dtype: To
//
/////
template <typename From, typename To>
requires(concepts::qdb_primitive<From> && concepts::delegate_dtype<To>) struct convert_array<From, To>
{
    // Destination value_type, e.g. std::int32_t
    using value_type = typename To::value_type;

    // Delegate dtype, e.g. traits::int64_dtype
    using Delegate = typename To::delegate_type;

    // Delegate value_type, e.g. std::int64_t
    using delegate_value_type = typename Delegate::value_type;

    static constexpr convert_array<From, Delegate> const delegate{};

    [[nodiscard]] constexpr inline auto operator()() const noexcept
    {
        auto xform = [](value_type const & x) -> delegate_value_type {
            if (To::is_null(x))
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

// qdb -> numpy
template <concepts::qdb_primitive From, concepts::dtype To, ranges::input_range R>
requires(concepts::input_range_t<R, From>) static inline qdb::masked_array masked_array(R && xs)
{
    if (ranges::empty(xs)) [[unlikely]]
    {
        return {};
    };

    py::array xs_ = detail::to_array<To>(xs | detail::convert_array<From, To>{}());
    return qdb::masked_array(xs_, qdb::masked_array::masked_null<To>(xs_));
}

}; // namespace qdb::convert
