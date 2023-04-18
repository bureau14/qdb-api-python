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
#include "../traits.hpp"
#include "../utils/unzip_view.hpp"
#include "array.hpp"
#include "range.hpp"
#include "value.hpp"
#include <qdb/ts.h>
#include <pybind11/pybind11.h>

namespace qdb::convert::detail
{

namespace py = pybind11;

////////////////////////////////////////////////////////////////////////////////
//
// 'POINT' <> PAIR CONVERTORS
//
///////////////////
//
// Allows splitting of qdb_ts_*_point to a pair<qdb_timespec_t, value> and back.
// calling point_to_pair(pair_to_point(x)) will always give you the original
// value back.
//
////////////////////////////////////////////////////////////////////////////////

template <typename T>
requires(concepts::qdb_primitive<T>) using point_type = typename traits::qdb_value<T>::point_type;

template <typename T>
requires(concepts::qdb_point<T>) using primitive_type = typename traits::qdb_value<T>::primitive_type;

// Base declaration of pair-to-point function
template <concepts::qdb_primitive T>
inline point_type<T> pair_to_point(std::pair<qdb_timespec_t, T> const & x)
{
    return {std::get<0>(x), std::get<1>(x)};
};

// Base declaration of point-to-pair function
template <concepts::qdb_point T>
inline std::pair<qdb_timespec_t, primitive_type<T>> point_to_pair(T const & x)
{
    return std::make_pair(x.timestamp, x.value);
};

// Specific overloads: qdb_blob_t <> qdb_ts_blob_point
template <>
constexpr inline qdb_ts_blob_point pair_to_point<qdb_blob_t>(
    std::pair<qdb_timespec_t, qdb_blob_t> const & x)
{
    qdb_blob_t const & x_ = std::get<1>(x);
    return {std::get<0>(x), x_.content, x_.content_length};
};

template <>
constexpr inline std::pair<qdb_timespec_t, qdb_blob_t> point_to_pair<qdb_ts_blob_point>(
    qdb_ts_blob_point const & x)
{
    return std::make_pair(x.timestamp, qdb_blob_t{x.content, x.content_length});
};

// Specific overloads: qdb_string_t <> qdb_ts_string_point
template <>
constexpr inline qdb_ts_string_point pair_to_point<qdb_string_t>(
    std::pair<qdb_timespec_t, qdb_string_t> const & x)
{
    qdb_string_t const & x_ = std::get<1>(x);
    return {std::get<0>(x), x_.data, x_.length};
};

template <>
constexpr inline std::pair<qdb_timespec_t, qdb_string_t> point_to_pair<qdb_ts_string_point>(
    qdb_ts_string_point const & x)
{
    return std::make_pair(x.timestamp, qdb_string_t{x.content, x.content_length});
};

////////////////////////////////////////////////////////////////////////////////
//
// 'POINT' ARRAY CONVERTERS
//
///////////////////
//
// These converters apply to all the qdb_ts_*_point structs. They are effectively
// a timestamp array and a 'value' array, and most of the work is dispatched to
// the array and value converters.
//
////////////////////////////////////////////////////////////////////////////////

template <typename From, typename To>
struct convert_point_array;

/////
//
// numpy->qdb transforms
//
// Input 1: range of length N, dtype: np.datetime64[ns]
// Input 2: range of length N, dtype: From (e.g. float64 or unicode)
// Output:  range of length N, type:  qdb_point, e.g. qdb_ts_double_point or qdb_ts_string_point
//
/////
template <typename From, typename To>
requires(concepts::dtype<From> && concepts::qdb_primitive<To>) struct convert_point_array<From, To>
{
    static constexpr convert_array<From, To> const value_delegate_{};
    static constexpr convert_array<traits::datetime64_ns_dtype, qdb_timespec_t> const ts_delegate_{};

    template <ranges::input_range TimestampRange, ranges::input_range ValueRange>
    constexpr inline auto operator()(TimestampRange && timestamps, ValueRange && values) const
    {
        assert(ranges::size(timestamps) == ranges::size(values));
        assert(ranges::empty(timestamps) == false);

        // Our flow:
        // * take two separate ranges;
        // * convert values from numpy to qdb, e.g. np.datetime64 (which is
        //   std::int64_t) to qdb_timespec_t)
        // * zips them together as pairs;
        // * convert pairs into qdb point structs
        // * profit

        auto timestamps_ = timestamps | ts_delegate_();
        auto values_     = values | value_delegate_();

        return ranges::zip_view(std::move(timestamps_), std::move(values_))
               | ranges::views::transform(pair_to_point<To>);
    }
};

/////
//
// qdb->numpy transforms
//
// Input:  range of length N, type:  qdb_point, e.g. qdb_ts_double_point or qdb_ts_string_point
// Output: pair<range of length N, range of length N>
//
// First output range is $timestamp array, dtype: np.datetime64[ns]
// Second output range is values array, dtype: To
//
/////
template <typename From, typename To>
requires(concepts::qdb_primitive<From> && concepts::dtype<To>) struct convert_point_array<From, To>
{
    static constexpr convert_array<From, To> const value_delegate_{};
    static constexpr convert_array<qdb_timespec_t, traits::datetime64_ns_dtype> const ts_delegate_{};

    template <ranges::input_range R>
    constexpr inline auto operator()(R && xs) const
    {
        assert(ranges::empty(xs) == false);
        auto range_of_pairs = xs | ranges::views::transform(point_to_pair<point_type<From>>);

        auto const && [timestamps, values] = qdb::make_unzip_views(range_of_pairs);

        auto timestamps_ = timestamps | ts_delegate_();
        auto values_     = values | value_delegate_();

        return std::make_pair(timestamps_, values_);
    }
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

template <typename Type>
using point_type = typename traits::qdb_value<Type>::point_type;

// numpy -> qdb
// input1: np.ndarray, dtype: datetime64[ns]
// input2: np.ndarray, dtype: From
// output: OutputIterator
template <concepts::dtype From,
    concepts::qdb_primitive To,
    ranges::output_iterator<point_type<To>> OutputIterator>
static inline void point_array(
    py::array const & timestamps, py::array const & values, OutputIterator dst)
{
    assert(timestamps.size() == values.size());

    if (ranges::empty(timestamps)) [[unlikely]]
    {
        return;
    }

    static constexpr detail::convert_point_array<From, To> const xform{};
    auto timestamps_ = detail::to_range<traits::datetime64_ns_dtype>(timestamps);
    auto values_     = detail::to_range<From>(values);

    ranges::copy(xform(timestamps_, values_), dst);
};

// numpy -> qdb
// input1: np.ndarray, dtype: datetime64[ns]
// input2: np.ndarray, dtype: From
// output: std::vector<To>
template <concepts::dtype From, concepts::qdb_primitive To>
static inline std::vector<point_type<To>> point_array(
    py::array const & timestamps, py::array const & values)
{
    assert(timestamps.size() == values.size());

    if (ranges::empty(timestamps)) [[unlikely]]
    {
        return {};
    }

    static constexpr detail::convert_point_array<From, To> const xform{};
    auto timestamps_ = detail::to_range<traits::datetime64_ns_dtype>(timestamps);
    auto values_     = detail::to_range<From>(values);

    return ranges::to<std::vector>(xform(timestamps_, values_));
};

// numpy -> qdb
template <concepts::dtype From, concepts::qdb_primitive To>
static inline void point_array(
    py::array const & timestamps, py::array const & values, std::vector<point_type<To>> & dst)
{
    dst.resize(timestamps.size()); // <- important!
    point_array<From, To>(timestamps, values, ranges::begin(dst));
};

// numpy -> qdb
template <concepts::dtype From, concepts::qdb_primitive To>
static inline void point_array(
    py::array const & timestamps, qdb::masked_array const & values, std::vector<point_type<To>> & dst)
{
    point_array<From, To>(timestamps, values.filled<From>(), dst);
};

// numpy -> qdb
template <concepts::dtype From, concepts::qdb_primitive To>
static inline std::vector<point_type<To>> point_array(
    py::array const & timestamps, qdb::masked_array const & values)
{
    return point_array<From, To>(timestamps, values.filled<From>());
};

// numpy -> qdb
template <concepts::dtype From, concepts::qdb_primitive To>
static inline std::vector<point_type<To>> point_array(
    std::pair<py::array, qdb::masked_array> const & xs)
{
    return point_array<From, To>(std::get<0>(xs), std::get<1>(xs));
};

// qdb -> numpy
//
// Takes a range of qdb point structs (eg qdb_ts_double_point) and returns a pair of two
// numpy ndarrays, one for timestamps and another for the values.
template <concepts::qdb_primitive From, concepts::dtype To, ranges::input_range R>
requires(concepts::input_range_t<R, point_type<From>>) static inline std::pair<py::array,
    qdb::masked_array> point_array(R && xs)
{
    if (ranges::empty(xs)) [[unlikely]]
    {
        return {};
    };

    static constexpr detail::convert_point_array<From, To> const xform{};

    auto const && [timestamps, values] = xform(xs);

    py::array timestamps_ = detail::to_array<traits::datetime64_ns_dtype>(std::move(timestamps));
    py::array values_     = detail::to_array<To>(std::move(values));

    assert(timestamps_.size() == values_.size());

    return std::make_pair(
        timestamps_, qdb::masked_array(values_, qdb::masked_array::masked_null<To>(values_)));
}

// qdb -> numpy
template <concepts::qdb_primitive From, concepts::dtype To>
static inline std::pair<py::array, qdb::masked_array> point_array(
    point_type<From> const * input, std::size_t n)
{
    return point_array<From, To>(ranges::views::counted(input, n));
};

}; // namespace qdb::convert
