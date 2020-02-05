/*
 *
 * Official Python API
 *
 * Copyright (c) 2009-2020, quasardb SAS. All rights reserved.
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

#include "utils.hpp"
#include <qdb/ts.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <iostream>

namespace py = pybind11;

namespace qdb
{

static inline std::int64_t convert_timestamp(const qdb_timespec_t & ts) noexcept
{
    return ts.tv_nsec + ts.tv_sec * 1'000'000'000ull;
}

// assuming ns
static inline qdb_timespec_t convert_timestamp(std::int64_t npdt64) noexcept
{
    qdb_timespec_t res;

    static constexpr std::int64_t ns = 1'000'000'000ull;

    res.tv_nsec = npdt64 % ns;
    res.tv_sec  = (npdt64 - res.tv_nsec) / ns;

    return res;
}

static inline std::int64_t prep_datetime(py::object v)
{
    // Starting version 3.8, Python does not allow implicit casting from numpy.datetime64
    // to an int, so we explicitly do it here.
    try
    {
        return v.cast<std::int64_t>();
    }
    catch (py::cast_error const & /*e*/)
    {
        throw qdb::invalid_datetime_exception{v};
    }
}

static inline qdb_timespec_t convert_timestamp(py::object v)
{
    try
    {
        return convert_timestamp(prep_datetime(v));
    }
    catch (py::cast_error const & /*e*/)
    {
        throw qdb::invalid_datetime_exception{};
    }
}

using time_range  = std::pair<std::int64_t, std::int64_t>;
using time_ranges = std::vector<time_range>;

using obj_time_range  = std::pair<py::object, py::object>;
using obj_time_ranges = std::vector<obj_time_range>;

static inline qdb_ts_range_t convert_range(const time_range & tr) noexcept
{
    return qdb_ts_range_t{convert_timestamp(tr.first), convert_timestamp(tr.second)};
}

static inline std::vector<qdb_ts_range_t> convert_ranges(const time_ranges & ranges)
{
    std::vector<qdb_ts_range_t> res(ranges.size());

    std::transform(ranges.cbegin(), ranges.cend(), res.begin(), [](const time_range & tr) { return convert_range(tr); });

    return res;
}

static inline time_ranges prep_ranges(const obj_time_ranges & ranges)
{
    time_ranges res(ranges.size());

    std::transform(ranges.cbegin(), ranges.cend(), res.begin(), [](const obj_time_range & tr) {
        auto x = prep_datetime(tr.first);
        auto y = prep_datetime(tr.second);

        return time_range{x, y};
    });

    return res;
}

// TODO: Is the a more compliant way to describe 'all data in a table'?
static inline time_ranges all_ranges()
{
    return time_ranges{{0, std::numeric_limits<std::int64_t>::max()}};
}

template <typename PointType>
static void update_str(PointType & pt, const char * s, size_t max_size) noexcept
{
#ifdef _MSC_VER
    pt.content_length = strnlen_s(s, max_size);
#else
    pt.content_length = strnlen(s, max_size);
#endif

    if (pt.content_length > 0)
    {
        pt.content = s;
    }
    else
    {
        pt.content = nullptr;
    }
}

template <typename Point, typename T>
struct convert_values
{
    std::vector<Point> operator()(const pybind11::array & timestamps, const pybind11::array_t<T> & values) const
    {
        if (timestamps.size() != values.size()) throw qdb::exception{qdb_e_invalid_argument, "Timestamps size must match values size"};
        if ((timestamps.ndim() != 1) || (values.ndim() != 1))
            throw qdb::exception{qdb_e_invalid_argument, "Only single-dimension numpy arrays are supported"};

        std::vector<Point> points(timestamps.size());

        auto t = timestamps.template unchecked<std::int64_t, 1>();
        auto v = values.template unchecked<1>();

        for (size_t i = 0; i < points.size(); ++i)
        {
            points[i].timestamp = convert_timestamp(t(i));
            points[i].value     = v(i);
        }

        return points;
    }
};

template <>
struct convert_values<qdb_ts_blob_point, const char *>
{
    std::vector<qdb_ts_blob_point> operator()(const pybind11::array & timestamps, const pybind11::array & values) const
    {
        if (timestamps.size() != values.size()) throw qdb::exception{qdb_e_invalid_argument, "Timestamps size must match values size"};
        if ((timestamps.ndim() != 1) || (values.ndim() != 1))
            throw qdb::exception{qdb_e_invalid_argument, "Only single-dimension numpy arrays are supported"};

        std::vector<qdb_ts_blob_point> points(timestamps.size());

        auto t           = timestamps.template unchecked<std::int64_t, 1>();
        const char * ptr = static_cast<const char *>(values.data());

        size_t str_size = values.itemsize();

        // compute string vie

        for (size_t i = 0; i < points.size(); ++i, ptr += str_size)
        {
            points[i].timestamp = convert_timestamp(t(i));
            update_str(points[i], ptr, str_size);
        }

        return points;
    }
};

template <>
struct convert_values<qdb_ts_string_point, const char *>
{
    std::vector<qdb_ts_string_point> operator()(const pybind11::array & timestamps, const pybind11::array & values) const
    {
        if (timestamps.size() != values.size()) throw qdb::exception{qdb_e_invalid_argument, "Timestamps size must match values size"};
        if ((timestamps.ndim() != 1) || (values.ndim() != 1))
            throw qdb::exception{qdb_e_invalid_argument, "Only single-dimension numpy arrays are supported"};

        std::vector<qdb_ts_string_point> points(timestamps.size());

        auto t              = timestamps.template unchecked<std::int64_t, 1>();
        const py::str * ptr = static_cast<const py::str *>(values.data());

        size_t str_size = values.itemsize();

        for (size_t i = 0; i < points.size(); ++i, ptr += 1)
        {
            points[i].timestamp = convert_timestamp(t(i));
            // update_str(points[i], ptr, str_size);

            // We use low-level APIs here because pybind11 wraps a bit too much.
            // Using the native API directly allows us to write our UTF-8 strings
            // directly into our qdb buffers.
            PyObject * temp = PyUnicode_AsUTF8String(ptr->ptr());

            if (PYBIND11_BYTES_AS_STRING_AND_SIZE(temp, (char **)(&points[i].content), (Py_ssize_t *)(&points[i].content_length)))
            {
                throw qdb::incompatible_type_exception{};
            }
        }

        return points;
    }
};

template <>
struct convert_values<qdb_ts_timestamp_point, std::int64_t>
{
    std::vector<qdb_ts_timestamp_point> operator()(const pybind11::array & timestamps, const pybind11::array_t<std::int64_t> & values) const
    {
        if (timestamps.size() != values.size()) throw qdb::exception{qdb_e_invalid_argument, "Timestamps size must match values size"};
        if ((timestamps.ndim() != 1) || (values.ndim() != 1))
            throw qdb::exception{qdb_e_invalid_argument, "Only single-dimension numpy arrays are supported"};

        std::vector<qdb_ts_timestamp_point> points(timestamps.size());

        auto t = timestamps.template unchecked<std::int64_t, 1>();
        auto v = values.template unchecked<1>();

        for (size_t i = 0; i < points.size(); ++i)
        {
            points[i].timestamp = convert_timestamp(t(i));
            points[i].value     = convert_timestamp(v(i));
        }

        return points;
    }
};

template <typename Point, typename T>
struct vectorize_result
{
    using result_type = std::pair<pybind11::array, typename pybind11::array_t<T>>;

    result_type operator()(const Point * points, size_t count) const
    {
        result_type res{pybind11::array{"datetime64[ns]", {count}}, pybind11::array_t<T>{{count}}};

        auto ts_dest = res.first.template mutable_unchecked<std::int64_t, 1>();
        auto v_dest  = res.second.template mutable_unchecked<1>();

        for (size_t i = 0; i < count; ++i)
        {
            ts_dest(i) = convert_timestamp(points[i].timestamp);
            v_dest(i)  = points[i].value;
        }

        return res;
    }
};

template <>
struct vectorize_result<qdb_ts_timestamp_point, std::int64_t>
{
    using result_type = std::pair<pybind11::array, pybind11::array>;

    result_type operator()(const qdb_ts_timestamp_point * points, size_t count) const
    {
        result_type res{pybind11::array{"datetime64[ns]", {count}}, pybind11::array{"datetime64[ns]", {count}}};

        auto ts_dest = res.first.template mutable_unchecked<std::int64_t, 1>();
        auto v_dest  = res.second.template mutable_unchecked<std::int64_t, 1>();

        for (size_t i = 0; i < count; ++i)
        {
            ts_dest(i) = convert_timestamp(points[i].timestamp);
            v_dest(i)  = convert_timestamp(points[i].value);
        }

        return res;
    }
};

template <>
struct vectorize_result<qdb_ts_blob_point, const char *>
{
    using result_type = std::pair<pybind11::array, pybind11::array>;

    result_type operator()(const qdb_ts_blob_point * points, size_t count) const
    {
        size_t item_size = max_length(points, count);

        std::stringstream ss;

        ss << "|S" << item_size;

        const std::string str = ss.str();

        result_type res{pybind11::array{"datetime64[ns]", {count}}, pybind11::array{str.c_str(), {count}}};

        auto ts_dest = res.first.template mutable_unchecked<std::int64_t, 1>();
        char * ptr   = static_cast<char *>(res.second.mutable_data());

        for (size_t i = 0; i < count; ++i, ptr += item_size)
        {
            ts_dest(i) = convert_timestamp(points[i].timestamp);

            assert(points[i].content_length <= item_size);

            memset(ptr, 0, item_size);
            memcpy(ptr, points[i].content, points[i].content_length);
        }

        return res;
    }
};

template <>
struct vectorize_result<qdb_ts_string_point, const char *>
{
    using result_type = std::pair<pybind11::array, pybind11::array>;

    result_type operator()(const qdb_ts_string_point * points, size_t count) const
    {
        size_t item_size = max_length(points, count);

        result_type res{pybind11::array{"datetime64[ns]", {count}}, pybind11::array{"O", {count}}};

        auto ts_dest = res.first.template mutable_unchecked<std::int64_t, 1>();
        auto v_dest  = res.second.template mutable_unchecked<pybind11::object, 1>();

        for (size_t i = 0; i < count; ++i)
        {
            ts_dest(i) = convert_timestamp(points[i].timestamp);
            v_dest(i)  = py::str(points[i].content, points[i].content_length);
        }

        return res;
    }
};

} // namespace qdb
