/*
 *
 * Official Python API
 *
 * Copyright (c) 2009-2019, quasardb SAS
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
#include <pybind11/numpy.h>

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

using time_range  = std::pair<std::int64_t, std::int64_t>;
using time_ranges = std::vector<time_range>;

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

// TODO: Is the a more compliant way to describe 'all data in a table'?
static inline time_ranges all_ranges()
{
    return time_ranges{{0, std::numeric_limits<std::int64_t>::max()}};
}

struct column_info
{
    column_info() = default;

    column_info(qdb_ts_column_type_t t, const std::string & n)
        : type{t}
        , name{n}
    {}

    column_info(const qdb_ts_column_info_t & ci)
        : column_info{ci.type, ci.name}
    {}

    operator qdb_ts_column_info_t() const noexcept
    {
        qdb_ts_column_info_t res;

        res.type = type;
        res.name = name.c_str();

        return res;
    }

    qdb_ts_column_type_t type{qdb_ts_column_uninitialized};
    std::string name;
};

static std::vector<qdb_ts_column_info_t> convert_columns(const std::vector<column_info> & columns)
{
    std::vector<qdb_ts_column_info_t> c_columns(columns.size());

    std::transform(columns.cbegin(), columns.cend(), c_columns.begin(), [](const column_info & ci) -> qdb_ts_column_info_t { return ci; });

    std::cout << "convert columns, python->native, count = " << columns.size() << std::endl;

    for (auto i : c_columns) {
      std::cout << "converted columns ->native, type = " << i.type << ", alias = " << i.name << std::endl;
    }

    return c_columns;
}

static std::vector<column_info> convert_columns(const qdb_ts_column_info_t * columns, size_t count)
{
    std::vector<column_info> c_columns(count);

    std::transform(columns, columns + count, c_columns.begin(), [](const qdb_ts_column_info_t & ci) { return column_info{ci}; });

    std::cout << "convert columns, native->python, count = " << count << std::endl;

    for (auto i : c_columns) {
      std::cout << "converted columns ->python, type = " << i.type << ", alias = " << i.name << std::endl;
    }




    return c_columns;
}

static std::vector<std::string> column_list_to_strings(const std::vector<column_info> & columns)
{
    std::vector<std::string> s_columns(columns.size());

    std::transform(columns.cbegin(), columns.cend(), s_columns.begin(), [](const column_info & ci) -> std::string { return ci.name; });

    return s_columns;
}

typedef std::map<std::string, std::pair<qdb_ts_column_type_t, qdb_size_t>> indexed_columns_t;

template <typename ColumnType>
static indexed_columns_t index_columns(const std::vector<ColumnType> & columns)
{
    indexed_columns_t i_columns;
    for (qdb_size_t i = 0; i < columns.size(); ++i)
    {
      std::cout << "indexing columns, columns[i].name = " << columns[i].name << std::endl;
        i_columns.insert(indexed_columns_t::value_type(columns[i].name, std::make_pair(columns[i].type, i)));
    }

    return i_columns;
}

static void update_str(qdb_ts_blob_point & pt, const char * s, size_t max_size) noexcept
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
        if (timestamps.size() != values.size()) throw qdb::exception{qdb_e_invalid_argument};
        if ((timestamps.ndim() != 1) || (values.ndim() != 1)) throw qdb::exception{qdb_e_invalid_argument};

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
        if (timestamps.size() != values.size()) throw qdb::exception{qdb_e_invalid_argument};
        if ((timestamps.ndim() != 1) || (values.ndim() != 1)) throw qdb::exception{qdb_e_invalid_argument};

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
struct convert_values<qdb_ts_timestamp_point, std::int64_t>
{
    std::vector<qdb_ts_timestamp_point> operator()(const pybind11::array & timestamps, const pybind11::array_t<std::int64_t> & values) const
    {
        if (timestamps.size() != values.size()) throw qdb::exception{qdb_e_invalid_argument};
        if ((timestamps.ndim() != 1) || (values.ndim() != 1)) throw qdb::exception{qdb_e_invalid_argument};

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

} // namespace qdb
