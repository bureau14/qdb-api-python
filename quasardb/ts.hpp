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

#include "entry.hpp"
#include "ts_convert.hpp"

namespace qdb
{

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

    return c_columns;
}

static std::vector<column_info> convert_columns(const qdb_ts_column_info_t * columns, size_t count)
{
    std::vector<column_info> c_columns(count);

    std::transform(columns, columns + count, c_columns.begin(), [](const qdb_ts_column_info_t & ci) { return column_info{ci}; });

    return c_columns;
}

class ts : public entry
{

public:
public:
    ts(handle_ptr h, std::string a) noexcept
        : entry{h, a}
    {}

public:
    void create(const std::vector<column_info> & columns, std::chrono::milliseconds shard_size = std::chrono::hours{24})
    {
        const auto c_columns = convert_columns(columns);
        qdb::qdb_throw_if_error(qdb_ts_create(*_handle, _alias.c_str(), shard_size.count(), c_columns.data(), c_columns.size()));
    }

    void insert_columns(const std::vector<column_info> & columns)
    {
        const auto c_columns = convert_columns(columns);
        qdb::qdb_throw_if_error(qdb_ts_insert_columns(*_handle, _alias.c_str(), c_columns.data(), c_columns.size()));
    }

    std::vector<column_info> list_columns()
    {
        qdb_ts_column_info_t * columns = nullptr;
        qdb_size_t count               = 0;

        qdb::qdb_throw_if_error(qdb_ts_list_columns(*_handle, _alias.c_str(), &columns, &count));

        auto c_columns = convert_columns(columns, count);

        qdb_release(*_handle, columns);

        return c_columns;
    }

public:
    qdb_uint_t erase_ranges(const std::string & column, const time_ranges & ranges)
    {
        const auto c_ranges = convert_ranges(ranges);

        qdb_uint_t erased_count = 0;

        qdb::qdb_throw_if_error(qdb_ts_erase_ranges(*_handle, _alias.c_str(), column.c_str(), c_ranges.data(), c_ranges.size(), &erased_count));

        return erased_count;
    }

public:
    void blob_insert(const std::string & column, const pybind11::array & timestamps, const pybind11::array & values)
    {
        const auto points = convert_values<qdb_ts_blob_point, const char *>{}(timestamps, values);
        qdb::qdb_throw_if_error(qdb_ts_blob_insert(*_handle, _alias.c_str(), column.c_str(), points.data(), points.size()));
    }

    void double_insert(const std::string & column, const pybind11::array & timestamps, const pybind11::array_t<double> & values)
    {
        const auto points = convert_values<qdb_ts_double_point, double>{}(timestamps, values);
        qdb::qdb_throw_if_error(qdb_ts_double_insert(*_handle, _alias.c_str(), column.c_str(), points.data(), points.size()));
    }

    void int64_insert(const std::string & column, const pybind11::array & timestamps, const pybind11::array_t<std::int64_t> & values)
    {
        const auto points = convert_values<qdb_ts_int64_point, std::int64_t>{}(timestamps, values);
        qdb::qdb_throw_if_error(qdb_ts_int64_insert(*_handle, _alias.c_str(), column.c_str(), points.data(), points.size()));
    }

    void timestamp_insert(const std::string & column, const pybind11::array & timestamps, const pybind11::array_t<std::int64_t> & values)
    {
        const auto points = convert_values<qdb_ts_timestamp_point, std::int64_t>{}(timestamps, values);
        qdb::qdb_throw_if_error(qdb_ts_timestamp_insert(*_handle, _alias.c_str(), column.c_str(), points.data(), points.size()));
    }

public:
    std::pair<pybind11::array, pybind11::array> blob_get_ranges(const std::string & column, const time_ranges & ranges)
    {
        qdb_ts_blob_point * points = nullptr;
        qdb_size_t count           = 0;

        const auto c_ranges = convert_ranges(ranges);

        qdb::qdb_throw_if_error(
            qdb_ts_blob_get_ranges(*_handle, _alias.c_str(), column.c_str(), c_ranges.data(), c_ranges.size(), &points, &count));

        const auto res = vectorize_result<qdb_ts_blob_point, const char *>{}(points, count);

        qdb_release(*_handle, points);

        return res;
    }

    std::pair<pybind11::array, pybind11::array_t<double>> double_get_ranges(const std::string & column, const time_ranges & ranges)
    {
        qdb_ts_double_point * points = nullptr;
        qdb_size_t count             = 0;

        const auto c_ranges = convert_ranges(ranges);

        qdb::qdb_throw_if_error(
            qdb_ts_double_get_ranges(*_handle, _alias.c_str(), column.c_str(), c_ranges.data(), c_ranges.size(), &points, &count));

        const auto res = vectorize_result<qdb_ts_double_point, double>{}(points, count);

        qdb_release(*_handle, points);

        return res;
    }

    std::pair<pybind11::array, pybind11::array_t<std::int64_t>> int64_get_ranges(const std::string & column, const time_ranges & ranges)
    {
        qdb_ts_int64_point * points = nullptr;
        qdb_size_t count            = 0;

        const auto c_ranges = convert_ranges(ranges);

        qdb::qdb_throw_if_error(
            qdb_ts_int64_get_ranges(*_handle, _alias.c_str(), column.c_str(), c_ranges.data(), c_ranges.size(), &points, &count));

        const auto res = vectorize_result<qdb_ts_int64_point, std::int64_t>{}(points, count);

        qdb_release(*_handle, points);

        return res;
    }

    std::pair<pybind11::array, pybind11::array> timestamp_get_ranges(const std::string & column, const time_ranges & ranges)
    {
        qdb_ts_timestamp_point * points = nullptr;
        qdb_size_t count                = 0;

        const auto c_ranges = convert_ranges(ranges);

        qdb::qdb_throw_if_error(
            qdb_ts_timestamp_get_ranges(*_handle, _alias.c_str(), column.c_str(), c_ranges.data(), c_ranges.size(), &points, &count));

        const auto res = vectorize_result<qdb_ts_timestamp_point, std::int64_t>{}(points, count);

        qdb_release(*_handle, points);

        return res;
    }
};

template <typename Module>
static inline void register_ts(Module & m)
{
    namespace py = pybind11;

    py::enum_<qdb_ts_column_type_t>{m, "ColumnType", py::arithmetic(), "Column type"} //
        .value("Uninitialized", qdb_ts_column_uninitialized)                          //
        .value("Double", qdb_ts_column_double)                                        //
        .value("Blob", qdb_ts_column_blob)                                            //
        .value("Int64", qdb_ts_column_int64)                                          //
        .value("Timestamp", qdb_ts_column_timestamp);                                 //

    py::class_<qdb::column_info>{m, "ColumnInfo"}                   //
        .def(py::init<qdb_ts_column_type_t, const std::string &>()) //
        .def_readwrite("type", &qdb::column_info::type)             //
        .def_readwrite("name", &qdb::column_info::name);            //

    py::class_<qdb::ts, qdb::entry>{m, "TimeSeries"}                                                         //
        .def(py::init<qdb::handle_ptr, std::string>())                                                       //
        .def("create", &qdb::ts::create, py::arg("columns"), py::arg("shard_size") = std::chrono::hours{24}) //
        .def("get_name", &qdb::ts::get_name) //
        .def("insert_columns", &qdb::ts::insert_columns)                                                     //
        .def("list_columns", &qdb::ts::list_columns)                                                         //
        .def("erase_ranges", &qdb::ts::erase_ranges)                                                         //
        .def("blob_insert", &qdb::ts::blob_insert)                                                           //
        .def("double_insert", &qdb::ts::double_insert)                                                       //
        .def("int64_insert", &qdb::ts::int64_insert)                                                         //
        .def("timestamp_insert", &qdb::ts::timestamp_insert)                                                 //
        .def("blob_get_ranges", &qdb::ts::blob_get_ranges)                                                   //
        .def("double_get_ranges", &qdb::ts::double_get_ranges)                                               //
        .def("int64_get_ranges", &qdb::ts::int64_get_ranges)                                                 //
        .def("timestamp_get_ranges", &qdb::ts::timestamp_get_ranges);                                        //
}

} // namespace qdb
