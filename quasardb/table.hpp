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

#include "entry.hpp"
#include "table_reader.hpp"
#include "ts_convert.hpp"
#include "detail/ts_column.hpp"
#include "reader/ts_row.hpp"

namespace qdb
{

class table : public entry
{
public:
    table(handle_ptr h, std::string a) noexcept
        : entry{h, a}
        , _has_indexed_columns(false)
    {}

public:
    std::string repr() const
    {
        return "<quasardb.TimeSeries name='" + get_name() + "'>";
    }

    void create(const std::vector<detail::column_info> & columns, std::chrono::milliseconds shard_size = std::chrono::hours{24})
    {
        const auto c_columns = detail::convert_columns(columns);
        qdb::qdb_throw_if_error(*_handle, qdb_ts_create(*_handle, _alias.c_str(), shard_size.count(), c_columns.data(), c_columns.size()));
    }

    void insert_columns(const std::vector<detail::column_info> & columns)
    {
        const auto c_columns = detail::convert_columns(columns);
        qdb::qdb_throw_if_error(*_handle, qdb_ts_insert_columns(*_handle, _alias.c_str(), c_columns.data(), c_columns.size()));
    }

    std::vector<detail::column_info> list_columns() const
    {
        qdb_ts_column_info_t * columns = nullptr;
        qdb_size_t count               = 0;

        qdb::qdb_throw_if_error(*_handle, qdb_ts_list_columns(*_handle, _alias.c_str(), &columns, &count));

        auto c_columns = detail::convert_columns(columns, count);

        qdb_release(*_handle, columns);

        return c_columns;
    }

    detail::indexed_column_info column_info_by_id(const std::string & alias) const
    {
        if (_has_indexed_columns == false)
        {
            // It's important to note that if additional columns are added during
            // the lifetime of this object, we will not pick up on this in our cache.
            _indexed_columns     = detail::index_columns(list_columns());
            _has_indexed_columns = true;
        }

        detail::indexed_columns_t::const_iterator i = _indexed_columns.find(alias);
        if (i == _indexed_columns.end()) throw qdb::exception{qdb_e_out_of_bounds, std::string("Column not found: ") + alias};

        return i->second;
    }

    qdb_size_t column_index_by_id(const std::string & alias) const
    {
        return column_info_by_id(alias).index;
    }

    qdb_ts_column_type_t column_type_by_id(const std::string & alias) const
    {
        return column_info_by_id(alias).type;
    }

    py::object reader(const std::vector<std::string> & columns, const obj_time_ranges & obj_ranges, bool dict_mode) const
    {
        time_ranges ranges = prep_ranges(obj_ranges);
        std::vector<detail::column_info> c_columns;

        if (columns.empty())
        {
            // This is a kludge, because technically a table can have no columns, and we're
            // abusing it as "no argument provided". It's a highly exceptional use case, and
            // doesn't really have any implication in practice (we just look up twice), so it
            // should be ok.
            c_columns = list_columns();
        }
        else
        {
            c_columns.reserve(columns.size());
            // This transformation can probably be optimized, but it's only invoked when constructing
            // the reader so it's unlikely to be a performance bottleneck.
            std::transform(std::cbegin(columns), std::cend(columns), std::back_inserter(c_columns), [this](const auto & col) {
                return detail::column_info{this->column_type_by_id(col), col};
            });
        }

        auto r = convert_ranges(ranges);

        return (dict_mode == true
                    ? py::cast(qdb::table_reader<reader::ts_dict_row>(_handle, _alias, c_columns, r), py::return_value_policy::move)
                    : py::cast(qdb::table_reader<reader::ts_fast_row>(_handle, _alias, c_columns, r), py::return_value_policy::move));
    }

public:
    qdb_uint_t erase_ranges(const std::string & column, const time_ranges & ranges)
    {
        const auto c_ranges = convert_ranges(ranges);

        qdb_uint_t erased_count = 0;

        qdb::qdb_throw_if_error(
            *_handle, qdb_ts_erase_ranges(*_handle, _alias.c_str(), column.c_str(), c_ranges.data(), c_ranges.size(), &erased_count));

        return erased_count;
    }

public:
    void blob_insert(const std::string & column, const pybind11::array & timestamps, const pybind11::array & values)
    {
        const auto points = convert_values<qdb_ts_blob_point, const char *>{}(timestamps, values);
        qdb::qdb_throw_if_error(*_handle, qdb_ts_blob_insert(*_handle, _alias.c_str(), column.c_str(), points.data(), points.size()));
    }

    void string_insert(const std::string & column, const pybind11::array & timestamps, const pybind11::array & values)
    {
        const auto points = convert_values<qdb_ts_string_point, const char *>{}(timestamps, values);
        qdb::qdb_throw_if_error(*_handle, qdb_ts_string_insert(*_handle, _alias.c_str(), column.c_str(), points.data(), points.size()));
    }

    void double_insert(const std::string & column, const pybind11::array & timestamps, const pybind11::array_t<double> & values)
    {
        const auto points = convert_values<qdb_ts_double_point, double>{}(timestamps, values);
        qdb::qdb_throw_if_error(*_handle, qdb_ts_double_insert(*_handle, _alias.c_str(), column.c_str(), points.data(), points.size()));
    }

    void int64_insert(const std::string & column, const pybind11::array & timestamps, const pybind11::array_t<std::int64_t> & values)
    {
        const auto points = convert_values<qdb_ts_int64_point, std::int64_t>{}(timestamps, values);
        qdb::qdb_throw_if_error(*_handle, qdb_ts_int64_insert(*_handle, _alias.c_str(), column.c_str(), points.data(), points.size()));
    }

    void timestamp_insert(const std::string & column, const pybind11::array & timestamps, const pybind11::array_t<std::int64_t> & values)
    {
        const auto points = convert_values<qdb_ts_timestamp_point, std::int64_t>{}(timestamps, values);
        qdb::qdb_throw_if_error(*_handle, qdb_ts_timestamp_insert(*_handle, _alias.c_str(), column.c_str(), points.data(), points.size()));
    }

public:
    std::pair<pybind11::array, pybind11::array> blob_get_ranges(const std::string & column, const time_ranges & ranges)
    {
        qdb_ts_blob_point * points = nullptr;
        qdb_size_t count           = 0;

        const auto c_ranges = convert_ranges(ranges);

        qdb::qdb_throw_if_error(
            *_handle, qdb_ts_blob_get_ranges(*_handle, _alias.c_str(), column.c_str(), c_ranges.data(), c_ranges.size(), &points, &count));

        const auto res = vectorize_result<qdb_ts_blob_point, const char *>{}(points, count);

        qdb_release(*_handle, points);

        return res;
    }

    std::pair<pybind11::array, pybind11::array> string_get_ranges(const std::string & column, const time_ranges & ranges)
    {
        qdb_ts_string_point * points = nullptr;
        qdb_size_t count             = 0;

        const auto c_ranges = convert_ranges(ranges);

        qdb::qdb_throw_if_error(*_handle,
            qdb_ts_string_get_ranges(*_handle, _alias.c_str(), column.c_str(), c_ranges.data(), c_ranges.size(), &points, &count));

        const auto res = vectorize_result<qdb_ts_string_point, const char *>{}(points, count);

        qdb_release(*_handle, points);

        return res;
    }

    std::pair<pybind11::array, pybind11::array_t<double>> double_get_ranges(const std::string & column, const time_ranges & ranges)
    {
        qdb_ts_double_point * points = nullptr;
        qdb_size_t count             = 0;

        const auto c_ranges = convert_ranges(ranges);

        qdb::qdb_throw_if_error(*_handle,
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
            *_handle, qdb_ts_int64_get_ranges(*_handle, _alias.c_str(), column.c_str(), c_ranges.data(), c_ranges.size(), &points, &count));

        const auto res = vectorize_result<qdb_ts_int64_point, std::int64_t>{}(points, count);

        qdb_release(*_handle, points);

        return res;
    }

    std::pair<pybind11::array, pybind11::array> timestamp_get_ranges(const std::string & column, const time_ranges & ranges)
    {
        qdb_ts_timestamp_point * points = nullptr;
        qdb_size_t count                = 0;

        const auto c_ranges = convert_ranges(ranges);

        qdb::qdb_throw_if_error(*_handle,
            qdb_ts_timestamp_get_ranges(*_handle, _alias.c_str(), column.c_str(), c_ranges.data(), c_ranges.size(), &points, &count));

        const auto res = vectorize_result<qdb_ts_timestamp_point, std::int64_t>{}(points, count);

        qdb_release(*_handle, points);

        return res;
    }

private:
    mutable bool _has_indexed_columns;
    mutable detail::indexed_columns_t _indexed_columns;
};

template <typename Module>
static inline void register_table(Module & m)
{
    namespace py = pybind11;

    py::enum_<qdb_ts_column_type_t>{m, "ColumnType", py::arithmetic(), "Column type"} //
        .value("Uninitialized", qdb_ts_column_uninitialized)                          //
        .value("Double", qdb_ts_column_double)                                        //
        .value("Blob", qdb_ts_column_blob)                                            //
        .value("String", qdb_ts_column_string)                                        //
        .value("Int64", qdb_ts_column_int64)                                          //
        .value("Timestamp", qdb_ts_column_timestamp);                                 //

    py::class_<qdb::table, qdb::entry>{m, "Table", "Table representation"} //
        .def(py::init<qdb::handle_ptr, std::string>())                     //
        .def("__repr__", &qdb::table::repr)
        .def("create", &qdb::table::create, py::arg("columns"), py::arg("shard_size") = std::chrono::hours{24}) //
        .def("get_name", &qdb::table::get_name)                                                                 //
        .def("column_index_by_id", &qdb::table::column_index_by_id)                                             //
        .def("column_type_by_id", &qdb::table::column_type_by_id)                                               //
        .def("insert_columns", &qdb::table::insert_columns)                                                     //
        .def("list_columns", &qdb::table::list_columns)                                                         //

        // We cannot initialize columns with all columns by default, because i don't
        // see a way to figure out the `this` address for qdb_ts_reader for the default
        // arguments, and we need it to call qdb::table::list_columns().
        .def("reader", &qdb::table::reader, py::arg("columns") = std::vector<std::string>(), py::arg("ranges") = all_ranges(),
            py::arg("dict") = false)

        .def("erase_ranges", &qdb::table::erase_ranges)                                                                       //
        .def("blob_insert", &qdb::table::blob_insert)                                                                         //
        .def("string_insert", &qdb::table::string_insert)                                                                     //
        .def("double_insert", &qdb::table::double_insert)                                                                     //
        .def("int64_insert", &qdb::table::int64_insert)                                                                       //
        .def("timestamp_insert", &qdb::table::timestamp_insert)                                                               //
        .def("blob_get_ranges", &qdb::table::blob_get_ranges, py::arg("column"), py::arg("ranges") = all_ranges())            //
        .def("string_get_ranges", &qdb::table::string_get_ranges, py::arg("column"), py::arg("ranges") = all_ranges())        //
        .def("double_get_ranges", &qdb::table::double_get_ranges, py::arg("column"), py::arg("ranges") = all_ranges())        //
        .def("int64_get_ranges", &qdb::table::int64_get_ranges, py::arg("column"), py::arg("ranges") = all_ranges())          //
        .def("timestamp_get_ranges", &qdb::table::timestamp_get_ranges, py::arg("column"), py::arg("ranges") = all_ranges()); //
}

} // namespace qdb
