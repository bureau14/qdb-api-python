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
#include "ts.hpp"

namespace qdb
{

struct batch_column_info
{
    batch_column_info() = default;
    batch_column_info(const std::string & ts_name, const std::string & col_name, qdb_size_t size_hint = 0)
        : timeseries{ts_name}
        , column{col_name}
        , elements_count_hint{size_hint}
    {}

    operator qdb_ts_batch_column_info_t() const noexcept
    {
        qdb_ts_batch_column_info_t res;

        res.timeseries          = timeseries.c_str();
        res.column              = column.c_str();
        res.elements_count_hint = elements_count_hint;
        return res;
    }

    std::string timeseries;
    std::string column;
    qdb_size_t elements_count_hint{0};
};

class ts_batch
{

public:
    ts_batch(qdb::handle_ptr h, const std::vector<batch_column_info> & ci)
        : _handle{h}
    {
        std::vector<qdb_ts_batch_column_info_t> converted(ci.size());

        std::transform(
            ci.cbegin(), ci.cend(), converted.begin(), [](const batch_column_info & ci) -> qdb_ts_batch_column_info_t { return ci; });

        qdb::qdb_throw_if_error(qdb_ts_batch_table_init(*_handle, converted.data(), converted.size(), &_batch_table));
    }

    // prevent copy because of the table object, use a unique_ptr of the batch in cluster
    // to return the object
    ts_batch(const ts_batch &) = delete;

    ~ts_batch()
    {
        if (_handle && _batch_table)
        {
            qdb_release(*_handle, _batch_table);
            _batch_table = nullptr;
        }
    }

public:
    void start_row(std::int64_t ts)
    {
        const qdb_timespec_t converted = convert_timestamp(ts);
        qdb::qdb_throw_if_error(qdb_ts_batch_start_row(_batch_table, &converted));
    }

    void set_blob(std::size_t index, const std::string & blob)
    {
        qdb::qdb_throw_if_error(qdb_ts_batch_row_set_blob(_batch_table, index, blob.data(), blob.size()));
    }

    void set_double(std::size_t index, double v)
    {
        qdb::qdb_throw_if_error(qdb_ts_batch_row_set_double(_batch_table, index, v));
    }

    void set_int64(std::size_t index, std::int64_t v)
    {
        qdb::qdb_throw_if_error(qdb_ts_batch_row_set_int64(_batch_table, index, v));
    }

    void set_timestamp(std::size_t index, std::int64_t v)
    {
        const qdb_timespec_t converted = convert_timestamp(v);
        qdb::qdb_throw_if_error(qdb_ts_batch_row_set_timestamp(_batch_table, index, &converted));
    }

    void push()
    {
        qdb::qdb_throw_if_error(qdb_ts_batch_push(_batch_table));
    }

    void push_async()
    {
        qdb::qdb_throw_if_error(qdb_ts_batch_push_async(_batch_table));
    }

private:
    qdb::handle_ptr _handle;
    qdb_batch_table_t _batch_table{nullptr};
};

// don't use shared_ptr, let Python do the reference counting, otherwise you will have an undefined behavior
using ts_batch_ptr = std::unique_ptr<ts_batch>;

template <typename Module>
static inline void register_ts_batch(Module & m)
{
    namespace py = pybind11;

    py::class_<qdb::batch_column_info>{m, "BatchColumnInfo"}                                 //
        .def(py::init<const std::string &, const std::string &, qdb_size_t>(),               //
            py::arg("ts_name"),                                                              //
            py::arg("col_name"),                                                             //
            py::arg("size_hint") = 0)                                                        //
        .def_readwrite("timeseries", &qdb::batch_column_info::timeseries)                    //
        .def_readwrite("column", &qdb::batch_column_info::column)                            //
        .def_readwrite("elements_count_hint", &qdb::batch_column_info::elements_count_hint); //

    py::class_<qdb::ts_batch>{m, "TimeSeriesBatch"}                               //
        .def(py::init<qdb::handle_ptr, const std::vector<batch_column_info> &>()) //
        .def("start_row", &qdb::ts_batch::start_row)                              //
        .def("set_blob", &qdb::ts_batch::set_blob)                                //
        .def("set_double", &qdb::ts_batch::set_double)                            //
        .def("set_int64", &qdb::ts_batch::set_int64)                              //
        .def("set_timestamp", &qdb::ts_batch::set_timestamp)                      //
        .def("push", &qdb::ts_batch::push)                                        //
        .def("push_async", &qdb::ts_batch::push_async);                           //
}

} // namespace qdb
