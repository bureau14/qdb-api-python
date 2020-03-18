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
#include "entry.hpp"
#include "logger.hpp"
#include "table.hpp"

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

class batch_inserter
{

public:
    batch_inserter(qdb::handle_ptr h, const std::vector<batch_column_info> & ci)
        : _logger("quasardb.batch_inserter")
        , _handle{h}
        , _row_count{0}
        , _point_count{0}
        , _min_max_ts {qdb_min_timespec, qdb_min_timespec}
    {
        std::vector<qdb_ts_batch_column_info_t> converted(ci.size());


        std::transform(
            ci.cbegin(), ci.cend(), converted.begin(), [](const batch_column_info & ci) -> qdb_ts_batch_column_info_t { return ci; });

        qdb::qdb_throw_if_error(*_handle, qdb_ts_batch_table_init(*_handle, converted.data(), converted.size(), &_batch_table));

        _logger.debug("initialized batch reader with %d columns", ci.size());
    }

    // prevent copy because of the table object, use a unique_ptr of the batch in cluster
    // to return the object
    batch_inserter(const batch_inserter &) = delete;

    ~batch_inserter()
    {
        if (_handle && _batch_table)
        {
            qdb_release(*_handle, _batch_table);
            _batch_table = nullptr;
        }
    }

public:
    void start_row(py::object ts)
    {
        const qdb_timespec_t converted = convert_timestamp(ts);
        qdb::qdb_throw_if_error(*_handle, qdb_ts_batch_start_row(_batch_table, &converted));

        // The block below is only necessary for insert_truncate, and even then if someone
        // does not explicitly provide a time range. It shouldn't be *too* much of a performance
        // impact, but maybe we can somehow optimize this away?
        if (_min_max_ts.begin == qdb_min_timespec &&
            _min_max_ts.end == qdb_min_timespec) {
          assert(_row_count == 0); // sanity check

          // First row
          _min_max_ts.begin = converted;
          _min_max_ts.end = converted;
        } else {
          assert(_row_count > 0);
          _min_max_ts.begin = std::min(converted, _min_max_ts.begin);
          _min_max_ts.end = std::max(converted, _min_max_ts.end);
        }

        ++_row_count;
    }

    void set_blob(std::size_t index, const py::bytes & blob)
    {
        std::string tmp = static_cast<std::string>(blob);
        qdb::qdb_throw_if_error(
            *_handle, qdb_ts_batch_row_set_blob(_batch_table, index, static_cast<void const *>(tmp.c_str()), tmp.length()));
        ++_point_count;
    }

    void set_string(std::size_t index, const std::string & string)
    {
        qdb::qdb_throw_if_error(*_handle, qdb_ts_batch_row_set_string(_batch_table, index, string.data(), string.size()));
    }

    void set_double(std::size_t index, double v)
    {
        qdb::qdb_throw_if_error(*_handle, qdb_ts_batch_row_set_double(_batch_table, index, v));
        ++_point_count;
    }

    void set_int64(std::size_t index, std::int64_t v)
    {
        qdb::qdb_throw_if_error(*_handle, qdb_ts_batch_row_set_int64(_batch_table, index, v));
        ++_point_count;
    }

    void set_timestamp(std::size_t index, py::object v)
    {
        const qdb_timespec_t converted = convert_timestamp(v);
        qdb::qdb_throw_if_error(*_handle, qdb_ts_batch_row_set_timestamp(_batch_table, index, &converted));
        ++_point_count;
    }

    void push()
    {
        _logger.debug("pushing batch of %d rows with %d data points", _row_count, _point_count);
        qdb::qdb_throw_if_error(*_handle, qdb_ts_batch_push(_batch_table));
        _logger.debug("pushed batch of %d rows with %d data points", _row_count, _point_count);

        _reset_counters();
    }

    void push_async()
    {
        _logger.debug("async pushing batch of %d rows with %d data points", _row_count, _point_count);
        qdb::qdb_throw_if_error(*_handle, qdb_ts_batch_push_async(_batch_table));
        _logger.debug("async pushed batch of %d rows with %d data points", _row_count, _point_count);

        _reset_counters();
    }

    void push_fast()
    {
        _logger.debug("fast pushing batch of %d rows with %d data points", _row_count, _point_count);
        qdb::qdb_throw_if_error(*_handle, qdb_ts_batch_push_fast(_batch_table));
        _logger.debug("fast pushed batch of %d rows with %d data points", _row_count, _point_count);

        _reset_counters();
    }

    void push_truncate(py::kwargs args)
    {
      // As we are actively removing data, let's add an additional check to ensure the user
      // doesn't accidentally truncate his whole database without inserting anything.
      if(_row_count == 0) {
        throw qdb::invalid_argument_exception{"Batch inserter is empty: you did not provide any rows to push."};
      }

      qdb_ts_range_t tr;

      if (args.contains("range")) {
        auto range = py::cast<py::tuple>(args["range"]);
        _logger.debug("using explicit range for truncate: %s", range);
        time_range input = prep_range(py::cast<obj_time_range>(range));
        tr = convert_range(input);
      } else {
        tr =  _min_max_ts;
        // our range is end-exclusive, so let's move the pointer one nanosecond
        // *after* the last element in this batch.
        tr.end.tv_nsec++;

      }
        _logger.debug("truncate pushing batch of %d rows with %d data points, start timestamp = %d.%d, end timestamp = %d.%d", _row_count, _point_count, tr.begin.tv_sec, tr.begin.tv_nsec, tr.end.tv_sec, tr.end.tv_nsec);

        qdb::qdb_throw_if_error(*_handle, qdb_ts_batch_push_truncate(_batch_table, &tr, 1));
        _logger.debug("truncate pushed batch of %d rows with %d data points", _row_count, _point_count);

        _reset_counters();
    }

private:
    void _reset_counters()
    {
        _row_count        = 0;
        _point_count      = 0;
        _min_max_ts.begin = qdb_min_timespec;
        _min_max_ts.end   = qdb_min_timespec;
    }

private:
    qdb::logger _logger;
    qdb::handle_ptr _handle;
    qdb_batch_table_t _batch_table{nullptr};

    int64_t _row_count;
    int64_t _point_count;

    qdb_ts_range_t _min_max_ts;

};

// don't use shared_ptr, let Python do the reference counting, otherwise you will have an undefined behavior
using batch_inserter_ptr = std::unique_ptr<batch_inserter>;

template <typename Module>
static inline void register_batch_inserter(Module & m)
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

    py::class_<qdb::batch_inserter>{m, "TimeSeriesBatch"}                                                                            //
        .def(py::init<qdb::handle_ptr, const std::vector<batch_column_info> &>())                                                    //
        .def("start_row", &qdb::batch_inserter::start_row, "Calling this function marks the beginning of processing a new row.")                                                                           //
        .def("set_blob", &qdb::batch_inserter::set_blob)                                                                             //
        .def("set_string", &qdb::batch_inserter::set_string)                                                                         //
        .def("set_double", &qdb::batch_inserter::set_double)                                                                         //
        .def("set_int64", &qdb::batch_inserter::set_int64)                                                                           //
        .def("set_timestamp", &qdb::batch_inserter::set_timestamp)                                                                   //
        .def("push", &qdb::batch_inserter::push, "Regular batch push")                                                               //
        .def("push_async", &qdb::batch_inserter::push_async, "Asynchronous batch push that buffers data inside the QuasarDB daemon") //
        .def("push_fast", &qdb::batch_inserter::push_fast,
            "Fast, in-place batch push that is efficient when doing lots of small, incremental pushes.")
        .def("push_truncate", &qdb::batch_inserter::push_truncate,
            "Before inserting data, truncates any existing data. This is useful when you want your insertions to be idempotent, e.g. in case of a retry.");
}

} // namespace qdb
