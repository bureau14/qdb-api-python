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
#include "logger.hpp"
#include "overload.hpp"
#include "table.hpp"
#include "utils.hpp"
#include "zip_iterator.hpp"
#include <variant>
#include <vector>

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
    using pinned_int64_column  = std::pair<std::vector<qdb_timespec_t>, std::vector<std::int64_t>>;
    using pinned_double_column = std::pair<std::vector<qdb_timespec_t>, std::vector<double>>;
    using pinned_column_type   = std::variant<pinned_int64_column, pinned_double_column>;

public:
    batch_inserter(qdb::handle_ptr h, const std::vector<batch_column_info> & ci)
        : _logger("quasardb.batch_inserter")
        , _handle{h}
        , _row_count{0}
        , _point_count{0}
        , _min_max_ts{qdb_min_timespec, qdb_min_timespec}
    {
        std::vector<qdb_ts_batch_column_info_t> converted(ci.size());

        std::transform(
            ci.cbegin(), ci.cend(), converted.begin(), [](const batch_column_info & ci) -> qdb_ts_batch_column_info_t { return ci; });

        qdb::qdb_throw_if_error(*_handle, qdb_ts_batch_table_init(*_handle, converted.data(), converted.size(), &_batch_table));

        _pinned_columns.resize(ci.size());

        if (!ci.empty())
        {
            // take the shard size from any table
            // as all tables shall have the same
            qdb_ts_shard_size(*_handle, ci[0].timeseries.c_str(), reinterpret_cast<qdb_uint_t *>(&_shard_size));
        }

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
        if (_min_max_ts.begin == qdb_min_timespec && _min_max_ts.end == qdb_min_timespec)
        {
            assert(_row_count == 0); // sanity check

            // First row
            _min_max_ts.begin = converted;
            _min_max_ts.end   = converted;
        }
        else
        {
            assert(_row_count > 0);
            _min_max_ts.begin = std::min(converted, _min_max_ts.begin);
            _min_max_ts.end   = std::max(converted, _min_max_ts.end);
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

    void set_symbol(std::size_t index, const std::string & symbol)
    {
        qdb::qdb_throw_if_error(*_handle, qdb_ts_batch_row_set_symbol(_batch_table, index, symbol.data(), symbol.size()));
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
        if (_row_count == 0)
        {
            throw qdb::invalid_argument_exception{"Batch inserter is empty: you did not provide any rows to push."};
        }

        qdb_ts_range_t tr;

        if (args.contains("range"))
        {
            auto range = py::cast<py::tuple>(args["range"]);
            _logger.debug("using explicit range for truncate: %s", range);
            time_range input = prep_range(py::cast<obj_time_range>(range));
            tr               = convert_range(input);
        }
        else
        {
            tr = _min_max_ts;
            // our range is end-exclusive, so let's move the pointer one nanosecond
            // *after* the last element in this batch.
            tr.end.tv_nsec++;
        }
        _logger.debug("truncate pushing batch of %d rows with %d data points, start timestamp = %d.%d, end timestamp = %d.%d", _row_count,
            _point_count, tr.begin.tv_sec, tr.begin.tv_nsec, tr.end.tv_sec, tr.end.tv_nsec);

        qdb::qdb_throw_if_error(*_handle, qdb_ts_batch_push_truncate(_batch_table, &tr, 1));
        _logger.debug("truncate pushed batch of %d rows with %d data points", _row_count, _point_count);

        _reset_counters();
    }

private:
    template <typename T>
    std::pair<std::vector<qdb_timespec_t>, std::vector<T>> _convert_column(
        const std::vector<py::object> & timestamps, const pybind11::array_t<T> & values)
    {
        std::vector<qdb_timespec_t> ts;
        ts.reserve(std::size(timestamps));
        std::transform(std::cbegin(timestamps), std::cend(timestamps), std::back_inserter(ts),
            [](const auto & timestamp) { return convert_timestamp(timestamp); });
        std::vector<std::int64_t> vs(values.size());
        auto v = values.template unchecked<1>();
        for (size_t i = 0; i < values.size(); ++i)
        {
            vs[i] = v(i);
        }
        return std::make_pair(std::move(ts), std::move(vs));
    }

    template <typename T>
    static void _sort_column(std::vector<qdb_timespec_t> & timestamps, std::vector<T> & values)
    {
        std::sort(make_zip_iterator(std::begin(timestamps), std::begin(values)), make_zip_iterator(std::end(timestamps), std::end(values)),
            [](const auto & a, const auto & b) { return std::get<0>(a) < std::get<0>(b); });
    }

    template <typename PinColumn, typename T>
    void _pin_column(std::size_t index, std::vector<qdb_timespec_t> & timestamps, std::vector<T> & values, PinColumn && pin_column)
    {
        auto begin     = make_zip_iterator(std::begin(timestamps), std::begin(values));
        auto end       = make_zip_iterator(std::end(timestamps), std::end(values));
        auto base_time = qdb_ts_bucket_base_time(*std::get<0>(begin), _shard_size);
        for (; begin < end;)
        {
            auto it = std::find_if(begin, end, [shard_size = _shard_size, base_time](const auto & val) {
                return base_time != qdb_ts_bucket_base_time(std::get<0>(val), shard_size);
            });
            // copy begin to it
            qdb_time_t * timeoffsets;
            T * data;
            qdb_size_t capacity = static_cast<qdb_size_t>(std::distance(begin, it));
            auto err            = pin_column(_batch_table, index, capacity, &(*std::get<0>(begin)), &timeoffsets, &data);
            size_t idx          = 0;
            for (; begin < it; ++begin)
            {
                timeoffsets[idx] = qdb_ts_bucket_offset(*std::get<0>(begin), _shard_size);
                data[idx]        = *std::get<1>(begin);
                ++idx;
            }
            base_time = qdb_ts_bucket_base_time(*std::get<0>(it), _shard_size);
        }
    }

public:
    void set_pinned_int64_column(std::size_t index, const std::vector<py::object> & ts, const pybind11::array_t<qdb_int_t> & vs)
    {
        auto [timestamps, values] = _convert_column(ts, vs);
        _sort_column(timestamps, values);
        _pin_column(index, timestamps, values, &qdb_ts_batch_pin_int64_column);
    }

    void set_pinned_double_column(std::size_t index, const pybind11::array_t<std::int64_t> & values)
    {}

    void pinned_push()
    {
        push();
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

    qdb_duration_t _shard_size;
    std::vector<pinned_column_type> _pinned_columns;

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
        .def("start_row", &qdb::batch_inserter::start_row, "Calling this function marks the beginning of processing a new row.")     //
        .def("set_blob", &qdb::batch_inserter::set_blob)                                                                             //
        .def("set_string", &qdb::batch_inserter::set_string)                                                                         //
        .def("set_symbol", &qdb::batch_inserter::set_symbol)                                                                         //
        .def("set_double", &qdb::batch_inserter::set_double)                                                                         //
        .def("set_int64", &qdb::batch_inserter::set_int64)                                                                           //
        .def("set_timestamp", &qdb::batch_inserter::set_timestamp)                                                                   //
        .def("set_pinned_int64_column", &qdb::batch_inserter::set_pinned_int64_column)                                               //
        .def("pinned_push", &qdb::batch_inserter::pinned_push)                                                                       //
        .def("push", &qdb::batch_inserter::push, "Regular batch push")                                                               //
        .def("push_async", &qdb::batch_inserter::push_async, "Asynchronous batch push that buffers data inside the QuasarDB daemon") //
        .def("push_fast", &qdb::batch_inserter::push_fast,
            "Fast, in-place batch push that is efficient when doing lots of small, incremental pushes.")
        .def("push_truncate", &qdb::batch_inserter::push_truncate,
            "Before inserting data, truncates any existing data. This is useful when you want your insertions to be idempotent, e.g. in "
            "case of a retry.");
}

} // namespace qdb
