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

#include "batch_column.hpp"
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
class pinned_writer
{

    using int64_column     = std::pair<std::vector<qdb_timespec_t>, std::vector<qdb_int_t>>;
    using double_column    = std::pair<std::vector<qdb_timespec_t>, std::vector<double>>;
    using timestamp_column = std::pair<std::vector<qdb_timespec_t>, std::vector<qdb_timespec_t>>;
    using blob_like_column = std::pair<std::vector<qdb_timespec_t>, std::vector<std::string>>;
    using any_column       = std::variant<int64_column, double_column, timestamp_column, blob_like_column>;

    any_column make_column(qdb_ts_column_type_t type)
    {
        switch (type)
        {
        case qdb_ts_column_int64:
            return int64_column{};
        case qdb_ts_column_double:
            return double_column{};
        case qdb_ts_column_timestamp:
            return timestamp_column{};
        case qdb_ts_column_blob:
            return blob_like_column{};
        case qdb_ts_column_string:
            return blob_like_column{};
        case qdb_ts_column_symbol:
            return blob_like_column{};
        case qdb_ts_column_uninitialized:
            throw "uninitialized column type";
        }
        return {};
    }

    constexpr const char * column_type_name(qdb_ts_column_type_t type)
    {
        switch (type)
        {
        case qdb_ts_column_int64:
            return "int64";
        case qdb_ts_column_double:
            return "double";
        case qdb_ts_column_timestamp:
            return "timestamp";
        case qdb_ts_column_blob:
            return "blob";
        case qdb_ts_column_string:
            return "string";
        case qdb_ts_column_symbol:
            return "symbol";
        case qdb_ts_column_uninitialized:
            return "uninitialized";
        }
        return "";
    }

    template <qdb_ts_column_type_t Expect>
    void check_column_type(qdb_ts_column_type_t type)
    {
        if (Expect != type)
        {
            throw qdb::exception{qdb_e_invalid_argument, "Expected another column type"};
        }
    }

public:
    pinned_writer(qdb::handle_ptr h, const std::vector<table> & tables)
        : _logger("quasardb.pinned_writer")
        , _handle{h}
        , _point_count{0}
        , _min_max_ts{qdb_min_timespec, qdb_min_timespec}
    {
        for (const auto & tbl : tables)
        {
            for (const auto & col : tbl.list_columns())
            {
                _batch_columns.push_back(batch_column_info{tbl.get_name(), col.name, 1});
                _column_types.push_back(col.type);
                _columns.push_back(make_column(col.type));
            }
        }
        std::vector<qdb_ts_batch_column_info_t> converted(_batch_columns.size());

        std::transform(_batch_columns.cbegin(), _batch_columns.cend(), converted.begin(),
            [](const batch_column_info & ci) -> qdb_ts_batch_column_info_t { return ci; });

        qdb::qdb_throw_if_error(*_handle, qdb_ts_batch_table_init(*_handle, converted.data(), converted.size(), &_batch_table));

        if (!_batch_columns.empty())
        {
            // take the shard size from any table
            // as all tables shall have the same
            qdb_ts_shard_size(*_handle, _batch_columns[0].timeseries.c_str(), reinterpret_cast<qdb_uint_t *>(&_shard_size));
        }

        _logger.debug("initialized batch reader with %d columns", _batch_columns.size());
    }

    // prevent copy because of the table object, use a unique_ptr of the batch in cluster
    // to return the object
    pinned_writer(const pinned_writer &) = delete;

    ~pinned_writer()
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
        _timestamp                     = converted;
    }

    void append_blob(std::size_t index, const py::bytes & blob)
    {
        set_blob(index, blob);
    }

    void append_blob(std::size_t index, const std::vector<py::object> & ts, const std::vector<py::bytes> & vs)
    {
        set_blob_column(index, ts, vs);
    }

    void set_blob(std::size_t index, const py::bytes & blob)
    {
        check_column_type<qdb_ts_column_blob>(_column_types[index]);
        std::string tmp = static_cast<std::string>(blob);
        auto & col      = std::get<blob_like_column>(_columns[index]);
        col.first.push_back(_timestamp);
        col.second.push_back(std::move(tmp));
        ++_point_count;
    }

    void set_blob_column(std::size_t index, const std::vector<py::object> & ts, const std::vector<py::bytes> & vs)
    {
        if (ts.size() != vs.size())
        {
            throw qdb::invalid_argument_exception{"timestamps array length does not match values array length."};
        }
        auto [timestamps, values] = _convert_blob_like_column(ts, vs);
        check_column_type<qdb_ts_column_blob>(_column_types[index]);
        auto * col = reinterpret_cast<blob_like_column *>(&_columns[index]);
        col->first.insert(
            std::end(col->first), std::make_move_iterator(std::begin(timestamps)), std::make_move_iterator(std::end(timestamps)));
        col->second.insert(std::end(col->second), std::make_move_iterator(std::begin(values)), std::make_move_iterator(std::end(values)));
        _point_count += std::size(col->first);
    }

    void append_string(std::size_t index, const std::string & blob)
    {
        set_string(index, blob);
    }

    void append_string(std::size_t index, const std::vector<py::object> & ts, const std::vector<std::string> & vs)
    {
        set_string_column(index, ts, vs);
    }

    void set_string(std::size_t index, const std::string & string)
    {
        check_column_type<qdb_ts_column_string>(_column_types[index]);
        auto & col = std::get<blob_like_column>(_columns[index]);
        col.first.push_back(_timestamp);
        col.second.push_back(string);
        ++_point_count;
    }

    void set_string_column(std::size_t index, const std::vector<py::object> & ts, const std::vector<std::string> & vs)
    {
        if (ts.size() != vs.size())
        {
            throw qdb::invalid_argument_exception{"timestamps array length does not match values array length."};
        }
        auto [timestamps, values] = _convert_blob_like_column(ts, vs);
        check_column_type<qdb_ts_column_string>(_column_types[index]);
        auto * col = reinterpret_cast<blob_like_column *>(&_columns[index]);
        col->first.insert(
            std::end(col->first), std::make_move_iterator(std::begin(timestamps)), std::make_move_iterator(std::end(timestamps)));
        col->second.insert(std::end(col->second), std::make_move_iterator(std::begin(values)), std::make_move_iterator(std::end(values)));
        _point_count += std::size(col->first);
    }

    void append_symbol(std::size_t index, const std::string & blob)
    {
        set_symbol(index, blob);
    }

    void append_symbol(std::size_t index, const std::vector<py::object> & ts, const std::vector<std::string> & vs)
    {
        set_symbol_column(index, ts, vs);
    }

    void set_symbol(std::size_t index, const std::string & symbol)
    {
        check_column_type<qdb_ts_column_symbol>(_column_types[index]);
        auto & col = std::get<blob_like_column>(_columns[index]);
        col.first.push_back(_timestamp);
        col.second.push_back(symbol);
        ++_point_count;
    }

    void set_symbol_column(std::size_t index, const std::vector<py::object> & ts, const std::vector<std::string> & vs)
    {
        if (ts.size() != vs.size())
        {
            throw qdb::invalid_argument_exception{"timestamps array length does not match values array length."};
        }
        auto [timestamps, values] = _convert_blob_like_column(ts, vs);
        check_column_type<qdb_ts_column_symbol>(_column_types[index]);
        auto * col = reinterpret_cast<blob_like_column *>(&_columns[index]);
        col->first.insert(
            std::end(col->first), std::make_move_iterator(std::begin(timestamps)), std::make_move_iterator(std::end(timestamps)));
        col->second.insert(std::end(col->second), std::make_move_iterator(std::begin(values)), std::make_move_iterator(std::end(values)));
        _point_count += std::size(col->first);
    }

    void append_double(std::size_t index, double blob)
    {
        set_double(index, blob);
    }

    void append_double(std::size_t index, const std::vector<py::object> & ts, const std::vector<double> & vs)
    {
        set_double_column(index, ts, vs);
    }

    void set_double(std::size_t index, double v)
    {
        check_column_type<qdb_ts_column_double>(_column_types[index]);
        auto & col = std::get<double_column>(_columns[index]);
        col.first.push_back(_timestamp);
        col.second.push_back(v);
        ++_point_count;
    }

    void set_double_column(std::size_t index, const std::vector<py::object> & ts, const std::vector<double> & vs)
    {
        if (ts.size() != vs.size())
        {
            throw qdb::invalid_argument_exception{"timestamps array length does not match values array length."};
        }
        auto [timestamps, values] = _convert_column(ts, vs);
        check_column_type<qdb_ts_column_double>(_column_types[index]);
        auto * col = reinterpret_cast<double_column *>(&_columns[index]);
        col->first.insert(
            std::end(col->first), std::make_move_iterator(std::begin(timestamps)), std::make_move_iterator(std::end(timestamps)));
        col->second.insert(std::end(col->second), std::make_move_iterator(std::begin(values)), std::make_move_iterator(std::end(values)));
        _point_count += std::size(col->first);
    }

    void append_int64(std::size_t index, qdb_int_t blob)
    {
        set_int64(index, blob);
    }

    void append_int64(std::size_t index, const std::vector<py::object> & ts, const std::vector<qdb_int_t> & vs)
    {
        set_int64_column(index, ts, vs);
    }

    void set_int64(std::size_t index, qdb_int_t v)
    {
        check_column_type<qdb_ts_column_int64>(_column_types[index]);
        auto * col = reinterpret_cast<int64_column *>(&_columns[index]);
        col->first.push_back(_timestamp);
        col->second.push_back(v);
        ++_point_count;
    }

    void set_int64_column(std::size_t index, const std::vector<py::object> & ts, const std::vector<qdb_int_t> & vs)
    {
        if (ts.size() != vs.size())
        {
            throw qdb::invalid_argument_exception{"timestamps array length does not match values array length."};
        }
        auto [timestamps, values] = _convert_column(ts, vs);
        check_column_type<qdb_ts_column_int64>(_column_types[index]);
        auto * col = reinterpret_cast<int64_column *>(&_columns[index]);
        col->first.insert(
            std::end(col->first), std::make_move_iterator(std::begin(timestamps)), std::make_move_iterator(std::end(timestamps)));
        col->second.insert(std::end(col->second), std::make_move_iterator(std::begin(values)), std::make_move_iterator(std::end(values)));
        _point_count += std::size(col->first);
    }

    void append_timestamp(std::size_t index, py::object blob)
    {
        set_timestamp(index, blob);
    }

    void append_timestamp(std::size_t index, const std::vector<py::object> & ts, const std::vector<py::object> & vs)
    {
        set_timestamp_column(index, ts, vs);
    }

    void set_timestamp(std::size_t index, py::object v)
    {
        const qdb_timespec_t converted = convert_timestamp(v);
        check_column_type<qdb_ts_column_timestamp>(_column_types[index]);
        auto & col = std::get<timestamp_column>(_columns[index]);
        col.first.push_back(_timestamp);
        col.second.push_back(converted);
        ++_point_count;
    }

    void set_timestamp_column(std::size_t index, const std::vector<py::object> & ts, const std::vector<py::object> & vs)
    {
        if (ts.size() != vs.size())
        {
            throw qdb::invalid_argument_exception{"timestamps array length does not match values array length."};
        }
        auto [timestamps, values] = _convert_timestamp_column(ts, vs);
        check_column_type<qdb_ts_column_timestamp>(_column_types[index]);
        auto * col = reinterpret_cast<timestamp_column *>(&_columns[index]);
        col->first.insert(
            std::end(col->first), std::make_move_iterator(std::begin(timestamps)), std::make_move_iterator(std::end(timestamps)));
        col->second.insert(std::end(col->second), std::make_move_iterator(std::begin(values)), std::make_move_iterator(std::end(values)));
        _point_count += std::size(col->first);
    }

    void push()
    {
        _logger.debug("flushing pinned columns");
        _flush_columns();
        _logger.debug("pushing pinned batch with %d data points", _point_count);
        qdb::qdb_throw_if_error(*_handle, qdb_ts_batch_push(_batch_table));
        _logger.debug("pushed pinned batch with %d data points", _point_count);

        _clear_columns();
        _reset_counters();
    }

    void push_async()
    {
        _logger.debug("flushing pinned columns");
        _flush_columns();
        _logger.debug("async pushing pinned batch with %d data points", _point_count);
        qdb::qdb_throw_if_error(*_handle, qdb_ts_batch_push_async(_batch_table));
        _logger.debug("async pushed pinned batch with %d data points", _point_count);

        _clear_columns();
        _reset_counters();
    }

    void push_fast()
    {
        _logger.debug("flushing pinned columns");
        _flush_columns();
        _logger.debug("fast pushing pinned batch with %d data points", _point_count);
        qdb::qdb_throw_if_error(*_handle, qdb_ts_batch_push_fast(_batch_table));
        _logger.debug("fast pushed pinned batch with %d data points", _point_count);

        _clear_columns();
        _reset_counters();
    }

    void push_truncate(py::kwargs args)
    {
        _logger.debug("flushing pinned columns");
        _flush_columns();
        // As we are actively removing data, let's add an additional check to ensure the user
        // doesn't accidentally truncate his whole database without inserting anything.
        if (std::all_of(std::cbegin(_columns), std::cend(_columns),
                [](const auto & column) { return std::visit([](const auto & col) { return col.first.empty(); }, column); }))
        {
            throw qdb::invalid_argument_exception{"Pinned writer is empty: you did not provide any rows to push."};
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
        _logger.debug("truncate pushing pinned batch with %d data points, start timestamp = %d.%d, end timestamp = %d.%d", _point_count,
            tr.begin.tv_sec, tr.begin.tv_nsec, tr.end.tv_sec, tr.end.tv_nsec);

        qdb::qdb_throw_if_error(*_handle, qdb_ts_batch_push_truncate(_batch_table, &tr, 1));
        _logger.debug("truncate pushed pinned batch with %d data points", _point_count);

        _clear_columns();
        _reset_counters();
    }

    const std::vector<qdb_ts_column_type_t> & column_types() const
    {
        return _column_types;
    }

private:
    template <typename T>
    std::pair<std::vector<qdb_timespec_t>, std::vector<T>> _convert_column(
        const std::vector<py::object> & timestamps, const std::vector<T> & values)
    {
        std::vector<qdb_timespec_t> ts;
        ts.reserve(std::size(timestamps));
        std::transform(std::cbegin(timestamps), std::cend(timestamps), std::back_inserter(ts),
            [](const auto & timestamp) { return convert_timestamp(timestamp); });
        return std::make_pair(std::move(ts), values);
    }

    std::pair<std::vector<qdb_timespec_t>, std::vector<qdb_timespec_t>> _convert_timestamp_column(
        const std::vector<py::object> & timestamps, const std::vector<py::object> & values)
    {
        std::vector<qdb_timespec_t> ts;
        ts.reserve(std::size(timestamps));
        std::transform(std::cbegin(timestamps), std::cend(timestamps), std::back_inserter(ts),
            [](const auto & timestamp) { return convert_timestamp(timestamp); });
        std::vector<qdb_timespec_t> vs;
        vs.reserve(std::size(values));
        std::transform(std::cbegin(values), std::cend(values), std::back_inserter(vs),
            [](const auto & timestamp) { return convert_timestamp(timestamp); });
        return std::make_pair(std::move(ts), std::move(vs));
    }

    template <typename T>
    std::pair<std::vector<qdb_timespec_t>, std::vector<std::string>> _convert_blob_like_column(
        const std::vector<py::object> & timestamps, const std::vector<T> & values)
    {
        std::vector<qdb_timespec_t> ts;
        ts.reserve(std::size(timestamps));
        std::transform(std::cbegin(timestamps), std::cend(timestamps), std::back_inserter(ts),
            [](const auto & timestamp) { return convert_timestamp(timestamp); });
        std::vector<std::string> vs;
        vs.reserve(std::size(values));
        std::transform(std::cbegin(values), std::cend(values), std::back_inserter(vs), [](const std::string & val) { return val; });
        return std::make_pair(std::move(ts), std::move(vs));
    }

    template <typename T>
    static void _sort_column(std::vector<qdb_timespec_t> & timestamps, std::vector<T> & values)
    {
        std::sort(make_zip_iterator(std::begin(timestamps), std::begin(values)), make_zip_iterator(std::end(timestamps), std::end(values)),
            [](const auto & a, const auto & b) { return std::get<0>(a) < std::get<0>(b); });
    }

    template <typename QdbColumnType, typename T>
    void _copy_value(size_t idx, QdbColumnType * data, const T & v)
    {
        data[idx] = v;
    }

    template <>
    void _copy_value(size_t idx, qdb_blob_t * data, const std::string & v)
    {
        data[idx].content        = v.c_str();
        data[idx].content_length = v.size();
    }

    template <>
    void _copy_value(size_t idx, qdb_string_t * data, const std::string & v)
    {
        data[idx].data   = v.c_str();
        data[idx].length = v.size();
    }

    template <typename QdbColumnType, typename PinColumn, typename T>
    void _pin_column(std::size_t index, std::vector<qdb_timespec_t> & timestamps, std::vector<T> & values, PinColumn && pin_column)
    {
        auto begin = make_zip_iterator(std::begin(timestamps), std::begin(values));
        auto end   = make_zip_iterator(std::end(timestamps), std::end(values));

        for (; begin < end;)
        {
            auto base_time = qdb_ts_bucket_base_time(*std::get<0>(begin), _shard_size);
            auto it        = std::find_if(begin, end, [shard_size = _shard_size, base_time](const auto & val) {
                return base_time != qdb_ts_bucket_base_time(std::get<0>(val), shard_size);
            });

            qdb_time_t * timeoffsets;
            QdbColumnType * data;
            qdb_size_t capacity      = static_cast<qdb_size_t>(std::distance(begin, it));
            qdb_timespec_t timestamp = *std::get<0>(begin);
            qdb::qdb_throw_if_error(*_handle, pin_column(_batch_table, index, capacity, &timestamp, &timeoffsets, &data));

            auto start = begin;
            for (; begin < it; ++begin)
            {
                size_t idx       = static_cast<size_t>(std::distance(start, begin));
                timeoffsets[idx] = qdb_ts_bucket_offset(*std::get<0>(begin), _shard_size);
                _copy_value(idx, data, *std::get<1>(begin));
            }
        }
    }

    void _check_timestamp_extremum(const std::vector<qdb_timespec_t> & timestamps)
    {
        if (timestamps.empty())
        {
            return;
        }
        if (_min_max_ts.begin == qdb_min_timespec && _min_max_ts.end == qdb_min_timespec)
        {
            // First row
            _min_max_ts.begin = timestamps.front();
            _min_max_ts.end   = timestamps.back();
        }
        else
        {
            _min_max_ts.begin = std::min(timestamps.front(), _min_max_ts.begin);
            _min_max_ts.end   = std::max(timestamps.back(), _min_max_ts.end);
        }
    }

    template <typename ColumnType, typename QdbColumnType, typename PinColumn>
    void _flush_column(size_t index, PinColumn && pin_column)
    {
        auto * col        = reinterpret_cast<ColumnType *>(&_columns[index]);
        auto & timestamps = col->first;
        auto & values     = col->second;
        _sort_column(timestamps, values);
        _check_timestamp_extremum(timestamps);
        _pin_column<QdbColumnType>(index, timestamps, values, std::forward<PinColumn>(pin_column));
    }

    void _flush_columns()
    {
        for (size_t index = 0; index < _columns.size(); ++index)
        {
            switch (_column_types[index])
            {
            case qdb_ts_column_int64:
                _flush_column<int64_column, qdb_int_t>(index, &qdb_ts_batch_pin_int64_column);
                break;
            case qdb_ts_column_double:
                _flush_column<double_column, double>(index, &qdb_ts_batch_pin_double_column);
                break;
            case qdb_ts_column_timestamp:
                _flush_column<timestamp_column, qdb_timespec_t>(index, &qdb_ts_batch_pin_timestamp_column);
                break;
            case qdb_ts_column_blob:
                _flush_column<blob_like_column, qdb_blob_t>(index, &qdb_ts_batch_pin_blob_column);
                break;
            case qdb_ts_column_string:
                _flush_column<blob_like_column, qdb_string_t>(index, &qdb_ts_batch_pin_string_column);
                break;
            case qdb_ts_column_symbol:
                _flush_column<blob_like_column, qdb_string_t>(index, &qdb_ts_batch_pin_symbol_column);
                break;
            case qdb_ts_column_uninitialized:
                throw qdb::invalid_argument_exception{std::string{"Uninitialized column at index "} + std::to_string(index)};
            }
        }
    }

    void _clear_columns()
    {
        for (auto & column : _columns)
        {
            std::visit(
                [](auto & col) {
                    col.first.clear();
                    col.second.clear();
                },
                column);
        }
    }

    void _reset_counters()
    {
        _point_count      = 0;
        _min_max_ts.begin = qdb_min_timespec;
        _min_max_ts.end   = qdb_min_timespec;
    }

private:
    qdb::logger _logger;
    qdb::handle_ptr _handle;
    qdb_batch_table_t _batch_table{nullptr};
    std::vector<qdb_ts_column_type_t> _column_types;
    std::vector<any_column> _columns;
    std::vector<batch_column_info> _batch_columns;

    qdb_timespec_t _timestamp;
    qdb_duration_t _shard_size;

    int64_t _point_count;

    qdb_ts_range_t _min_max_ts;
};

// don't use shared_ptr, let Python do the reference counting, otherwise you will have an undefined behavior
using pinned_writer_ptr = std::unique_ptr<pinned_writer>;

template <typename Module>
static inline void register_pinned_writer(Module & m)
{
    namespace py = pybind11;

    py::class_<qdb::pinned_writer>{m, "PinnedWriter"}                                                                           //
        .def(py::init<qdb::handle_ptr, const std::vector<table> &>())                                                           //
        .def("column_types", &qdb::pinned_writer::column_types)                                                                 //
        .def("start_row", &qdb::pinned_writer::start_row, "Calling this function marks the beginning of processing a new row.") //
        .def("append_blob", py::overload_cast<size_t, const py::bytes &>(&qdb::pinned_writer::append_blob))                     //
        .def("append_blob", py::overload_cast<std::size_t, const std::vector<py::object> &, const std::vector<py::bytes> &>(
                                &qdb::pinned_writer::append_blob))                                                     //
        .def("append_string", py::overload_cast<std::size_t, const std::string &>(&qdb::pinned_writer::append_string)) //
        .def("append_string", py::overload_cast<std::size_t, const std::vector<py::object> &, const std::vector<std::string> &>(
                                  &qdb::pinned_writer::append_string))                                                 //
        .def("append_symbol", py::overload_cast<std::size_t, const std::string &>(&qdb::pinned_writer::append_symbol)) //
        .def("append_symbol", py::overload_cast<std::size_t, const std::vector<py::object> &, const std::vector<std::string> &>(
                                  &qdb::pinned_writer::append_symbol))                                    //
        .def("append_double", py::overload_cast<std::size_t, double>(&qdb::pinned_writer::append_double)) //
        .def("append_double", py::overload_cast<std::size_t, const std::vector<py::object> &, const std::vector<double> &>(
                                  &qdb::pinned_writer::append_double))                                     //
        .def("append_int64", py::overload_cast<std::size_t, qdb_int_t>(&qdb::pinned_writer::append_int64)) //
        .def("append_int64", py::overload_cast<std::size_t, const std::vector<py::object> &, const std::vector<qdb_int_t> &>(
                                 &qdb::pinned_writer::append_int64))                                                //
        .def("append_timestamp", py::overload_cast<std::size_t, py::object>(&qdb::pinned_writer::append_timestamp)) //
        .def("append_timestamp", py::overload_cast<std::size_t, const std::vector<py::object> &, const std::vector<py::object> &>(
                                     &qdb::pinned_writer::append_timestamp))    //
        .def("set_blob", &qdb::pinned_writer::set_blob)                         //
        .def("set_string", &qdb::pinned_writer::set_string)                     //
        .def("set_symbol", &qdb::pinned_writer::set_symbol)                     //
        .def("set_double", &qdb::pinned_writer::set_double)                     //
        .def("set_int64", &qdb::pinned_writer::set_int64)                       //
        .def("set_timestamp", &qdb::pinned_writer::set_timestamp)               //
        .def("set_blob_column", &qdb::pinned_writer::set_blob_column)           //
        .def("set_double_column", &qdb::pinned_writer::set_double_column)       //
        .def("set_int64_column", &qdb::pinned_writer::set_int64_column)         //
        .def("set_string_column", &qdb::pinned_writer::set_string_column)       //
        .def("set_symbol_column", &qdb::pinned_writer::set_symbol_column)       //
        .def("set_timestamp_column", &qdb::pinned_writer::set_timestamp_column) //
        .def("push", &qdb::pinned_writer::push, "Regular batch push")           //
        .def("push_async", &qdb::pinned_writer::push_async,
            "Asynchronous batch push that buffers data inside the QuasarDB daemon") //
        .def("push_fast", &qdb::pinned_writer::push_fast,
            "Fast, in-place batch push that is efficient when doing lots of small, incremental pushes.")
        .def("push_truncate", &qdb::pinned_writer::push_truncate,
            "Before inserting data, truncates any existing data. This is useful when you want your insertions to be idempotent, e.g. in "
            "case of a retry.");
}

} // namespace qdb
