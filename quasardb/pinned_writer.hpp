/*
 *
 * Official Python API
 *
 * Copyright (c) 2009-2021, quasardb SAS. All rights reserved.
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
#include "table.hpp"
#include "ts_iterator.hpp"
#include "utils.hpp"
#include <vector>

namespace qdb
{

namespace detail
{

template <typename T>
T null_value();

template <>
qdb_int_t null_value()
{
    return (qdb_int_t)0x8000000000000000ll;
}

template <>
double null_value()
{
    return NAN;
}

template <>
qdb_timespec_t null_value()
{
    return qdb_timespec_t{qdb_min_time, qdb_min_time};
}

template <>
std::string null_value()
{
    return "";
}

using int64_column     = std::pair<std::vector<qdb_timespec_t>, std::vector<qdb_int_t>>;
using double_column    = std::pair<std::vector<qdb_timespec_t>, std::vector<double>>;
using timestamp_column = std::pair<std::vector<qdb_timespec_t>, std::vector<qdb_timespec_t>>;
using blob_like_column = std::pair<std::vector<qdb_timespec_t>, std::vector<std::string>>;
struct any_column
{
    int64_column ints;
    double_column doubles;
    timestamp_column timestamps;
    blob_like_column blobs;
};

template <typename ColumnType>
ColumnType * access_column(std::vector<any_column> & columns, size_t index);

template <>
int64_column * access_column<int64_column>(std::vector<any_column> & columns, size_t index)
{
    return &columns[index].ints;
}

template <>
double_column * access_column<double_column>(std::vector<any_column> & columns, size_t index)
{
    return &columns[index].doubles;
}

template <>
timestamp_column * access_column<timestamp_column>(std::vector<any_column> & columns, size_t index)
{
    return &columns[index].timestamps;
}

template <>
blob_like_column * access_column<blob_like_column>(std::vector<any_column> & columns, size_t index)
{
    return &columns[index].blobs;
}

} // namespace detail

class pinned_writer
{

    using int64_column     = detail::int64_column;
    using double_column    = detail::double_column;
    using timestamp_column = detail::timestamp_column;
    using blob_like_column = detail::blob_like_column;
    using any_column       = detail::any_column;

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
                _columns.push_back(_make_column(col.type));
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

    std::vector<batch_column_info> batch_column_infos() const
    {
        return _batch_columns;
    }

    void start_row(py::object ts)
    {
        const qdb_timespec_t converted = convert_timestamp(ts);
        _timestamp                     = converted;
    }

    void set_blob(std::size_t index, const py::object & val)
    {
        _set_impl<qdb_ts_column_blob, blob_like_column>(
            index, (val.is(py::none()) ? detail::null_value<std::string>() : val.cast<std::string>()));
    }

    void set_blob_column(std::size_t index, const std::vector<py::object> & ts, const std::vector<py::object> & vs)
    {
        _set_column_impl<qdb_ts_column_blob, blob_like_column, std::string>(index, ts, vs, &_convert_blob_like_column);
    }

    void set_string(std::size_t index, const py::object & val)
    {
        _set_impl<qdb_ts_column_string, blob_like_column>(
            index, (val.is(py::none()) ? detail::null_value<std::string>() : val.cast<std::string>()));
    }

    void set_string_column(std::size_t index, const std::vector<py::object> & ts, const std::vector<py::object> & vs)
    {
        _set_column_impl<qdb_ts_column_string, blob_like_column, std::string>(index, ts, vs, &_convert_blob_like_column);
    }

    void set_double(std::size_t index, const py::object & val)
    {
        _set_impl<qdb_ts_column_double, double_column>(index, (val.is(py::none()) ? detail::null_value<double>() : val.cast<double>()));
    }

    void set_double_column(std::size_t index, const std::vector<py::object> & ts, const std::vector<py::object> & vs)
    {
        _set_column_impl<qdb_ts_column_double, double_column, double>(index, ts, vs, &_convert_double_column);
    }

    void set_int64(std::size_t index, const py::object & val)
    {
        _set_impl<qdb_ts_column_int64, int64_column>(index, (val.is(py::none()) ? detail::null_value<qdb_int_t>() : val.cast<qdb_int_t>()));
    }

    void set_int64_column(std::size_t index, const std::vector<py::object> & ts, const std::vector<py::object> & vs)
    {
        _set_column_impl<qdb_ts_column_int64, int64_column, qdb_int_t>(index, ts, vs, &_convert_int64_column);
    }

    void set_timestamp(std::size_t index, const py::object & val)
    {
        _set_impl<qdb_ts_column_timestamp, timestamp_column>(
            index, (val.is(py::none()) ? detail::null_value<qdb_timespec_t>() : convert_timestamp(val)));
    }

    void set_timestamp_column(std::size_t index, const std::vector<py::object> & ts, const std::vector<py::object> & vs)
    {
        _set_column_impl<qdb_ts_column_timestamp, timestamp_column, qdb_timespec_t>(index, ts, vs, &_convert_timestamp_column);
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
        if (_empty_columns())
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
    any_column _make_column(qdb_ts_column_type_t type)
    {
        any_column col;
        switch (type)
        {
        case qdb_ts_column_int64:
            col.ints = int64_column{};
            break;
        case qdb_ts_column_double:
            col.doubles = double_column{};
            break;
        case qdb_ts_column_timestamp:
            col.timestamps = timestamp_column{};
            break;
        case qdb_ts_column_blob:
            col.blobs = blob_like_column{};
            break;
        case qdb_ts_column_string:
            col.blobs = blob_like_column{};
            break;
        case qdb_ts_column_uninitialized:
            throw "uninitialized column type";
        }
        return {};
    }

    template <qdb_ts_column_type_t Expect>
    void _check_column_type(qdb_ts_column_type_t type)
    {
        if (Expect != type)
        {
            throw qdb::exception{qdb_e_invalid_argument, "Expected another column type"};
        }
    }

    template <typename T>
    static std::pair<std::vector<qdb_timespec_t>, std::vector<T>> _convert_column(
        const std::vector<py::object> & timestamps, const std::vector<py::object> & values)
    {
        std::vector<qdb_timespec_t> ts;
        ts.reserve(timestamps.size());
        std::transform(std::cbegin(timestamps), std::cend(timestamps), std::back_inserter(ts),
            [](const auto & timestamp) { return convert_timestamp(timestamp); });

        std::vector<T> vs;
        vs.reserve(values.size());
        std::transform(std::cbegin(values), std::cend(values), std::back_inserter(vs),
            [](const py::object & val) {
                if (val.is(py::none()))
                {
                    return detail::null_value<T>();
                }
                auto v = val.cast<double>();
                return (v == std::numeric_limits<double>::quiet_NaN() ? detail::null_value<T>() : static_cast<T>(v));
            });
        return std::make_pair(std::move(ts), std::move(vs));
    }

    static std::pair<std::vector<qdb_timespec_t>, std::vector<qdb_int_t>> _convert_int64_column(
        const std::vector<py::object> & timestamps, const std::vector<py::object> & values)
    {
        return _convert_column<qdb_int_t>(timestamps, values);
    }

    static std::pair<std::vector<qdb_timespec_t>, std::vector<double>> _convert_double_column(
        const std::vector<py::object> & timestamps, const std::vector<py::object> & values)
    {
        return _convert_column<double>(timestamps, values);
    }

    static std::pair<std::vector<qdb_timespec_t>, std::vector<qdb_timespec_t>> _convert_timestamp_column(
        const std::vector<py::object> & timestamps, const std::vector<py::object> & values)
    {
        std::vector<qdb_timespec_t> ts;
        ts.reserve(timestamps.size());
        std::transform(std::cbegin(timestamps), std::cend(timestamps), std::back_inserter(ts),
            [](const auto & timestamp) { return convert_timestamp(timestamp); });
        std::vector<qdb_timespec_t> vs;
        vs.reserve(values.size());
        std::transform(std::cbegin(values), std::cend(values), std::back_inserter(vs),
            [](const py::object & val) { return (val.is(py::none()) ? detail::null_value<qdb_timespec_t>() : convert_timestamp(val)); });
        return std::make_pair(std::move(ts), std::move(vs));
    }

    static std::pair<std::vector<qdb_timespec_t>, std::vector<std::string>> _convert_blob_like_column(
        const std::vector<py::object> & timestamps, const std::vector<py::object> & values)
    {
        std::vector<qdb_timespec_t> ts;
        ts.reserve(timestamps.size());
        std::transform(std::cbegin(timestamps), std::cend(timestamps), std::back_inserter(ts),
            [](const auto & timestamp) { return convert_timestamp(timestamp); });
        std::vector<std::string> vs;
        vs.reserve(values.size());
        std::transform(std::cbegin(values), std::cend(values), std::back_inserter(vs),
            [](const py::object & val) { return (val.is(py::none()) ? detail::null_value<std::string>() : val.cast<std::string>()); });
        return std::make_pair(std::move(ts), std::move(vs));
    }

    template <typename T>
    static void _sort_column(std::vector<qdb_timespec_t> & timestamps, std::vector<T> & values)
    {
        std::sort(make_ts_iterator(std::begin(timestamps), std::begin(values)), make_ts_iterator(std::end(timestamps), std::end(values)),
            [](const auto & a, const auto & b) { return std::get<0>(a) < std::get<0>(b); });
    }

    template <typename QdbColumnType, typename T>
    void _copy_value(size_t idx, QdbColumnType * data, const T & val)
    {
        data[idx] = val;
    }

    void _copy_value(size_t idx, qdb_blob_t * data, const std::string & val)
    {
        data[idx].content        = val.c_str();
        data[idx].content_length = val.size();
    }

    void _copy_value(size_t idx, qdb_string_t * data, const std::string & val)
    {
        data[idx].data   = val.c_str();
        data[idx].length = val.size();
    }

    template <typename QdbColumnType, typename PinColumn, typename T>
    void _pin_column(std::size_t index, std::vector<qdb_timespec_t> & timestamps, std::vector<T> & values, PinColumn && pin_column)
    {
        auto begin = make_ts_iterator(std::begin(timestamps), std::begin(values));
        auto end   = make_ts_iterator(std::end(timestamps), std::end(values));

        for (; begin < end;)
        {
            auto base_time = qdb_ts_bucket_base_time(*qdb::get<0>(begin), _shard_size);
            auto it        = std::find_if(begin, end, [shard_size = _shard_size, base_time](const auto & val) {
                return base_time != qdb_ts_bucket_base_time(std::get<0>(val), shard_size);
            });

            qdb_time_t * timeoffsets;
            QdbColumnType * data;
            qdb_size_t capacity      = static_cast<qdb_size_t>(std::distance(begin, it));
            qdb_timespec_t timestamp = *qdb::get<0>(begin);
            qdb::qdb_throw_if_error(*_handle, pin_column(_batch_table, index, capacity, &timestamp, &timeoffsets, &data));

            auto start = begin;
            for (; begin < it; ++begin)
            {
                size_t idx       = static_cast<size_t>(std::distance(start, begin));
                timeoffsets[idx] = qdb_ts_bucket_offset(*qdb::get<0>(begin), _shard_size);
                _copy_value(idx, data, *qdb::get<1>(begin));
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
        auto * col        = detail::access_column<ColumnType>(_columns, index);
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
            case qdb_ts_column_uninitialized:
                throw qdb::invalid_argument_exception{std::string{"Uninitialized column at index "} + std::to_string(index)};
            }
        }
    }

    template <typename ColumnType>
    bool _empty_column(size_t index)
    {
        auto * col = detail::access_column<ColumnType>(_columns, index);
        return col->first.empty();
    }

    bool _empty_columns()
    {
        bool is_empty = true;
        for (size_t index = 0; index < _columns.size(); ++index)
        {
            switch (_column_types[index])
            {
            case qdb_ts_column_int64:
                is_empty = _empty_column<int64_column>(index);
                break;
            case qdb_ts_column_double:
                is_empty = _empty_column<double_column>(index);
                break;
            case qdb_ts_column_timestamp:
                is_empty = _empty_column<timestamp_column>(index);
                break;
            case qdb_ts_column_blob:
                is_empty = _empty_column<blob_like_column>(index);
                break;
            case qdb_ts_column_string:
                is_empty = _empty_column<blob_like_column>(index);
                break;
            case qdb_ts_column_uninitialized:
                throw qdb::invalid_argument_exception{std::string{"Uninitialized column at index "} + std::to_string(index)};
            }
            if (!is_empty)
            {
                return false;
            }
        }
        return true;
    }

    template <typename ColumnType>
    void _clear_column(size_t index)
    {
        auto * col = detail::access_column<ColumnType>(_columns, index);
        col->first.clear();
        col->second.clear();
    }

    void _clear_columns()
    {
        for (size_t index = 0; index < _columns.size(); ++index)
        {
            switch (_column_types[index])
            {
            case qdb_ts_column_int64:
                _clear_column<int64_column>(index);
                break;
            case qdb_ts_column_double:
                _clear_column<double_column>(index);
                break;
            case qdb_ts_column_timestamp:
                _clear_column<timestamp_column>(index);
                break;
            case qdb_ts_column_blob:
                _clear_column<blob_like_column>(index);
                break;
            case qdb_ts_column_string:
                _clear_column<blob_like_column>(index);
                break;
            case qdb_ts_column_uninitialized:
                throw qdb::invalid_argument_exception{std::string{"Uninitialized column at index "} + std::to_string(index)};
            }
        }
    }

    template <qdb_ts_column_type_t Expect, typename ColumnType, typename T>
    void _set_impl(std::size_t index, T val)
    {
        _check_column_type<Expect>(_column_types[index]);
        auto * col = detail::access_column<ColumnType>(_columns, index);
        col->first.push_back(_timestamp);
        col->second.push_back(val);
        ++_point_count;
    }

    template <qdb_ts_column_type_t Expect, typename ColumnType, typename ValueType, typename Convert>
    void _set_column_impl(std::size_t index, const std::vector<py::object> & ts, const std::vector<py::object> & vs, Convert && convert)
    {
        if (ts.size() != vs.size())
        {
            throw qdb::invalid_argument_exception{"timestamps array length does not match values array length."};
        }
        std::vector<qdb_timespec_t> timestamps;
        std::vector<ValueType> values;
        std::tie(timestamps, values) = convert(ts, vs);
        _check_column_type<Expect>(_column_types[index]);
        auto * col = detail::access_column<ColumnType>(_columns, index);
        col->first.insert(
            std::end(col->first), std::make_move_iterator(std::begin(timestamps)), std::make_move_iterator(std::end(timestamps)));
        col->second.insert(std::end(col->second), std::make_move_iterator(std::begin(values)), std::make_move_iterator(std::end(values)));
        _point_count += col->first.size();
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
        .def("batch_column_infos", &qdb::pinned_writer::batch_column_infos)                                                       //
        .def("start_row", &qdb::pinned_writer::start_row, "Calling this function marks the beginning of processing a new row.") //
        .def("set_blob", &qdb::pinned_writer::set_blob)                                                                         //
        .def("set_string", &qdb::pinned_writer::set_string)                                                                     //
        .def("set_double", &qdb::pinned_writer::set_double)                                                                     //
        .def("set_int64", &qdb::pinned_writer::set_int64)                                                                       //
        .def("set_timestamp", &qdb::pinned_writer::set_timestamp)                                                               //
        .def("set_blob_column", &qdb::pinned_writer::set_blob_column)                                                           //
        .def("set_double_column", &qdb::pinned_writer::set_double_column)                                                       //
        .def("set_int64_column", &qdb::pinned_writer::set_int64_column)                                                         //
        .def("set_string_column", &qdb::pinned_writer::set_string_column)                                                       //
        .def("set_timestamp_column", &qdb::pinned_writer::set_timestamp_column)                                                 //
        .def("push", &qdb::pinned_writer::push, "Regular batch push")                                                           //
        .def("push_async", &qdb::pinned_writer::push_async,
            "Asynchronous batch push that buffers data inside the QuasarDB daemon") //
        .def("push_fast", &qdb::pinned_writer::push_fast,
            "Fast, in-place batch push that is efficient when doing lots of small, incremental pushes.")
        .def("push_truncate", &qdb::pinned_writer::push_truncate,
            "Before inserting data, truncates any existing data. This is useful when you want your insertions to be idempotent, e.g. in "
            "case of a retry.");
}

} // namespace qdb
