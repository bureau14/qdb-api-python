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
#include "utils/blob_deque.hpp"
#include "utils/ostream.hpp"
#include "utils/permutation.hpp"
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

template <>
qdb_blob_t null_value()
{
    return qdb_blob_t{nullptr, 0};
}

using int64_column     = std::vector<qdb_int_t>;
using double_column    = std::vector<double>;
using timestamp_column = std::vector<qdb_timespec_t>;
using blob_like_column = std::vector<qdb_blob_t>;
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
    pinned_writer(qdb::handle_ptr h, const table & table)
        : _logger("quasardb.pinned_writer")
        , _handle{h}
        , _table_name{table.get_name()}
    {
        _column_infos = table.list_columns();
        for (const auto & col : _column_infos)
        {
            _columns.push_back(_make_column(col.type));
        }
    }

    // prevent copy because of the table object, use a unique_ptr of the batch in cluster
    // to return the object
    pinned_writer(const pinned_writer &) = delete;

    ~pinned_writer()
    {
        if (_handle)
        {
            qdb_release(*_handle, _table_schemas);
        }
    }

    void start_row(py::object ts)
    {
        _timestamps.push_back(convert_timestamp(ts));
    }

    void set_blob(std::size_t index, const py::object & val)
    {
        _set_blob_impl<qdb_ts_column_blob, blob_like_column>(index, val);
    }

    void set_timestamps(const std::vector<py::object> & timestamps)
    {
        _timestamps.reserve(timestamps.size());
        std::transform(std::cbegin(timestamps), std::cend(timestamps), std::back_inserter(_timestamps),
            [](const auto & timestamp) { return convert_timestamp(timestamp); });
    }

    void set_blob_column(std::size_t index, const std::vector<py::object> & vs)
    {
        _set_blob_column_impl<qdb_ts_column_blob, blob_like_column, qdb_blob_t>(index, vs);
    }

    void set_string(std::size_t index, const py::object & val)
    {
        _set_blob_impl<qdb_ts_column_string, blob_like_column>(index, val);
    }

    void set_string_column(std::size_t index, const std::vector<py::object> & vs)
    {
        _set_blob_column_impl<qdb_ts_column_string, blob_like_column, qdb_blob_t>(index, vs);
    }

    void set_symbol(std::size_t index, const py::object & val)
    {
        _set_blob_impl<qdb_ts_column_symbol, blob_like_column>(index, val);
    }

    void set_symbol_column(std::size_t index, const std::vector<py::object> & vs)
    {
        _set_blob_column_impl<qdb_ts_column_symbol, blob_like_column, qdb_blob_t>(index, vs);
    }

    void set_double(std::size_t index, const py::object & val)
    {
        _set_impl<qdb_ts_column_double, double_column>(index, (val.is(py::none()) ? detail::null_value<double>() : val.cast<double>()));
    }

    void set_double_column(std::size_t index, const std::vector<py::object> & vs)
    {
        _set_column_impl<qdb_ts_column_double, double_column, double>(index, vs, &_convert_double_column);
    }

    void set_int64(std::size_t index, const py::object & val)
    {
        _set_impl<qdb_ts_column_int64, int64_column>(index, (val.is(py::none()) ? detail::null_value<qdb_int_t>() : val.cast<qdb_int_t>()));
    }

    void set_int64_column(std::size_t index, const std::vector<py::object> & vs)
    {
        _set_column_impl<qdb_ts_column_int64, int64_column, qdb_int_t>(index, vs, &_convert_int64_column);
    }

    void set_timestamp(std::size_t index, const py::object & val)
    {
        _set_impl<qdb_ts_column_timestamp, timestamp_column>(index, convert_timestamp(val));
    }

    void set_timestamp_column(std::size_t index, const std::vector<py::object> & vs)
    {
        _set_column_impl<qdb_ts_column_timestamp, timestamp_column, qdb_timespec_t>(index, vs, &_convert_timestamp_column);
    }

    const std::vector<qdb_exp_batch_push_column_t> & prepare_columns()
    {
        _columns_data.clear();
        _columns_data.reserve(_columns.size());
        for (size_t index = 0; index < _columns.size(); ++index)
        {
            const auto & column_name = _column_infos[index].name;

            qdb_exp_batch_push_column_t column;
            column.name = qdb_string_t{column_name.data(), column_name.size()};
            const auto data_type = _column_infos[index].type;
            column.data_type = data_type != qdb_ts_column_symbol ? data_type : qdb_ts_column_string;
            switch (_column_infos[index].type)
            {
            case qdb_ts_column_int64:
                column.data.ints = detail::access_column<int64_column>(_columns, index)->data();
                break;
            case qdb_ts_column_double:
                column.data.doubles = detail::access_column<double_column>(_columns, index)->data();
                break;
            case qdb_ts_column_timestamp:
                column.data.timestamps = detail::access_column<timestamp_column>(_columns, index)->data();
                break;
            case qdb_ts_column_blob:
                column.data.blobs = detail::access_column<blob_like_column>(_columns, index)->data();
            case qdb_ts_column_string:
            case qdb_ts_column_symbol:
                column.data.strings = reinterpret_cast<const qdb_string_t *>(detail::access_column<blob_like_column>(_columns, index)->data());
                break;
            case qdb_ts_column_uninitialized:
                throw qdb::invalid_argument_exception{std::string{"Uninitialized column at index "} + std::to_string(index)};
            }
            if (column.data.ints != nullptr)
            {
                _columns_data.push_back(column);
            }
        }
        return _columns_data;
    }

    qdb_exp_batch_push_table_data_t prepare_table_data()
    {
        qdb_exp_batch_push_table_data_t table_data;
        table_data.row_count = _timestamps.size(),
        table_data.timestamps = _timestamps.data();

        const auto & columns = prepare_columns();
        table_data.columns = columns.data();
        table_data.column_count = columns.size();

        return table_data;
    }

    qdb_exp_batch_push_table_t prepare_batch(qdb_exp_batch_push_mode_t mode, qdb_ts_range_t * ranges)
    {
        qdb_exp_batch_push_table_t batch;
        batch.name = qdb_string_t{_table_name.data(), _table_name.size()};
        batch.data = prepare_table_data();
        if (mode == qdb_exp_batch_push_truncate)
        {
            batch.truncate_ranges = ranges;
            batch.truncate_range_count = ranges == nullptr ? 0u : 1u;
        }
        batch.options = qdb_exp_batch_option_standard;
        return batch;
    }

    void _push_impl(qdb_exp_batch_push_mode_t mode, qdb_ts_range_t * ranges = nullptr)
    {
        _logger.debug("[push] Sorting dataset...");
        _sort_columns();
        _logger.debug("[push] Preparing batch...");
        auto batch = prepare_batch(mode, ranges);
        _logger.debug("Pushing %d rows with %d columns in %s", batch.data.row_count, batch.data.column_count, _table_name);
        qdb::qdb_throw_if_error(*_handle, qdb_exp_batch_push(*_handle, mode, &batch, &_table_schemas, 1u));
        _clear_columns();
    }

    void push()
    {
        _push_impl(qdb_exp_batch_push_transactional);
    }

    void push_async()
    {
        _push_impl(qdb_exp_batch_push_async);
    }

    void push_fast()
    {
        _push_impl(qdb_exp_batch_push_fast);
    }

    void push_truncate(py::kwargs args)
    {
        // As we are actively removing data, let's add an additional check to ensure the user
        // doesn't accidentally truncate his whole database without inserting anything.
        if (_empty_columns())
        {
            throw qdb::invalid_argument_exception{"Pinned writer is empty: you did not provide any rows to push."};
        }

        qdb_ts_range_t tr{_timestamps.front(), _timestamps.back()};
        if (args.contains("range"))
        {
            auto range = py::cast<py::tuple>(args["range"]);
            _logger.debug("[push] using explicit range for truncate: %s", range);
            time_range input = prep_range(py::cast<obj_time_range>(range));
            tr               = convert_range(input);
        }
        else
        {
            // our range is end-exclusive, so let's move the pointer one nanosecond
            // *after* the last element in this batch.
            tr.end.tv_nsec++;
        }
        _push_impl(qdb_exp_batch_push_truncate, &tr);
    }

    std::vector<detail::column_info> column_infos() const
    {
        return _column_infos;
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
        case qdb_ts_column_string:
        case qdb_ts_column_symbol:
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
    static std::vector<T> _convert_column(const std::vector<py::object> & values)
    {
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
        return std::move(vs);
    }

    static std::vector<qdb_int_t> _convert_int64_column(const std::vector<py::object> & values)
    {
        return _convert_column<qdb_int_t>(values);
    }

    static std::vector<double> _convert_double_column(const std::vector<py::object> & values)
    {
        return _convert_column<double>(values);
    }

    static std::vector<qdb_timespec_t> _convert_timestamp_column(const std::vector<py::object> & values)
    {
        std::vector<qdb_timespec_t> vs;
        vs.reserve(values.size());
        std::transform(std::cbegin(values), std::cend(values), std::back_inserter(vs),
            [](const py::object & val) { return convert_timestamp(val); });
        return std::move(vs);
    }

    static std::vector<qdb_blob_t> _convert_blob_like_column(const std::vector<py::object> & values, utils::blob_deque & storage)
    {
        std::vector<qdb_blob_t> vs;
        vs.reserve(values.size());
        std::transform(std::cbegin(values), std::cend(values), std::back_inserter(vs),
            [&storage](const py::object & val) {
                auto str = val.is(py::none()) ? detail::null_value<std::string>() : val.cast<std::string>();
                return storage.add(qdb_blob_t{str.data(), str.size()});
            });
        return std::move(vs);
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

    void _sort_columns()
    {
        const auto permutations = utils::sort_permutation(_timestamps, [](const qdb_timespec_t & lhs, const qdb_timespec_t & rhs) {
            return lhs < rhs;
        });
        auto permutations_copy  = permutations;
        utils::apply_permutation(_timestamps, permutations_copy);
        for (size_t index = 0; index < _columns.size(); ++index)
        {
            if (detail::access_column<int64_column>(_columns, index)->empty())
            {
                continue;
            }
            // doesn't reallocate a new vector
            // just reassign values
            permutations_copy = permutations;
            switch (_column_infos[index].type)
            {
            case qdb_ts_column_int64:
                utils::apply_permutation(*detail::access_column<int64_column>(_columns, index), permutations_copy);
                break;
            case qdb_ts_column_double:
                utils::apply_permutation(*detail::access_column<double_column>(_columns, index), permutations_copy);
                break;
            case qdb_ts_column_timestamp:
                utils::apply_permutation(*detail::access_column<timestamp_column>(_columns, index), permutations_copy);
                break;
            case qdb_ts_column_blob:
                utils::apply_permutation(*detail::access_column<blob_like_column>(_columns, index), permutations_copy);
                break;
            case qdb_ts_column_string:
                utils::apply_permutation(*detail::access_column<blob_like_column>(_columns, index), permutations_copy);
                break;
            case qdb_ts_column_symbol:
                utils::apply_permutation(*detail::access_column<blob_like_column>(_columns, index), permutations_copy);
                break;
            case qdb_ts_column_uninitialized:
                throw qdb::invalid_argument_exception{std::string{"Uninitialized column at index "} + std::to_string(index)};
            }
        }
    }

    bool _empty_columns()
    {
        return _timestamps.empty();
    }

    template <typename ColumnType>
    void _clear_column(size_t index)
    {
        detail::access_column<ColumnType>(_columns, index)->clear();
    }

    void _clear_columns()
    {
        _timestamps.clear();
        for (size_t index = 0; index < _columns.size(); ++index)
        {
            switch (_column_infos[index].type)
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
            case qdb_ts_column_string:
            case qdb_ts_column_symbol:
                _clear_column<blob_like_column>(index);
                break;
            case qdb_ts_column_uninitialized:
                throw qdb::invalid_argument_exception{std::string{"Uninitialized column at index "} + std::to_string(index)};
            }
        }
    }

    template <qdb_ts_column_type_t Expect, typename ColumnType>
    void _set_blob_impl(std::size_t index, const py::object & val)
    {
        _check_column_type<Expect>(_column_infos[index].type);
        if (val.is(py::none()))
        {
            detail::access_column<ColumnType>(_columns, index)->push_back(detail::null_value<qdb_blob_t>());
        }
        else
        {
            const auto str = val.cast<std::string>();
            const auto v = _bytes_storage.add(qdb_blob_t{str.data(), str.size()});
            detail::access_column<ColumnType>(_columns, index)->push_back(v);
        }
    }

    template <qdb_ts_column_type_t Expect, typename ColumnType, typename T>
    void _set_impl(std::size_t index, T val)
    {
        _check_column_type<Expect>(_column_infos[index].type);
        detail::access_column<ColumnType>(_columns, index)->push_back(val);
    }

    template <qdb_ts_column_type_t Expect, typename ColumnType, typename ValueType>
    void _set_blob_column_impl(std::size_t index, const std::vector<py::object> & vs)
    {
        if (_timestamps.size() != vs.size())
        {
            throw qdb::invalid_argument_exception{"timestamps array length does not match values array length."};
        }
        std::vector<ValueType> values = _convert_blob_like_column(vs, _bytes_storage);
        _check_column_type<Expect>(_column_infos[index].type);
        auto * col = detail::access_column<ColumnType>(_columns, index);
        col->insert(std::end(*col), std::make_move_iterator(std::begin(values)), std::make_move_iterator(std::end(values)));
    }

    template <qdb_ts_column_type_t Expect, typename ColumnType, typename ValueType, typename Convert>
    void _set_column_impl(std::size_t index, const std::vector<py::object> & vs, Convert && convert)
    {
        if (_timestamps.size() != vs.size())
        {
            throw qdb::invalid_argument_exception{"timestamps array length does not match values array length."};
        }
        std::vector<ValueType> values = convert(vs);
        _check_column_type<Expect>(_column_infos[index].type);
        auto * col = detail::access_column<ColumnType>(_columns, index);
        col->insert(std::end(*col), std::make_move_iterator(std::begin(values)), std::make_move_iterator(std::end(values)));
    }

private:
    qdb::logger _logger;
    qdb::handle_ptr _handle;
    std::string _table_name;

    utils::blob_deque _bytes_storage;

    std::vector<detail::column_info> _column_infos;
    std::vector<qdb_timespec_t> _timestamps;
    std::vector<any_column> _columns;

    std::vector<qdb_exp_batch_push_column_t> _columns_data;
    const qdb_exp_batch_push_table_schema_t * _table_schemas{nullptr};
};

// don't use shared_ptr, let Python do the reference counting, otherwise you will have an undefined behavior
using pinned_writer_ptr = std::unique_ptr<pinned_writer>;

template <typename Module>
static inline void register_pinned_writer(Module & m)
{
    namespace py = pybind11;

    py::class_<qdb::pinned_writer>{m, "PinnedWriter"}                                                                           //
        .def(py::init<qdb::handle_ptr, const table &>())                                                                        //
        .def("column_infos", &qdb::pinned_writer::column_infos)                                                                 //
        .def("start_row", &qdb::pinned_writer::start_row, "Calling this function marks the beginning of processing a new row.") //
        .def("set_blob", &qdb::pinned_writer::set_blob)                                                                         //
        .def("set_string", &qdb::pinned_writer::set_string)                                                                     //
        .def("set_symbol", &qdb::pinned_writer::set_symbol)                                                                     //
        .def("set_double", &qdb::pinned_writer::set_double)                                                                     //
        .def("set_int64", &qdb::pinned_writer::set_int64)                                                                       //
        .def("set_timestamp", &qdb::pinned_writer::set_timestamp)                                                               //
        .def("set_timestamps", &qdb::pinned_writer::set_timestamps)                                                             //
        .def("set_blob_column", &qdb::pinned_writer::set_blob_column)                                                           //
        .def("set_double_column", &qdb::pinned_writer::set_double_column)                                                       //
        .def("set_int64_column", &qdb::pinned_writer::set_int64_column)                                                         //
        .def("set_string_column", &qdb::pinned_writer::set_string_column)                                                       //
        .def("set_symbol_column", &qdb::pinned_writer::set_symbol_column)                                                       //
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
