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
#include "concepts.hpp"
#include "dispatch.hpp"
#include "error.hpp"
#include "logger.hpp"
#include "metrics.hpp"
#include "object_tracker.hpp"
#include "table.hpp"
#include "utils.hpp"
#include "detail/retry.hpp"
#include <chrono>
#include <random>
#include <variant>
#include <vector>

namespace qdb
{

namespace detail
{

using deduplicate = std::variant<std::vector<std::string>, bool>;

enum deduplication_mode_t
{
    deduplication_mode_drop,
    deduplication_mode_upsert

};

constexpr inline qdb_exp_batch_deduplication_mode_t to_qdb(enum detail::deduplication_mode_t mode)
{
    switch (mode)
    {
    case deduplication_mode_drop:
        return qdb_exp_batch_deduplication_mode_drop;
    case deduplication_mode_upsert:
        return qdb_exp_batch_deduplication_mode_upsert;
    default:
        return qdb_exp_batch_deduplication_mode_disabled;
    }
}

struct deduplicate_options
{
    detail::deduplicate columns_;
    deduplication_mode_t mode_;

    deduplicate_options()
    {
        columns_ = false;
        mode_    = deduplication_mode_drop;
    };

    deduplicate_options(deduplication_mode_t mode, detail::deduplicate columns)
        : columns_{columns}
        , mode_{mode} {};

    static detail::deduplicate_options from_kwargs(py::kwargs args);
};

using int64_column     = std::vector<qdb_int_t>;
using double_column    = std::vector<double>;
using timestamp_column = std::vector<qdb_timespec_t>;
using blob_column      = std::vector<qdb_blob_t>;
using string_column    = std::vector<qdb_string_t>;

using any_column =
    std::variant<int64_column, double_column, timestamp_column, blob_column, string_column>;

template <qdb_ts_column_type_t T>
struct column_of_type;

template <qdb_ts_column_type_t T>
struct make_column;

#define COLUMN_OF_TYPE_DECL(TYPE, COLUMN) \
    template <>                           \
    struct column_of_type<TYPE>           \
    {                                     \
        using value_type = COLUMN;        \
    };                                    \
                                          \
    template <>                           \
    struct make_column<TYPE>              \
    {                                     \
        any_column inline operator()()    \
        {                                 \
            return COLUMN{};              \
        };                                \
    };

COLUMN_OF_TYPE_DECL(qdb_ts_column_int64, int64_column);
COLUMN_OF_TYPE_DECL(qdb_ts_column_double, double_column);
COLUMN_OF_TYPE_DECL(qdb_ts_column_timestamp, timestamp_column);
COLUMN_OF_TYPE_DECL(qdb_ts_column_blob, blob_column);
COLUMN_OF_TYPE_DECL(qdb_ts_column_string, string_column);
COLUMN_OF_TYPE_DECL(qdb_ts_column_symbol, string_column);

#undef COLUMN_OF_TYPE_DECL

template <qdb_ts_column_type_t T>
std::vector<typename traits::qdb_column<T>::value_type> & access_column(
    std::vector<any_column> & columns, size_t index)
{
    using column_type = typename column_of_type<T>::value_type;
    try
    {
        return std::get<column_type>(columns[index]);
    }
    catch (std::bad_variant_access const & /*e*/)
    {
        throw qdb::incompatible_type_exception{};
    }
}

template <qdb_ts_column_type_t T>
std::size_t column_size(std::vector<any_column> & columns, size_t index)
{
    return access_column<T>(columns, index).size();
}

template <qdb_ts_column_type_t T>
struct clear_column
{
    void operator()(any_column & xs)
    {
        using value_type = typename detail::column_of_type<T>::value_type;
        std::get<value_type>(xs).clear();
    }
};

class staged_table
{
public:
    staged_table(qdb::table const & table)
        : _logger("quasardb.writer")
        , _table_name(table.get_name())
    {
        _column_infos = table.list_columns();

        std::transform(std::cbegin(_column_infos), std::cend(_column_infos),
            std::back_inserter(_columns),

            [](auto const & col) { return dispatch::by_column_type<detail::make_column>(col.type); });
    }

    ~staged_table()
    {
        clear();
    }

    void set_index(py::array const & timestamps);
    void set_blob_column(std::size_t index, const masked_array & xs);
    void set_string_column(std::size_t index, const masked_array & xs);
    void set_double_column(std::size_t index, masked_array_t<traits::float64_dtype> const & xs);
    void set_int64_column(std::size_t index, masked_array_t<traits::int64_dtype> const & xs);
    void set_timestamp_column(
        std::size_t index, masked_array_t<traits::datetime64_ns_dtype> const & xs);

    std::vector<qdb_exp_batch_push_column_t> const & prepare_columns();

    void prepare_table_data(qdb_exp_batch_push_table_data_t & table_data);

    void prepare_batch(qdb_exp_batch_push_mode_t mode,
        detail::deduplicate_options const & deduplicate_options,
        qdb_ts_range_t * ranges,
        qdb_exp_batch_push_table_t & batch);

    static inline void _set_deduplication_mode(
        enum detail::deduplication_mode_t mode, bool columns, qdb_exp_batch_push_table_t & out)
    {
        // Set deduplication mode only when `columns` is true, in which we will deduplicate based on
        // *all* columns.
        out.deduplication_mode =
            (columns == true ? detail::to_qdb(mode) : qdb_exp_batch_deduplication_mode_disabled);
    }

    static inline void _set_deduplication_mode(enum detail::deduplication_mode_t mode,
        std::vector<std::string> const & columns,
        qdb_exp_batch_push_table_t & out)
    {
        // A specific set of columns to deduplicate has been provided, in which case
        // we'll need to do a small transformation of the column names.
        auto where_duplicate = std::make_unique<char const *[]>(columns.size());

        std::transform(std::cbegin(columns), std::cend(columns), where_duplicate.get(),
            [](std::string const & column) -> char const * { return column.c_str(); });

        out.deduplication_mode    = detail::to_qdb(mode);
        out.where_duplicate       = where_duplicate.release();
        out.where_duplicate_count = columns.size();
    }

    inline void clear()
    {
        _index.clear();
        for (size_t index = 0; index < _columns.size(); ++index)
        {
            dispatch::by_column_type<detail::clear_column>(_column_infos[index].type, _columns[index]);
        }

        _table_name.clear();
        _column_infos.clear();
        _columns_data.clear();
    }

    inline qdb_ts_range_t time_range() const
    {
        qdb_ts_range_t tr{_index.front(), _index.back()};
        // our range is end-exclusive, so let's move the pointer one nanosecond
        // *after* the last element in this batch.
        //
        // XXX(leon): this overflows if we're at exactly the last nanosecond of a second
        tr.end.tv_nsec++;

        return tr;
    }

    inline bool empty() const
    {
        return _index.empty();
    }

private:
private:
    qdb::logger _logger;

    std::string _table_name;
    std::vector<detail::column_info> _column_infos;
    std::vector<qdb_timespec_t> _index;
    std::vector<any_column> _columns;

    std::vector<qdb_exp_batch_push_column_t> _columns_data;
};

/**
 * Convenience class that holds data that can be pushed to the writer. Makes it
 * easier for the end-user to provide the data in the correct format, in a single
 * function call, if they decide to use the low-level writer API themselves.
 */
class writer_data
{
public:
    struct value_type
    {
        qdb::table table;
        py::array index;
        py::list column_data;
    };

public:
    void append(qdb::table const & table, py::handle const & index, py::list const & column_data)
    {
        py::array index_ = numpy::array::ensure<traits::datetime64_ns_dtype>(index);

        /**
         * Additional check that all the data is actually of the same length, and data has been
         * provided for each and every column.
         */
        if (column_data.size() != table.list_columns().size())
        {
            throw qdb::invalid_argument_exception{"data must be provided for every table column"};
        }

        for (py::handle const & data : column_data)
        {
            qdb::masked_array data_ = data.cast<qdb::masked_array>();
            if (data_.size() != static_cast<std::size_t>(index_.size()))
            {
                throw qdb::invalid_argument_exception{
                    "every data array should be exactly the same length as the index array"};
            }
        }

        xs_.push_back(value_type{table, index_, column_data});
    }

    inline bool empty() const noexcept
    {
        return xs_.empty();
    }

    inline value_type const & front() const noexcept
    {
        assert(empty() == false);

        return xs_.front();
    }

    inline value_type const & back() const noexcept
    {
        assert(empty() == false);

        return xs_.back();
    }

    std::vector<value_type> xs() const
    {
        return xs_;
    }

private:
    std::vector<value_type> xs_;
};

static qdb_uint_t batch_push_flags_from_kwargs(py::kwargs const & kwargs)
{
    if (!kwargs.contains("write_through"))
    {
        return static_cast<qdb_uint_t>(qdb_exp_batch_push_flag_none);
    }

    try
    {
        return py::cast<bool>(kwargs["write_through"])
                   ? static_cast<qdb_uint_t>(qdb_exp_batch_push_flag_write_through)
                   : static_cast<qdb_uint_t>(qdb_exp_batch_push_flag_none);
    }
    catch (py::cast_error const & /*e*/)
    {
        std::string error_msg = "Invalid argument provided for `write_through`: expected bool, got: ";
        error_msg += py::str(py::type::of(kwargs["write_through"])).cast<std::string>();

        throw qdb::invalid_argument_exception{error_msg};
    }
}

/**
 * Wraps an index to staged tables. Provides functionality for indexing writer
 * data into staged tables as well.
 */
class staged_tables
{
public:
    using key_type       = std::string;
    using value_type     = staged_table;
    using container_type = std::map<key_type, staged_table>;
    using iterator       = container_type::iterator;
    using const_iterator = container_type::const_iterator;

public:
    /**
     * Free function that takes indexes all writer data into a staged_table object.
     */
    static staged_tables index(writer_data const & data);

public:
    constexpr inline container_type::size_type size() const
    {
        return idx_.size();
    }

    constexpr inline bool empty() const
    {
        return idx_.empty();
    }

    inline iterator begin()
    {
        return idx_.begin();
    }

    inline iterator end()
    {
        return idx_.end();
    }

    inline const_iterator begin() const
    {
        return idx_.cbegin();
    }

    inline const_iterator cend() const
    {
        return idx_.cend();
    }

    /**
     * Returns the first staged table. Useful in scenarios where we are only working with a single
     * table.
     */
    inline value_type & first()
    {
        assert(size() == 1);

        return idx_.begin()->second;
    }

    /**
     * Returns the first staged table. Useful in scenarios where we are only working with a single
     * table.
     */
    inline value_type const & first() const
    {
        assert(size() == 1);

        return idx_.cbegin()->second;
    }

    /**
     * Retrieves table by table object, or creates a new empty entry.
     */
    inline value_type & get_or_create(qdb::table const & table)
    {
        std::string const & table_name = table.get_name();
        auto pos                       = idx_.lower_bound(table_name);

        if (pos == idx_.end() || pos->first != table_name) [[unlikely]]
        {
            // The table was not yet found
            pos = idx_.emplace_hint(pos, table_name, table);
            assert(pos->second.empty());
        }

        assert(pos != idx_.end());
        assert(pos->first == table_name);

        return pos->second;
    }

private:
    container_type idx_;
};

} // namespace detail

class writer
{

    using int64_column     = detail::int64_column;
    using double_column    = detail::double_column;
    using timestamp_column = detail::timestamp_column;
    using blob_column      = detail::blob_column;
    using string_column    = detail::string_column;
    using any_column       = detail::any_column;

public:
public:
    writer(qdb::handle_ptr h)
        : _logger("quasardb.writer")
        , _handle{h}
    {}

    // prevent copy because of the table object, use a unique_ptr of the batch in cluster
    // to return the object
    writer(const writer &) = delete;

    ~writer()
    {}

    const std::vector<qdb_exp_batch_push_column_t> & prepare_columns();

    template <qdb::concepts::sleep_strategy SS>
    void push(detail::writer_data const & data, py::kwargs kwargs)
    {
        qdb::object_tracker::scoped_capture capture{_object_tracker};

        const qdb_exp_batch_options_t options = {
            .mode       = qdb_exp_batch_push_transactional,            //
            .push_flags = detail::batch_push_flags_from_kwargs(kwargs) //
        };

        _push_impl<SS>(                         //
            detail::staged_tables::index(data), //
            options,                            //
            kwargs                              //
        );                                      //
    }

    template <qdb::concepts::sleep_strategy SS>
    void push_async(detail::writer_data const & data, py::kwargs kwargs)
    {
        qdb::object_tracker::scoped_capture capture{_object_tracker};

        const qdb_exp_batch_options_t options = {
            .mode       = qdb_exp_batch_push_async,                    //
            .push_flags = detail::batch_push_flags_from_kwargs(kwargs) //
        };

        _push_impl<SS>(                         //
            detail::staged_tables::index(data), //
            options,                            //
            kwargs                              //
        );                                      //
    }

    template <qdb::concepts::sleep_strategy SS>
    void push_fast(detail::writer_data const & data, py::kwargs kwargs)
    {
        qdb::object_tracker::scoped_capture capture{_object_tracker};
        auto staged_tables = detail::staged_tables::index(data);

        const qdb_exp_batch_options_t options = {
            .mode       = qdb_exp_batch_push_fast,                     //
            .push_flags = detail::batch_push_flags_from_kwargs(kwargs) //
        };

        _push_impl<SS>(                         //
            detail::staged_tables::index(data), //
            options,                            //
            kwargs                              //
        );                                      //
    }

    template <qdb::concepts::sleep_strategy SS>
    void push_truncate(detail::writer_data const & data, py::kwargs kwargs)
    {
        qdb::object_tracker::scoped_capture capture{_object_tracker};
        auto idx = detail::staged_tables::index(data);

        // As we are actively removing data, let's add an additional check to ensure the user
        // doesn't accidentally truncate his whole database without inserting anything.
        if (data.empty()) [[unlikely]]
        {
            throw qdb::invalid_argument_exception{
                "Writer is empty: you did not provide any rows to push."};
        }

        qdb_ts_range_t tr;

        if (kwargs.contains("range"))
        {
            tr = convert::value<py::tuple, qdb_ts_range_t>(py::cast<py::tuple>(kwargs["range"]));
        }
        else
        {
            // TODO(leon): support multiple tables for push truncate
            if (idx.size() != 1) [[unlikely]]
            {
                throw qdb::invalid_argument_exception{"Writer push truncate only supports a single "
                                                      "table unless an explicit range is provided: you "
                                                      "provided more than one table without"
                                                      " an explicit range."};
            }

            detail::staged_table const & staged_table = idx.first();
            tr                                        = staged_table.time_range();
        }

        const qdb_exp_batch_options_t options = {
            .mode       = qdb_exp_batch_push_truncate,                 //
            .push_flags = detail::batch_push_flags_from_kwargs(kwargs) //
        };

        _push_impl<SS>(     //
            std::move(idx), //
            options,        //
            kwargs,         //
            &tr             //
        );                  //
    }

private:
    template <concepts::sleep_strategy SS>
    void _push_impl(detail::staged_tables && idx,
        qdb_exp_batch_options_t const & options,
        py::kwargs const & kwargs,
        qdb_ts_range_t * ranges = nullptr)
    {
        _handle->check_open();

        if (idx.empty()) [[unlikely]]
        {
            throw qdb::invalid_argument_exception{"No data written to batch writer."};
        }

        auto deduplicate_options = detail::deduplicate_options::from_kwargs(kwargs);

        std::vector<qdb_exp_batch_push_table_t> batch;
        batch.assign(idx.size(), qdb_exp_batch_push_table_t());

        int cur = 0;

        for (auto pos = idx.begin(); pos != idx.end(); ++pos)
        {
            std::string const & table_name      = pos->first;
            detail::staged_table & staged_table = pos->second;
            auto & batch_table                  = batch.at(cur++);

            staged_table.prepare_batch(options.mode, deduplicate_options, ranges, batch_table);

            if (batch_table.data.column_count == 0) [[unlikely]]
            {
                throw qdb::invalid_argument_exception{
                    "Writer is empty: you did not provide any columns to push."};
            }

            _logger.debug("Pushing %d rows with %d columns in %s", batch_table.data.row_count,
                batch_table.data.column_count, table_name);
        }

        _do_push<SS>(options, batch, detail::retry_options::from_kwargs(kwargs));
    }

    template <concepts::sleep_strategy SleepStrategy>
    void _do_push(qdb_exp_batch_options_t const & options,
        std::vector<qdb_exp_batch_push_table_t> const & batch,
        detail::retry_options const & retry_options)
    {
        qdb_error_t err{qdb_e_ok};

        // #ifdef QDB_TESTS_ENABLED
        //     auto mock_failure_options = detail::mock_failure_options::from_kwargs(kwargs);

        //     bool mocked_failure{false};

        //     if (mock_failure_options.has_next() == true)
        //     {
        //         mock_failure_options = mock_failure_options.next();
        //         err                  = mock_failure_options.error();
        //         mocked_failure       = true;
        //         _logger.info("mocked failure, failures left: %d",
        //         mock_failure_options.failures_left);
        //     }
        //     else
        // #endif
        {
            // Make sure to measure the time it takes to do the actual push.
            // This is in its own scoped block so that we only actually measure
            // the push time, not e.g. retry time.
            qdb::metrics::scoped_capture capture{"qdb_batch_push"};

            err = qdb_exp_batch_push_with_options(
                *_handle, &options, batch.data(), nullptr, batch.size());
        }

        if (retry_options.should_retry(err))
            [[unlikely]] // Unlikely, because err is most likely to be qdb_e_ok
        {
            if (err == qdb_e_async_pipe_full) [[likely]]
            {
                _logger.info("Async pipelines are currently full");
            }
            else
            {
                _logger.warn("A temporary error occurred");
            }

            std::chrono::milliseconds delay = retry_options.delay;
            _logger.info("Sleeping for %d milliseconds", delay.count());

            SleepStrategy::sleep(delay);

            // Now try again -- easier way to go about this is to enter recursion. Note how
            // we permutate the retry_options, which automatically adjusts the amount of retries
            // left and the next sleep duration.
            _logger.warn("Retrying push operation, retries left: %d", retry_options.retries_left);
            return _do_push<SleepStrategy>(options, batch, retry_options.next());
        }

        qdb::qdb_throw_if_error(*_handle, err);
    }

private:
    qdb::logger _logger;
    qdb::handle_ptr _handle;

    qdb::object_tracker::scoped_repository _object_tracker;

public:
    // the 'legacy' API needs some state attached to the pinned writer; monkey patching
    // the pinned writer purely in python for this is possible, but annoying to do right;
    // it's much easier to just have it live here (for now), until the batch writer is
    // fully deprecated.
    py::dict legacy_state_;
};

// don't use shared_ptr, let Python do the reference counting, otherwise you will have an undefined
// behavior
using writer_ptr = std::unique_ptr<writer>;

template <qdb::concepts::sleep_strategy SS>
static void register_writer(py::module_ & m)
{
    namespace py = pybind11;

    // Writer data
    auto writer_data_c = py::class_<qdb::detail::writer_data>{m, "WriterData"};
    writer_data_c.def(py::init())
        .def("append", &qdb::detail::writer_data::append, py::arg("table"), py::arg("index"),
            py::arg("column_data"), "Append new data")
        .def("empty", &qdb::detail::writer_data::empty, "Returns true if underlying data is empty");

    // And the actual pinned writer
    auto writer_c = py::class_<qdb::writer>{m, "Writer"};

    // basic interface
    writer_c.def(py::init<qdb::handle_ptr>()); //

    writer_c.def_readwrite("_legacy_state", &qdb::writer::legacy_state_);

    // push functions
    writer_c
        .def("push", &qdb::writer::push<SS>, "Regular batch push") //
        .def("push_async", &qdb::writer::push_async<SS>,
            "Asynchronous batch push that buffers data inside the QuasarDB daemon") //
        .def("push_fast", &qdb::writer::push_fast<SS>,
            "Fast, in-place batch push that is efficient when doing lots of small, incremental "
            "pushes.") //
        .def("push_truncate", &qdb::writer::push_truncate<SS>,
            "Before inserting data, truncates any existing data. This is useful when you want your "
            "insertions to be idempotent, e.g. in "
            "case of a retry.");
}

} // namespace qdb
