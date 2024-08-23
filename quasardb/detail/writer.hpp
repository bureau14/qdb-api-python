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

#include "../concepts.hpp"
#include "../convert/value.hpp"
#include "../dispatch.hpp"
#include "../error.hpp"
#include "../logger.hpp"
#include "../table.hpp"
#include "retry.hpp"
#include <variant>
#include <vector>

namespace qdb::detail
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
    inline container_type::size_type size() const
    {
        return idx_.size();
    }

    inline bool empty() const
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

struct batch_push_flags
{
    static constexpr char const * kw_write_through = "write_through";
    static constexpr bool default_write_through    = true;

    /**
     * Ensures all options are always explicitly set, according to the defaults above.
     */
    static py::kwargs ensure(py::kwargs kwargs);

    /**
     * Returns the push mode, throws error if push mode is not set.
     */
    static qdb_uint_t from_kwargs(py::kwargs const & kwargs);
};

struct batch_push_mode
{
    static constexpr char const * kw_push_mode                   = "push_mode";
    static constexpr qdb_exp_batch_push_mode_t default_push_mode = qdb_exp_batch_push_transactional;

    /**
     * Ensures push mode is always set in the kwargs, and uses the `default_push_mode` if not found.
     */
    static py::kwargs ensure(py::kwargs kwargs);

    /**
     * Sets the push mode to a certain value.
     */
    static py::kwargs set(py::kwargs kwargs, qdb_exp_batch_push_mode_t push_mode);

    /**
     * Returns the push mode, throws error if push mode is not set.
     */
    static qdb_exp_batch_push_mode_t from_kwargs(py::kwargs const & kwargs);
};

struct batch_options
{
    static qdb_exp_batch_options_t from_kwargs(py::kwargs const & kwargs);
};

struct batch_truncate_ranges
{

    static constexpr char const * kw_range = "range";

    /**
     * Ensures a range is set. If not set, derives it from the range of the provided
     * tables.
     */
    static py::kwargs ensure(py::kwargs kwargs, detail::staged_tables const & idx)
    {
        if (kwargs.contains(kw_range) == false)
        {
            if (idx.size() != 1) [[unlikely]]
            {
                throw qdb::invalid_argument_exception{"Writer push truncate only supports a single "
                                                      "table unless an explicit range is provided: "
                                                      "you  provided more than one table without "
                                                      " an explicit range."};
            }

            detail::staged_table const & staged_table = idx.first();

            auto range_ = staged_table.time_range();

            py::print("1 before");
            py::tuple x = convert::value<qdb_ts_range_t, py::tuple>(range_);
            py::print("2 middle");
            kwargs[kw_range] = x;
            py::print("3 after");
        }

        return kwargs;
    }

    /**
     * Returns the truncate ranges based on the kwargs.
     */
    static std::vector<qdb_ts_range_t> from_kwargs(py::kwargs kwargs)
    {
        // This function *could* be invoked, but doesn't make sense to be invoked, when we're not
        // doing a push truncate.
        //
        // Strictly speaking this assertion is not necessary, but it doesn't hurt to have it.
        assert(qdb::detail::batch_push_mode::from_kwargs(kwargs) == qdb_exp_batch_push_truncate);

        // We also always assume a range is provided, i.e. `ensure` is called beforehand.
        assert(kwargs.contains(detail::batch_truncate_ranges::kw_range) == true);
        std::vector<qdb_ts_range_t> ret{};

        py::tuple range_ = py::cast<py::tuple>(kwargs[detail::batch_truncate_ranges::kw_range]);
        ret.push_back(convert::value<py::tuple, qdb_ts_range_t>(range_));

        return ret;
    }
};

/**
 * Default batch push strategy, just invokes the regular function and returns the result.
 */
struct default_writer_push_strategy
{

    static default_writer_push_strategy from_kwargs(py::kwargs const & /* kwargs */)
    {
        return {};
    }

    inline qdb_error_t operator()(qdb_handle_t handle,
        qdb_exp_batch_options_t const * options,
        qdb_exp_batch_push_table_t const * tables,
        qdb_exp_batch_push_table_schema_t const ** table_schemas,
        qdb_size_t table_count) const noexcept
    {
        return qdb_exp_batch_push_with_options(handle, options, tables, table_schemas, table_count);
    }
};

static_assert(concepts::writer_push_strategy<default_writer_push_strategy>);

} // namespace qdb::detail
