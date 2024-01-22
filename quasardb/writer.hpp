/*
 *
 * Official Python API
 *
 * Copyright (c) 2009-2024, quasardb SAS. All rights reserved.
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
#include "dispatch.hpp"
#include "error.hpp"
#include "logger.hpp"
#include "object_tracker.hpp"
#include "table.hpp"
#include "utils.hpp"
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

constexpr inline qdb_exp_batch_push_options_t to_push_options(enum detail::deduplication_mode_t mode)
{
    switch (mode)
    {
    case deduplication_mode_drop:
        return qdb_exp_batch_option_unique_drop;
    case deduplication_mode_upsert:
        return qdb_exp_batch_option_unique_upsert;
    default:
        return qdb_exp_batch_option_standard;
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

    static inline void _set_push_options(
        enum detail::deduplication_mode_t mode, bool columns, qdb_exp_batch_push_table_t & out)
    {
        out.options = (columns == true ? detail::to_push_options(mode) : qdb_exp_batch_option_standard);
    }

    static inline void _set_push_options(enum detail::deduplication_mode_t mode,
        std::vector<std::string> const & columns,
        qdb_exp_batch_push_table_t & out)
    {
        auto where_duplicate = std::make_unique<qdb_string_t[]>(columns.size());

        std::transform(std::cbegin(columns), std::cend(columns), where_duplicate.get(),
            [](std::string const & column) -> qdb_string_t {
                // Note, we're not copying any strings here. This works,
                // because we pass the vector by reference, and it is "owned"
                // by _push_impl, so the lifetime is longer.
                return qdb_string_t{column.c_str(), column.size()};
            });

        out.options               = detail::to_push_options(mode);
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

} // namespace detail

class writer
{

    using int64_column     = detail::int64_column;
    using double_column    = detail::double_column;
    using timestamp_column = detail::timestamp_column;
    using blob_column      = detail::blob_column;
    using string_column    = detail::string_column;
    using any_column       = detail::any_column;
    using staged_tables_t  = std::map<std::string, detail::staged_table>;

public:
    /**
     * Convenience class that holds data that can be pushed to the writer. Makes it
     * easier for the end-user to provide the data in the correct format, in a single
     * function call, if they decide to use the low-level writer API themselves.
     */
    class data
    {
        friend class writer;

    protected:
        struct value_type
        {
            qdb::table table;
            py::array index;
            py::list column_data;
        };

    public:
        void append(qdb::table const & table, py::handle const & index, py::list const & column_data);

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

    void push(writer::data const & data, py::kwargs args);
    void push_async(writer::data const & data, py::kwargs args);
    void push_fast(writer::data const & data, py::kwargs args);
    void push_truncate(writer::data const & data, py::kwargs args);

private:
    static inline detail::staged_table & _get_staged_table(
        qdb::table const & table, staged_tables_t & staged_tables)
    {
        std::string table_name = table.get_name();

        auto pos = staged_tables.lower_bound(table_name);

        // XXX(leon): can be optimized by using lower_bound and reusing the `pos` for insertion into
        //            the correct place.
        if (pos == staged_tables.end() || pos->first != table_name) [[unlikely]]
        {
            // The table was not yet found
            pos = staged_tables.emplace_hint(pos, table_name, table);
            assert(pos->second.empty());
        }

        assert(pos != staged_tables.end());
        assert(pos->first == table_name);

        return pos->second;
    }

    static staged_tables_t _stage_tables(writer::data const & data);

    void _push_impl(staged_tables_t & staged_tables,
        qdb_exp_batch_push_mode_t mode,
        detail::deduplicate_options deduplicate_options,
        qdb_ts_range_t * ranges = nullptr);

    detail::deduplicate_options _deduplicate_from_args(py::kwargs args);

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

void register_writer(py::module_ & m);

} // namespace qdb
