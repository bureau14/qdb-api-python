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
#include "query.hpp"
#include "ts.hpp"

namespace py = pybind11;

namespace qdb
{

static std::string make_string(const qdb_string_t & str)
{
    return std::string{str.data, str.length};
}

static std::vector<qdb_query_result_value_type_t> scan_col_types(const qdb_table_result_t & table)
{
    std::vector<qdb_query_result_value_type_t> res(table.columns_count, qdb_query_result_none);

    size_t found = 0;

    for (size_t r = 0; (r < table.rows_count) && (found < res.size()); ++r)
    {
        for (size_t c = 0; (c < table.columns_count) && (found < res.size()); ++c)
        {
            if ((res[c] == qdb_query_result_none) && (table.rows[r][c].type != qdb_query_result_none))
            {
                res[c] = table.rows[r][c].type;
                ++found;
            }
        }
    }

    return res;
}

static qdb_size_t scan_blob_max_length(const qdb_table_result_t & table, size_t c)
{
    if (table.columns_count < c) throw qdb::exception{qdb_e_out_of_bounds};

    qdb_size_t max_length = 0;

    for (size_t r = 0; r < table.rows_count; ++r)
    {
        if (table.rows[r][c].type == qdb_query_result_blob)
        {
            max_length = std::max(table.rows[r][c].payload.blob.content_length, max_length);
        }
    }

    return max_length;
}

template <typename MutableArray>
static void fill_column_double(MutableArray & dest, const qdb_table_result_t & table, size_t c)
{
    for (size_t r = 0; r < table.rows_count; ++r)
    {
        if (table.rows[r][c].type == qdb_query_result_double)
        {
            dest(r) = table.rows[r][c].payload.double_.value;
        }
        else
        {
            dest(r) = std::numeric_limits<double>::quiet_NaN();
        }
    }
}

template <typename MutableArray>
static void fill_column_int64(MutableArray & dest, const qdb_table_result_t & table, size_t c)
{
    for (size_t r = 0; r < table.rows_count; ++r)
    {
        if (table.rows[r][c].type == qdb_query_result_int64)
        {
            dest(r) = table.rows[r][c].payload.int64_.value;
        }
        else
        {
            dest(r) = std::numeric_limits<std::int64_t>::min();
        }
    }
}

template <typename MutableArray>
static void fill_column_count(MutableArray & dest, const qdb_table_result_t & table, size_t c)
{
    for (size_t r = 0; r < table.rows_count; ++r)
    {
        if (table.rows[r][c].type == qdb_query_result_count)
        {
            dest(r) = table.rows[r][c].payload.count.value;
        }
        else
        {
            dest(r) = std::numeric_limits<std::int64_t>::min();
        }
    }
}

template <typename MutableArray>
static void fill_column_timestamp(MutableArray & dest, const qdb_table_result_t & table, size_t c)
{
    for (size_t r = 0; r < table.rows_count; ++r)
    {
        if (table.rows[r][c].type == qdb_query_result_timestamp)
        {
            dest(r) = convert_timestamp(table.rows[r][c].payload.timestamp.value);
        }
        else
        {
            dest(r) = std::numeric_limits<std::int64_t>::min();
        }
    }
}

static void fill_column_blob(char * dest, size_t item_size, const qdb_table_result_t & table, size_t c)
{
    for (size_t r = 0; r < table.rows_count; ++r, dest += item_size)
    {
        memset(dest, 0, item_size);

        if (table.rows[r][c].type == qdb_query_result_blob)
        {
            assert(table.rows[r][c].payload.blob.content_length <= item_size);
            memcpy(dest, table.rows[r][c].payload.blob.content, table.rows[r][c].payload.blob.content_length);
        }
    }
}

static void create_columns(
    query::table_result & t, const qdb_table_result_t & table, const std::vector<qdb_query_result_value_type_t> & col_types)
{
    t.resize(table.columns_count);

    for (size_t c = 0; c < table.columns_count; ++c)
    {
        t[c].name = make_string(table.columns_names[c]);

        switch (col_types[c])
        {
        case qdb_query_result_none:
            t[c].data = py::array_t<double>{{table.rows_count}};
            break;

        case qdb_query_result_double:
            t[c].data = py::array_t<double>{{table.rows_count}};
            break;

        case qdb_query_result_blob:
        {
            // need to compute the max length for proper allocation
            const auto max_length = scan_blob_max_length(table, c);
            std::stringstream ss;
            ss << "|S" << max_length;
            const std::string str = ss.str();
            t[c].data             = py::array{str.c_str(), {table.rows_count}};
            break;
        }

        case qdb_query_result_int64:
            t[c].data = py::array_t<std::int64_t>{{table.rows_count}};
            break;

        case qdb_query_result_timestamp:
            t[c].data = py::array{"datetime64[ns]", {table.rows_count}};
            break;

        case qdb_query_result_count:
            t[c].data = py::array_t<std::int64_t>{{table.rows_count}};
            break;
        }
    }
}

static void insert_table_result(query::query_result & r, const qdb_table_result_t & table)
{
    auto it = r.tables.insert(std::make_pair(make_string(table.table_name), query::table_result{})).first;

    const auto col_types = scan_col_types(table);

    create_columns(it->second, table, col_types);

    // and now we fill, column by column
    for (size_t c = 0; c < table.columns_count; ++c)
    {
        switch (col_types[c])
        {
        case qdb_query_result_none:
            break;

        case qdb_query_result_double:
        {
            auto dest = it->second[c].data.mutable_unchecked<double, 1>();
            fill_column_double(dest, table, c);
            break;
        }

        case qdb_query_result_int64:
        {
            auto dest = it->second[c].data.mutable_unchecked<std::int64_t, 1>();
            fill_column_int64(dest, table, c);
            break;
        }

        case qdb_query_result_count:
        {
            auto dest = it->second[c].data.mutable_unchecked<std::int64_t, 1>();
            fill_column_count(dest, table, c);
            break;
        }

        case qdb_query_result_timestamp:
        {
            auto dest = it->second[c].data.mutable_unchecked<std::int64_t, 1>();
            fill_column_timestamp(dest, table, c);
            break;
        }

        case qdb_query_result_blob:
        {
            char * dest      = static_cast<char *>(it->second[c].data.mutable_data());
            size_t item_size = it->second[c].data.itemsize();

            fill_column_blob(dest, item_size, table, c);

            break;
        }
        }
    }
}

query::query_result query::run()
{
    qdb_query_result_t * result = nullptr;

    qdb::qdb_throw_if_error(qdb_query(*_handle, _query_string.c_str(), &result), [&]() noexcept { qdb_release(*_handle, result); });

    query::query_result converted_result{};
    if (nullptr != result)
    {
        converted_result.scanned_point_count = result->scanned_point_count;
        converted_result.tables.reserve(result->tables_count);

        for (size_t t = 0; t < result->tables_count; ++t)
        {
            insert_table_result(converted_result, result->tables[t]);
        }

        qdb_release(*_handle, result);
    }
    return converted_result;
}

} // namespace qdb
