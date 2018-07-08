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

    QDB_THROW_IF_ERROR(qdb_exp_query(*_handle, _query_string.c_str(), &result));

    query::query_result converted_result;

    converted_result.scanned_rows_count = result->scanned_rows_count;
    converted_result.tables.reserve(result->tables_count);

    for (size_t t = 0; t < result->tables_count; ++t)
    {
        insert_table_result(converted_result, result->tables[t]);
    }

    qdb_release(*_handle, result);

    return converted_result;
}

} // namespace qdb
