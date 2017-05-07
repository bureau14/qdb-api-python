%inline%{

namespace qdb
{

// thanks to the definitions, content/content_length will be matched for a Python string
qdb::api_buffer_ptr make_api_buffer_ptr_from_string(handle_ptr h, const char * content, size_t content_length)
{
    if (!h || !content || !content_length)
    {
        return qdb::api_buffer_ptr();
    }

    return qdb::make_api_buffer_ptr(*h, content, content_length);
}

PyObject * api_buffer_ptr_as_string(const qdb::api_buffer_ptr & buf)
{
    return SWIG_FromCharPtrAndSize(buf->data(), buf->size());
}

api_buffer_ptr blob_get(handle_ptr h, const char * alias, error_carrier * error)
{
    return h->blob_get(alias, error->error);
}

api_buffer_ptr blob_get_and_remove(handle_ptr h, const char * alias, error_carrier * error)
{
    return h->blob_get_and_remove(alias, error->error);
}

api_buffer_ptr blob_get_and_update(handle_ptr h, const char * alias, const char * update_content, size_t update_content_length, qdb_time_t expiry_time, error_carrier * error)
{
    return h->blob_get_and_update(alias, update_content, update_content_length, expiry_time, error->error);
}

api_buffer_ptr blob_compare_and_swap(handle_ptr h, const char * alias,
    const char * new_value,
    size_t new_value_length,
    const char * comparand,
    size_t comparand_length,
    qdb_time_t expiry_time,
    error_carrier * error)
{
    return h->blob_compare_and_swap(alias, new_value, new_value_length, comparand, comparand_length, expiry_time, error->error);
}

std::string node_status(handle_ptr h, const char * uri, error_carrier * error)
{
    return h->node_status(uri, error->error);
}

std::string node_config(handle_ptr h, const char * uri, error_carrier * error)
{
    return h->node_config(uri, error->error);
}

std::string node_topology(handle_ptr h, const char * uri, error_carrier * error)
{
    return h->node_topology(uri, error->error);
}

handle_ptr connect(const char * uri, int timeout_ms, error_carrier * error)
{
    handle_ptr h(new handle());
    error->error = h->set_timeout(timeout_ms);
    if (QDB_SUCCESS(error->error))
    {
        error->error = h->connect(uri);
    }
    return h;
}

qdb_time_t get_expiry_time_wrapper(handle_ptr h, const char * alias, error_carrier * error)
{
    qdb_time_t val = 0;
    error->error = h->get_expiry_time(alias, val);
    val /= 1000; // convert milliseconds to seconds
    return val;
}

qdb_int_t int_get(handle_ptr h, const char * alias, error_carrier * error)
{
    qdb_int_t res = 0;
    error->error = h->int_get(alias, &res);
    return res;
}

qdb_int_t int_add(handle_ptr h, const char * alias, qdb_int_t addend, error_carrier * error)
{
    qdb_int_t res = 0;
    error->error = h->int_add(alias, addend, &res);
    return res;
}

api_buffer_ptr deque_pop_front(handle_ptr h, const char * alias, error_carrier * error)
{
    error->error = qdb_e_uninitialized;
    return h->deque_pop_front(alias, error->error);
}

api_buffer_ptr deque_pop_back(handle_ptr h, const char * alias, error_carrier * error)
{
    error->error = qdb_e_uninitialized;
    return h->deque_pop_back(alias, error->error);
}

api_buffer_ptr deque_front(handle_ptr h, const char * alias, error_carrier * error)
{
    error->error = qdb_e_uninitialized;
    return h->deque_front(alias, error->error);
}

api_buffer_ptr deque_back(handle_ptr h, const char * alias, error_carrier * error)
{
    error->error = qdb_e_uninitialized;
    return h->deque_back(alias, error->error);
}

std::vector<std::string> get_tags(handle_ptr h, const char * alias, error_carrier * error)
{
    error->error = qdb_e_uninitialized;
    return h->get_tags(alias, error->error);
}

std::vector<std::string> get_tagged(handle_ptr h, const char * tag, error_carrier * error)
{
    error->error = qdb_e_uninitialized;
    return h->get_tagged(tag, error->error);
}

std::vector<std::string> blob_scan(handle_ptr h,
                                   const void * pattern,
                                   qdb_size_t pattern_length,
                                   qdb_int_t max_count,
                                   error_carrier * error)
{
    error->error = qdb_e_uninitialized;
    return h->blob_scan(pattern, pattern_length, max_count, error->error);
}

std::vector<std::string> blob_scan_regex(handle_ptr h,
                                   const char * pattern,
                                   qdb_int_t max_count,
                                   error_carrier * error)
{
    error->error = qdb_e_uninitialized;
    return h->blob_scan_regex(pattern, max_count, error->error);
}

struct result_as_string
{
    std::string operator()(const char * alias) const
    {
        return std::string(alias);
    }
};

std::vector<std::string> run_query(handle_ptr h, const char * q, error_carrier * error)
{
    const char ** aliases = NULL;
    size_t count = 0;

    error->error = qdb_query(*h, q, &aliases, &count);

    std::vector<std::string> v(count);

    std::transform(aliases, aliases + count, v.begin(), result_as_string());

    qdb_release(*h, aliases);

    return v;
}

std::vector<std::string> prefix_get(handle_ptr h, const char * prefix, qdb_int_t max_count, error_carrier * error)
{
    error->error = qdb_e_uninitialized;
    return h->prefix_get(prefix, max_count, error->error);
}

qdb_uint_t prefix_count(handle_ptr h, const char * prefix, error_carrier * error)
{
    error->error = qdb_e_uninitialized;
    return h->prefix_count(prefix, error->error);
}

std::vector<std::string> suffix_get(handle_ptr h, const char * suffix, qdb_int_t max_count, error_carrier * error)
{
    error->error = qdb_e_uninitialized;
    return h->suffix_get(suffix, max_count, error->error);
}

qdb_uint_t suffix_count(handle_ptr h, const char * suffix, error_carrier * error)
{
    error->error = qdb_e_uninitialized;
    return h->suffix_count(suffix, error->error);
}

struct transform_to_col_info
{
    qdb_ts_column_info_t operator()(const wrap_ts_column & wtc) const
    {
        qdb_ts_column_info_t res;

        res.name = wtc.name.c_str();
        res.type = wtc.type;

        return res;
    }
};

qdb_error_t ts_create(handle_ptr h, const char * alias, const std::vector<wrap_ts_column> & columns)
{
    std::vector<qdb_ts_column_info_t> qdb_cols_info(columns.size());

    std::transform(columns.begin(), columns.end(), qdb_cols_info.begin(), transform_to_col_info());

    return qdb_ts_create(*h, alias, &qdb_cols_info[0], qdb_cols_info.size());
}

struct column_creator
{
    wrap_ts_column operator()(const qdb_ts_column_info_t & ci) const
    {
        return wrap_ts_column(ci.name, ci.type);
    }
};

std::vector<wrap_ts_column> ts_list_columns(handle_ptr h, const char * alias, error_carrier * error)
{
    qdb_ts_column_info_t * column_infos = NULL;
    qdb_size_t count = 0;

    std::vector<wrap_ts_column> res;

    error->error = qdb_ts_list_columns(*h, alias, &column_infos, &count);
    if (QDB_SUCCESS(error->error))
    {
        res.resize(count);

        std::transform(column_infos, column_infos + count, res.begin(), column_creator());
    }

    qdb_release(*h, column_infos);

    return res;
}

qdb_error_t ts_double_insert(handle_ptr h, const char * alias, const char * column, const std::vector<qdb_ts_double_point> & values)
{
    return qdb_ts_double_insert(*h, alias, column, &values[0], values.size());
}

struct to_qdb_ts_blob_point
{
    qdb_ts_blob_point operator()(const wrap_ts_blob_point & pt) const
    {
        qdb_ts_blob_point res;

        res.timestamp = pt.timestamp;
        res.content = pt.data.data();
        res.content_length = pt.data.size();

        return res;
    }
};

qdb_error_t ts_blob_insert(handle_ptr h, const char * alias, const char * column, const std::vector<wrap_ts_blob_point> & values)
{
    std::vector<qdb_ts_blob_point> points(values.size());

    std::transform(values.begin(), values.end(), points.begin(), to_qdb_ts_blob_point());

    return qdb_ts_blob_insert(*h, alias, column, &points[0], points.size());
}

std::vector<qdb_ts_double_point> ts_double_get_ranges(handle_ptr h, const char * alias, const char * column,
    const std::vector<qdb_ts_range_t> & ranges, error_carrier * error)
{
    qdb_ts_double_point * points = NULL;
    qdb_size_t count = 0;

    std::vector<qdb_ts_double_point> res;

    error->error = qdb_ts_double_get_ranges(*h, alias, column, &ranges[0], ranges.size(), &points, &count);
    if (QDB_SUCCESS(error->error))
    {
        res.resize(count);
        std::copy(points, points + count, res.begin());

        qdb_release(*h, points);
    }

    return res;
}

struct to_ts_blob_point
{
    wrap_ts_blob_point operator()(const qdb_ts_blob_point & pt) const
    {
        return wrap_ts_blob_point(pt.timestamp, std::string(static_cast<const char *>(pt.content), pt.content_length));
    }
};

std::vector<wrap_ts_blob_point> ts_blob_get_ranges(handle_ptr h, const char * alias, const char * column,
    const std::vector<qdb_ts_range_t> & ranges, error_carrier * error)
{
    qdb_ts_blob_point * points = NULL;
    qdb_size_t count = 0;

    std::vector<wrap_ts_blob_point> res;

    error->error = qdb_ts_blob_get_ranges(*h, alias, column, &ranges[0], ranges.size(), &points, &count);
    if (QDB_SUCCESS(error->error))
    {
        res.resize(count);
        std::transform(points, points + count, res.begin(), to_ts_blob_point());

        qdb_release(*h, points);
    }

    return res;
}

struct range_to_agg
{
    qdb_ts_double_aggregation_t operator()(const qdb_ts_range_t & t) const
    {
        qdb_ts_double_aggregation_t res;
        res.range = t;
        return res;
    }
};

void ts_double_aggregation(handle_ptr h, const char * alias, const char * column, 
    std::vector<qdb_ts_double_aggregation_t> & ranges, error_carrier * error)
{
    error->error = qdb_ts_double_aggregate(*h, alias, column, &ranges[0], ranges.size());
}

}
%}
