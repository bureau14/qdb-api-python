%inline%{

struct retval
{
    retval() : buffer(0), buffer_size(0) {}

    char * buffer;
    size_t buffer_size;
};

struct error_carrier
{
    error_carrier() : error(qdb_e_ok) {}

    qdb_error_t error;
};

struct results_list
{
    results_list() : error(qdb_e_uninitialized) {}

    qdb_error_t error;
    std::vector<std::string> results;
};

struct wrap_ts_column
{
    wrap_ts_column(std::string n = std::string(), qdb_ts_column_type_t t = qdb_ts_column_uninitialized) : name(n), type(t) {}

    std::string name;
    qdb_ts_column_type_t type;
};

struct wrap_ts_blob_point
{
    wrap_ts_blob_point(qdb_timespec_t ts = qdb_timespec_t(), std::string d = std::string()) : timestamp(ts), data(d) {}

    qdb_timespec_t timestamp;
    std::string data;
};

class wrap_qdb_table_result_t
{
public:

    qdb_error_t check_out_of_bounds(qdb_size_t r, qdb_size_t c) const
    {
        if (r >= 0 && c >= 0 && r < rows_count && c < columns_count) return qdb_e_ok;
        return qdb_e_out_of_bounds;
    }

    std::pair<qdb_error_t, qdb_query_result_value_type_t> get_type(qdb_size_t r, qdb_size_t c) const
    {
        qdb_error_t err = check_out_of_bounds(r, c);
        if (err != qdb_e_ok) return std::make_pair(err, qdb_query_result_none);
        return std::make_pair(err, rows[r][c].type);
    }

    std::pair<qdb_error_t, std::string> get_payload_blob(qdb_size_t r, qdb_size_t c) const
    {
        qdb_error_t err = check_out_of_bounds(r, c);
        if(err != qdb_e_ok) return std::make_pair(err, "");
        if(rows[r][c].type != qdb_query_result_blob) return std::make_pair(qdb_e_incompatible_type, "");
        return std::make_pair(err, std::string(static_cast<const char *> (rows[r][c].payload.blob.content), rows[r][c].payload.blob.content_length));
    }

    std::pair<qdb_error_t, qdb_int_t> get_payload_int64(qdb_size_t r, qdb_size_t c) const
    {
        qdb_error_t err = check_out_of_bounds(r, c);
        if(err != qdb_e_ok) return std::make_pair(err, 0);
        if(rows[r][c].type != qdb_query_result_int64) return std::make_pair(qdb_e_incompatible_type, 0);
        return std::make_pair(err, rows[r][c].payload.int64_.value);
    }

    std::pair<qdb_error_t, double> get_payload_double(qdb_size_t r, qdb_size_t c) const
    {
        qdb_error_t err = check_out_of_bounds(r, c);
        if(err != qdb_e_ok) return std::make_pair(err, 0.0);
        if(rows[r][c].type != qdb_query_result_double) return std::make_pair(qdb_e_incompatible_type, 0.0);
        return std::make_pair(err, rows[r][c].payload.double_.value);
    }

    std::pair<qdb_error_t, qdb_timespec_t> get_payload_timestamp(qdb_size_t r, qdb_size_t c) const
    {
        qdb_error_t err = check_out_of_bounds(r, c);
        qdb_timespec_t zero_ts;
        zero_ts.tv_sec = 0;
        zero_ts.tv_nsec = 0;

        if(err != qdb_e_ok) return std::make_pair(err, zero_ts);
        if(rows[r][c].type != qdb_query_result_value_type_t::qdb_query_result_timestamp) return std::make_pair(qdb_e_incompatible_type, zero_ts);
        return std::make_pair(err, rows[r][c].payload.timestamp.value);
    }

    std::string table_name;
    std::vector<std::string> columns_names;
    qdb_size_t columns_count;
    qdb_size_t rows_count;
    std::vector < std::vector < qdb_point_result_t > > rows;
};

struct copy_point 
{
    qdb_point_result_t operator()(const qdb_point_result_t &that_point)
    {
        qdb_point_result_t this_point;
        this_point = that_point;

        if (that_point.type == qdb_query_result_blob)
        {
            qdb_size_t len = that_point.payload.blob.content_length;
            char *new_copy = new char[len];
            std::memcpy(static_cast<void*> (new_copy), that_point.payload.blob.content, len);
            this_point.payload.blob.content = static_cast<const void*>(new_copy);
        }
        return this_point;
    }
};

struct copy_column_names
{
    std::string operator() (const qdb_string_t &str)
    {
        return std::string(str.data);
    }
};

struct copy_table 
{
    wrap_qdb_table_result_t operator () (const qdb_table_result_t &that_table)
    {
        wrap_qdb_table_result_t _tbl;
        _tbl.rows_count = that_table.rows_count;
        _tbl.columns_count = that_table.columns_count;
        _tbl.table_name = that_table.table_name.data;
        _tbl.columns_names.resize(_tbl.columns_count);

        std::transform(that_table.columns_names, that_table.columns_names + that_table.columns_count, _tbl.columns_names.begin(), copy_column_names());

        _tbl.rows.resize(_tbl.rows_count);

        for (qdb_size_t i = 0; i < _tbl.rows_count; ++i)
        {
            _tbl.rows[i].resize(_tbl.columns_count);
            std::transform(that_table.rows[i], that_table.rows[i] + that_table.columns_count, _tbl.rows[i].begin(),
                copy_point());
        }
        return _tbl;
    }
};

class wrap_qdb_query_result_t
{
public:
    std::vector <wrap_qdb_table_result_t> tables;
    qdb_size_t tables_count, scanned_rows_count;
};

%}

// we need these structures defined and accessible in Python

typedef struct
{
    qdb_time_t tv_sec;
    qdb_time_t tv_nsec;
} qdb_timespec_t;

typedef struct
{
    qdb_timespec_t timestamp;
    double value;
} qdb_ts_double_point;

typedef struct
{
    qdb_timespec_t timestamp;
    qdb_int_t value;
} qdb_ts_int64_point;

typedef struct
{
    qdb_timespec_t timestamp;
    qdb_timespec_t value;
} qdb_ts_timestamp_point;

typedef struct
{
    qdb_timespec_t timestamp;
    const void * content;
    qdb_size_t content_length;
} qdb_ts_blob_point;

typedef struct
{
    qdb_timespec_t begin;
    qdb_timespec_t end;
} qdb_ts_range_t;

typedef struct
{
    qdb_ts_filter_type_t type;

    union {

        struct
        {
            qdb_size_t size;
        } sample;

        struct
        {
            double min;
            double max;
        } double_range;

    } params;
} qdb_ts_filter_t;

typedef struct
{
    qdb_ts_range_t range;
    qdb_ts_filter_t filter;
} qdb_ts_filtered_range_t;

typedef struct
{
    qdb_ts_aggregation_type_t type;
    qdb_ts_filtered_range_t filtered_range;
    qdb_size_t count;
    qdb_ts_blob_point result;
} qdb_ts_blob_aggregation_t;

typedef struct
{
    qdb_ts_aggregation_type_t type;
    qdb_ts_filtered_range_t filtered_range;
    qdb_size_t count;
    qdb_ts_double_point result;
} qdb_ts_double_aggregation_t;

typedef struct
{
    qdb_ts_aggregation_type_t type;
    qdb_ts_filtered_range_t filtered_range;
    qdb_size_t count;
    qdb_ts_int64_point result;
} qdb_ts_int64_aggregation_t;

typedef struct
{
    qdb_ts_aggregation_type_t type;
    qdb_ts_filtered_range_t filtered_range;
    qdb_size_t count;
    qdb_ts_timestamp_point result;
} qdb_ts_timestamp_aggregation_t;
