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

//Space unoptimized as using union was creating problem while copying payload.blob.content
//Had to copy the blob.content in a heap which was fine until the time came to destruct the heap allocated
//memory in destructor. For some reasons the destructor was called twice and the program crashed.
struct wrap_qdb_point_result_t
{
    qdb_query_result_value_type_t type;
    struct 
    {
        std::string blob_value;
        qdb_int_t int_value;
        double double_value;
        qdb_timespec_t timestamp_value;
    } payload;
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
        if (err != qdb_e_ok) return std::make_pair(err, "");
        if (rows[r][c].type != qdb_query_result_blob) return std::make_pair(qdb_e_incompatible_type, "");
        return std::make_pair(err, rows[r][c].payload.blob_value);
    }

    std::pair<qdb_error_t, qdb_int_t> get_payload_int64(qdb_size_t r, qdb_size_t c) const
    {
        qdb_error_t err = check_out_of_bounds(r, c);
        if (err != qdb_e_ok) return std::make_pair(err, 0);
        if (rows[r][c].type != qdb_query_result_int64) return std::make_pair(qdb_e_incompatible_type, 0);
        return std::make_pair(err, rows[r][c].payload.int_value);
    }

    std::pair<qdb_error_t, double> get_payload_double(qdb_size_t r, qdb_size_t c) const
    {
        qdb_error_t err = check_out_of_bounds(r, c);
        if (err != qdb_e_ok) return std::make_pair(err, 0.0);
        if (rows[r][c].type != qdb_query_result_double) return std::make_pair(qdb_e_incompatible_type, 0.0);
        return std::make_pair(err, rows[r][c].payload.double_value);
    }

    std::pair<qdb_error_t, qdb_timespec_t> get_payload_timestamp(qdb_size_t r, qdb_size_t c) const
    {
        qdb_error_t err = check_out_of_bounds(r, c);
        qdb_timespec_t zero_ts;
        zero_ts.tv_sec = 0;
        zero_ts.tv_nsec = 0;

        if (err != qdb_e_ok) return std::make_pair(err, zero_ts);
        if (rows[r][c].type != qdb_query_result_value_type_t::qdb_query_result_timestamp) return std::make_pair(qdb_e_incompatible_type, zero_ts);
        return std::make_pair(err, rows[r][c].payload.timestamp_value);
    }
    
    std::string table_name;
    std::vector<std::string> columns_names;
    qdb_size_t columns_count;
    qdb_size_t rows_count;
    friend class copy_table;

private:
    std::vector < std::vector < wrap_qdb_point_result_t > > rows;
};

struct copy_point 
{
    wrap_qdb_point_result_t operator()(const qdb_point_result_t &that_point)
    {
        wrap_qdb_point_result_t this_point;
        this_point.type = that_point.type;
        switch (this_point.type)
        {
            case qdb_query_result_blob:       this_point.payload.blob_value = std::string(static_cast<const char*> (that_point.payload.blob.content),                                                                                                 that_point.payload.blob.content_length);
                                              break;

            case qdb_query_result_int64 :     this_point.payload.int_value = that_point.payload.int64_.value;
                                              break;

            case qdb_query_result_double :    this_point.payload.double_value = that_point.payload.double_.value;
                                              break;

            case qdb_query_result_timestamp : this_point.payload.timestamp_value = that_point.payload.timestamp.value;
                                              break;
        }
        return this_point;
    }
};

struct convert_qdb_string_to_std_string
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

        std::transform(that_table.columns_names, that_table.columns_names + that_table.columns_count, _tbl.columns_names.begin(), convert_qdb_string_to_std_string());

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
    wrap_qdb_query_result_t() {}
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
