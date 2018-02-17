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

struct wrap_qdb_table_result_t
{
    wrap_qdb_table_result_t& operator = (const qdb_table_result_t& tbl)
    {
        rows_count = tbl.rows_count;
        columns_count = tbl.columns_count;
        table_name = tbl.table_name.data;

        std::transform(tbl.columns_names , tbl.columns_names + tbl.columns_count, std::back_inserter(columns_names), [](const qdb_string_t &str) { return std::string{str.data}; });
        rows.resize(rows_count);

        for (qdb_size_t row = 0; row < rows_count; ++row) 
        {
            rows[row].resize(columns_count);
            for(qdb_size_t col = 0 ; col < columns_count; ++col)
            {
                rows[row][col] = tbl.rows[row][col];
                // Copy assignment does a shallow copy of the const void pointer present in blob.
                // We must deep copy the blob, because we call qdb_release after the query_exp.
                if(rows[row][col].type == qdb_query_result_value_type_t::qdb_query_result_blob)
                {
                    qdb_size_t len =  rows[row][col].payload.blob.content_length;
                    char *content = new char[len];
                    std::memcpy(static_cast<void*> (content), rows[row][col].payload.blob.content, len);
                    rows[row][col].payload.blob.content = static_cast<const char*>(content);
                }
            }
        }
        return *this;
    }

    qdb_query_result_value_type_t get_type(qdb_size_t r, qdb_size_t c) const
    {
        assert(r >= 0 && c >= 0 && r < rows_count && c < columns_count);
        return rows[r][c].type;
    }

    std::string get_payload_blob(qdb_size_t r, qdb_size_t c) const
    {
        assert(r >= 0 && c >= 0 && r < rows_count && c < columns_count && rows[r][c].type == qdb_query_result_value_type_t::qdb_query_result_blob);
        return std::string {static_cast<const char *> (rows[r][c].payload.blob.content), rows[r][c].payload.blob.content_length};
    }

    qdb_int_t get_payload_int64(qdb_size_t r, qdb_size_t c) const
    {
        assert(r >= 0 && c >= 0 && r < rows_count && c < columns_count && rows[r][c].type == qdb_query_result_value_type_t::qdb_query_result_int64);
        return rows[r][c].payload.int64_.value;
    }

    double get_payload_double(qdb_size_t r, qdb_size_t c) const
    {
        assert(r >= 0 && c >= 0 && r < rows_count && c < columns_count && rows[r][c].type == qdb_query_result_value_type_t::qdb_query_result_double);
        return rows[r][c].payload.double_.value;
    }

    qdb_timespec_t get_payload_timestamp(qdb_size_t r, qdb_size_t c) const
    {
        assert(r >= 0 && c >= 0 && r < rows_count && c < columns_count && rows[r][c].type == qdb_query_result_value_type_t::qdb_query_result_timestamp);
        return rows[r][c].payload.timestamp.value;
    }


    ~wrap_qdb_table_result_t()
    {
         for (qdb_size_t row = 0; row < rows_count; ++row) 
         {
            for(qdb_size_t col = 0 ; col < columns_count; ++col)
            {
                if(rows[row][col].type == qdb_query_result_value_type_t::qdb_query_result_blob)
                delete [] rows[row][col].payload.blob.content;
            }
            rows[row].clear();
        }
        rows.clear();
    }

    std::string table_name;
    std::vector<std::string> columns_names;
    qdb_size_t columns_count;
    qdb_size_t rows_count;

    private:
        std::vector < std::vector < qdb_point_result_t > > rows;

};

struct wrap_qdb_query_result_t
{
    wrap_qdb_query_result_t() = default;
    wrap_qdb_query_result_t(qdb_query_result_t *res)
    {
        tables_count = res->tables_count; 
        scanned_rows_count = res->scanned_rows_count;
        tables.resize(tables_count);
        std::copy(res->tables , res->tables + res->tables_count, tables.begin());
    }
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