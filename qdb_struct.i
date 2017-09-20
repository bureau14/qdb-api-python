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
    const void * content;
    qdb_size_t content_length;
} qdb_ts_blob_point;

typedef struct
{
    //! Beginning of the interval, inclusive.
    qdb_timespec_t begin;
    //! End of the interval, exclusive.
    qdb_timespec_t end;

    //! Filter the interval
    qdb_ts_filter_type_t filter;

    //! Optional filter parameters
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

    } filter_params;

} qdb_ts_range_t;

typedef struct qdb_ts_blob_aggregation
{
    qdb_ts_aggregation_type_t type;
    qdb_ts_range_t range;
    qdb_size_t count;
    qdb_ts_blob_point result;
} qdb_ts_blob_aggregation_t;

typedef struct qdb_ts_double_aggregation
{
    qdb_ts_aggregation_type_t type;
    qdb_ts_range_t range;
    qdb_size_t count;
    qdb_ts_double_point result;
} qdb_ts_double_aggregation_t;
