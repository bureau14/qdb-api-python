struct qdb_const_iterator_t
{
    %immutable;
    qdb_handle_t handle;            /* [in] */
    const void * token;             /* [in] */

    const void * node;              /* [out] */
    const void * ref;               /* [out] */

    // we get rid of the const otherwise SWIG might leak memory
    char * alias;                   /* [out] */
    char * content;                 /* [out] */
    size_t  content_size;           /* [out] */
    %mutable;
} ;

struct qdb_remote_node_t
{
    // we get rid of the const otherwise SWIG might leak memory
    char * address;                 /* [in] */
    unsigned short port;            /* [in] */
    qdb_error_t error;              /* [out] */
};

struct qdb_operation_t
{
    
    qdb_operation_type_t type;      /* [in] */

    // we get rid of the const otherwise SWIG might leak memory
    char * alias;                   /* [in] */

    char * content;                  /* [in] */
    size_t content_size;            /* [in] */

    char * comparand;               /* [in] */
    size_t comparand_size;          /* [in] */

    qdb_time_t expiry_time;         /* [in] */

    %immutable;
    qdb_error_t error;              /* [out] */

    char * result;                  /* [out] API allocated */
    size_t result_size;             /* [out] */
    %mutable;
};

namespace qdb
{

struct batch_request
{
    batch_request(void) : type(qdb_op_uninitialized), expiry_time(0) {}
    batch_request(qdb_operation_type_t t, std::string a, qdb::api_buffer_ptr cont = qdb::api_buffer_ptr(), qdb::api_buffer_ptr comp = qdb::api_buffer_ptr(), qdb_time_t exp = 0) : type(t), alias(a), content(cont), comparand(comp), expiry_time(exp) {}

    qdb_operation_type_t type;
    std::string alias;

    // api_buffer_ptr make more sense for content, because most calls will return an api_buffer_ptr
    // this makes it easy to use the result from a previous operation to build a batch request
    qdb::api_buffer_ptr content;
    qdb::api_buffer_ptr comparand;

    qdb_time_t expiry_time;
    
};

struct batch_result
{
    batch_result(void) : error(qdb_e_uninitialized) {}
    explicit batch_result(qdb_operation_type_t t, std::string a, qdb_error_t err, qdb::api_buffer_ptr res = qdb::api_buffer_ptr()) : type(t), alias(a), error(err), result(res) {}

    // a request reminded
    %immutable;
    qdb_operation_type_t type;
    std::string alias;

    qdb_error_t error;
    qdb::api_buffer_ptr result;
    %mutable;
};

}

%inline%{

struct retval
{
    retval(void) : buffer(0), buffer_size(0) {}

    char * buffer;
    size_t buffer_size;
};

struct error_carrier
{
    error_carrier(void) : error(qdb_e_ok) {}

    qdb_error_t error;
};

struct run_batch_result
{
    run_batch_result(void) : successes(0) {}

    size_t successes;
    std::vector<qdb_operation_t> results;
};

%}
