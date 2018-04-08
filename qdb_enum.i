enum qdb_limits_t
{
    qdb_l_alias_max_length = 1024
};

typedef enum
{
    qdb_e_origin_system_remote      = (int) 0xf0000000,
    qdb_e_origin_system_local       = (int) 0xe0000000,
    qdb_e_origin_connection         = (int) 0xd0000000,
    qdb_e_origin_input              = (int) 0xc0000000,
    qdb_e_origin_operation          = (int) 0xb0000000,
    qdb_e_origin_protocol           = (int) 0xa0000000
} qdb_error_origin_t;

#define QDB_ERROR_ORIGIN(x)         ((int)((x) & 0xf0000000))

typedef enum
{
    qdb_e_severity_unrecoverable    = 0x03000000,
    qdb_e_severity_error            = 0x02000000,
    qdb_e_severity_warning          = 0x01000000,
    qdb_e_severity_info             = 0x00000000,
} qdb_error_severity_t;

#define QDB_ERROR_SEVERITY(x)       ((x) & 0x0f000000)

#define QDB_SUCCESS(x)              (!(x) || (QDB_ERROR_SEVERITY(x) == qdb_e_severity_info))

#define QDB_FAILURE(x)              (!QDB_SUCCESS(x))

typedef enum qdb_error_t
{
    //! Success.
    qdb_e_ok = 0,

    // ------------------------------------------------------------------------------------------------------
    //  error name                      = origin                     | severity                     | code
    // ------------------------------------------------------------------------------------------------------
    //! Uninitialized error.
    qdb_e_uninitialized                 = qdb_e_origin_input         | qdb_e_severity_unrecoverable | 0xFFFF,
    //! Entry alias/key was not found.
    qdb_e_alias_not_found               = qdb_e_origin_operation     | qdb_e_severity_warning       | 0x0008,
    //! Entry alias/key already exists.
    qdb_e_alias_already_exists          = qdb_e_origin_operation     | qdb_e_severity_warning       | 0x0009,
    //! Index out of bounds.
    qdb_e_out_of_bounds                 = qdb_e_origin_input         | qdb_e_severity_warning       | 0x0019,
    //! Skipped operation. Used in batches and transactions.
    qdb_e_skipped                       = qdb_e_origin_operation     | qdb_e_severity_warning       | 0x0021,
    //! Entry or column is incompatible with the operation.
    qdb_e_incompatible_type             = qdb_e_origin_operation     | qdb_e_severity_warning       | 0x0022,
    //! Container is empty.
    qdb_e_container_empty               = qdb_e_origin_operation     | qdb_e_severity_warning       | 0x0023,
    //! Container is full.
    qdb_e_container_full                = qdb_e_origin_operation     | qdb_e_severity_warning       | 0x0024,
    //! Element was not found.
    qdb_e_element_not_found             = qdb_e_origin_operation     | qdb_e_severity_info          | 0x0025,
    //! Element already exists.
    qdb_e_element_already_exists        = qdb_e_origin_operation     | qdb_e_severity_info          | 0x0026,
    //! Arithmetic operation overflows.
    qdb_e_overflow                      = qdb_e_origin_operation     | qdb_e_severity_warning       | 0x0027,
    //! Arithmetic operation underflows.
    qdb_e_underflow                     = qdb_e_origin_operation     | qdb_e_severity_warning       | 0x0028,
    //! Tag is already set.
    qdb_e_tag_already_set               = qdb_e_origin_operation     | qdb_e_severity_info          | 0x0029,
    //! Tag is not set.
    qdb_e_tag_not_set                   = qdb_e_origin_operation     | qdb_e_severity_info          | 0x002a,
    //! Operation timed out.
    qdb_e_timeout                       = qdb_e_origin_connection    | qdb_e_severity_error         | 0x000a,
    //! Connection was refused.
    qdb_e_connection_refused            = qdb_e_origin_connection    | qdb_e_severity_unrecoverable | 0x000e,
    //! Connection was reset.
    qdb_e_connection_reset              = qdb_e_origin_connection    | qdb_e_severity_error         | 0x000f,
    //! Cluster is unstable.
    qdb_e_unstable_cluster              = qdb_e_origin_connection    | qdb_e_severity_error         | 0x0012,
    //! Please retry.
    qdb_e_try_again                     = qdb_e_origin_connection    | qdb_e_severity_error         | 0x0017,
    //! There is another ongoing conflicting operation.
    qdb_e_conflict                      = qdb_e_origin_operation     | qdb_e_severity_error         | 0x001a,
    //! Handle is not connected.
    qdb_e_not_connected                 = qdb_e_origin_connection    | qdb_e_severity_error         | 0x001b,
    //! Resource is locked.
    qdb_e_resource_locked               = qdb_e_origin_operation     | qdb_e_severity_error         | 0x002d,
    //! System error on remote node (server-side).
    //! Please check `errno` or `GetLastError()` for actual error.
    qdb_e_system_remote                 = qdb_e_origin_system_remote | qdb_e_severity_unrecoverable | 0x0001,
    //! System error on local system (client-side).
    //! Please check `errno` or `GetLastError()` for actual error.
    qdb_e_system_local                  = qdb_e_origin_system_local  | qdb_e_severity_unrecoverable | 0x0001,
    //! Internal error on remote node (server-side).
    qdb_e_internal_remote               = qdb_e_origin_system_remote | qdb_e_severity_unrecoverable | 0x0002,
    //! Internal error on local system (client-side).
    qdb_e_internal_local                = qdb_e_origin_system_local  | qdb_e_severity_unrecoverable | 0x0002,
    //! No memory on remote node (server-side).
    qdb_e_no_memory_remote              = qdb_e_origin_system_remote | qdb_e_severity_unrecoverable | 0x0003,
    //! No memory on local system (client-side).
    qdb_e_no_memory_local               = qdb_e_origin_system_local  | qdb_e_severity_unrecoverable | 0x0003,
    //! Protocol is invalid.
    qdb_e_invalid_protocol              = qdb_e_origin_protocol      | qdb_e_severity_unrecoverable | 0x0004,
    //! Host was not found.
    qdb_e_host_not_found                = qdb_e_origin_connection    | qdb_e_severity_error         | 0x0005,
    //! Buffer is too small.
    qdb_e_buffer_too_small              = qdb_e_origin_input         | qdb_e_severity_warning       | 0x000b,
    //! Operation is not implemented.
    qdb_e_not_implemented               = qdb_e_origin_system_remote | qdb_e_severity_unrecoverable | 0x0011,
    //! Version is invalid.
    qdb_e_invalid_version               = qdb_e_origin_protocol      | qdb_e_severity_unrecoverable | 0x0016,
    //! Argument is invalid.
    qdb_e_invalid_argument              = qdb_e_origin_input         | qdb_e_severity_error         | 0x0018,
    //! Handle is invalid.
    qdb_e_invalid_handle                = qdb_e_origin_input         | qdb_e_severity_error         | 0x001c,
    //! Alias/key is reserved.
    qdb_e_reserved_alias                = qdb_e_origin_input         | qdb_e_severity_error         | 0x001d,
    //! Content did not match.
    qdb_e_unmatched_content             = qdb_e_origin_operation     | qdb_e_severity_info          | 0x001e,
    //! Iterator is invalid.
    qdb_e_invalid_iterator              = qdb_e_origin_input         | qdb_e_severity_error         | 0x001f,
    //! Entry is too large.
    qdb_e_entry_too_large               = qdb_e_origin_input         | qdb_e_severity_error         | 0x002b,
    //! Transaction failed partially.
    //! \warning This may provoke failures until the transaction has not been rolled back.
    //! \see Cluster configuration parameter `global/cluster/max_transaction_duration`.
    qdb_e_transaction_partial_failure   = qdb_e_origin_operation     | qdb_e_severity_error         | 0x002c,
    //! Operation has not been enabled in cluster configuration.
    qdb_e_operation_disabled            = qdb_e_origin_operation     | qdb_e_severity_error         | 0x002e,
    //! Operation is not permitted.
    qdb_e_operation_not_permitted       = qdb_e_origin_operation     | qdb_e_severity_error         | 0x002f,
    //! Iterator reached the end.
    qdb_e_iterator_end                  = qdb_e_origin_operation     | qdb_e_severity_info          | 0x0030,
    //! Cluster sent an invalid reply.
    qdb_e_invalid_reply                 = qdb_e_origin_protocol      | qdb_e_severity_unrecoverable | 0x0031,
    //! Success. A new entry has been created.
    qdb_e_ok_created                    = qdb_e_origin_operation     | qdb_e_severity_info          | 0x0032,
    //! No more space on disk.
    qdb_e_no_space_left                 = qdb_e_origin_system_remote | qdb_e_severity_unrecoverable | 0x0033,
    //! Disk space quota has been reached.
    qdb_e_quota_exceeded                = qdb_e_origin_system_remote | qdb_e_severity_unrecoverable | 0x0034,
    //! Alias is too long.
    //! \see \ref qdb_l_max_alias_length
    qdb_e_alias_too_long                = qdb_e_origin_input         | qdb_e_severity_error         | 0x0035,
    //! Cluster nodes have important clock differences.
    qdb_e_clock_skew                    = qdb_e_origin_system_remote | qdb_e_severity_error         | 0x0036,
    //! Access is denied.
    qdb_e_access_denied                 = qdb_e_origin_operation     | qdb_e_severity_error         | 0x0037,
    //! Login failed.
    qdb_e_login_failed                  = qdb_e_origin_system_remote | qdb_e_severity_error         | 0x0038,
    //! Column was not found.
    qdb_e_column_not_found              = qdb_e_origin_operation     | qdb_e_severity_warning       | 0x0039,
    //! Query is too complex.
    qdb_e_query_too_complex             = qdb_e_origin_operation     | qdb_e_severity_error         | 0x0040,
    //! Security key is invalid.
    qdb_e_invalid_crypto_key            = qdb_e_origin_input         | qdb_e_severity_error         | 0x0041,
    //! Malformed query
    qdb_e_invalid_query                 = qdb_e_origin_input         | qdb_e_severity_error         | 0x0042,
    //! Malformed regex
    qdb_e_invalid_regex                 = qdb_e_origin_input         | qdb_e_severity_error         | 0x0043
} qdb_error_t;

typedef qdb_error_t qdb_status_t;

enum qdb_compression_t
{
    qdb_comp_none = 0,
    qdb_comp_fast = 1, /* default */
    qdb_comp_best = 2  /* not implemented yet */
};

typedef enum qdb_encryption {
    qdb_crypt_none = 0,
    qdb_crypt_aes_gcm_256 = 1
} qdb_encryption_t;

enum qdb_protocol_t
{
    qdb_p_tcp = 0
};

enum qdb_entry_type_t
{
    qdb_entry_uninitialized = -1,
    qdb_entry_blob = 0,
    qdb_entry_integer = 1,
    qdb_entry_hset = 2,
    qdb_entry_tag = 3,
    qdb_entry_deque = 4
};

enum qdb_operation_type_t
{
    qdb_op_uninitialized = -1,
    qdb_op_blob_get = 0,
    qdb_op_blob_put = 1,
    qdb_op_blob_update = 2,
    // qdb_op_remove = 3,
    qdb_op_blob_cas = 4,
    qdb_op_blob_get_and_update = 5,
    // qdb_op_blob_get_and_remove = 6,
    // qdb_op_blob_remove_if = 7,
    qdb_op_has_tag = 8,
    qdb_op_int_put = 9,
    qdb_op_int_update = 10,
    qdb_op_int_get = 11,
    qdb_op_int_add = 12
};

enum qdb_log_level_t
{
    qdb_log_detailed = 100,
    qdb_log_debug = 200,
    qdb_log_info = 300,
    qdb_log_warning = 400,
    qdb_log_error = 500,
    qdb_log_panic = 600
};

enum qdb_ts_aggregation_type_t
{
    qdb_agg_first = 0,
    qdb_agg_last = 1,
    qdb_agg_min = 2,
    qdb_agg_max = 3,
    qdb_agg_arithmetic_mean = 4,
    qdb_agg_harmonic_mean = 5,
    qdb_agg_geometric_mean = 6,
    qdb_agg_quadratic_mean = 7,
    qdb_agg_count = 8,
    qdb_agg_sum = 9,
    qdb_agg_sum_of_squares = 10,
    qdb_agg_spread = 11,
    qdb_agg_sample_variance = 12,
    qdb_agg_sample_stddev = 13,
    qdb_agg_population_variance = 14,
    qdb_agg_population_stddev = 15,
    qdb_agg_abs_min = 16,
    qdb_agg_abs_max = 17,
    qdb_agg_product = 18,
    qdb_agg_skewness = 19,
    qdb_agg_kurtosis = 20
};

enum qdb_ts_column_type_t
{
    qdb_ts_column_uninitialized = -1,
    qdb_ts_column_double = 0,
    qdb_ts_column_blob = 1,
    qdb_ts_column_int64 = 2,
    qdb_ts_column_timestamp = 3
};

enum qdb_ts_filter_type_t
{
    qdb_ts_filter_none = 0,
    qdb_ts_filter_unique = 1,
    qdb_ts_filter_sample = 2,
    qdb_ts_filter_double_inside_range = 3,
    qdb_ts_filter_double_outside_range = 4
};

enum qdb_duration_t
{
    qdb_d_millisecond = 1,
    qdb_d_second = qdb_d_millisecond * 1000,
    qdb_d_minute = qdb_d_second * 60,
    qdb_d_hour = qdb_d_minute * 60,
    qdb_d_day = qdb_d_hour * 24,
    qdb_d_week = qdb_d_day * 7,

    qdb_d_default_shard_size = qdb_d_day
};
