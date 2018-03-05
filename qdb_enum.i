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

typedef enum
{
    qdb_e_ok = 0,

    // ------------------------------------------------------------------------------------------------------
    //  error name                      = origin                     | severity                     | code
    // ------------------------------------------------------------------------------------------------------
    qdb_e_uninitialized                 = qdb_e_origin_input         | qdb_e_severity_unrecoverable | 0xFFFF,
    qdb_e_alias_not_found               = qdb_e_origin_operation     | qdb_e_severity_warning       | 0x0008,
    qdb_e_alias_already_exists          = qdb_e_origin_operation     | qdb_e_severity_warning       | 0x0009,
    qdb_e_out_of_bounds                 = qdb_e_origin_input         | qdb_e_severity_warning       | 0x0019,
    qdb_e_skipped                       = qdb_e_origin_operation     | qdb_e_severity_warning       | 0x0021,
    qdb_e_incompatible_type             = qdb_e_origin_operation     | qdb_e_severity_warning       | 0x0022,
    qdb_e_container_empty               = qdb_e_origin_operation     | qdb_e_severity_warning       | 0x0023,
    qdb_e_container_full                = qdb_e_origin_operation     | qdb_e_severity_warning       | 0x0024,
    qdb_e_element_not_found             = qdb_e_origin_operation     | qdb_e_severity_info          | 0x0025,
    qdb_e_element_already_exists        = qdb_e_origin_operation     | qdb_e_severity_info          | 0x0026,
    qdb_e_overflow                      = qdb_e_origin_operation     | qdb_e_severity_warning       | 0x0027,
    qdb_e_underflow                     = qdb_e_origin_operation     | qdb_e_severity_warning       | 0x0028,
    qdb_e_tag_already_set               = qdb_e_origin_operation     | qdb_e_severity_info          | 0x0029,
    qdb_e_tag_not_set                   = qdb_e_origin_operation     | qdb_e_severity_info          | 0x002a,
    qdb_e_timeout                       = qdb_e_origin_connection    | qdb_e_severity_error         | 0x000a,
    qdb_e_connection_refused            = qdb_e_origin_connection    | qdb_e_severity_unrecoverable | 0x000e,
    qdb_e_connection_reset              = qdb_e_origin_connection    | qdb_e_severity_error         | 0x000f,
    qdb_e_unstable_cluster              = qdb_e_origin_connection    | qdb_e_severity_error         | 0x0012,
    qdb_e_try_again                     = qdb_e_origin_connection    | qdb_e_severity_error         | 0x0017,
    qdb_e_conflict                      = qdb_e_origin_operation     | qdb_e_severity_error         | 0x001a,
    qdb_e_not_connected                 = qdb_e_origin_connection    | qdb_e_severity_error         | 0x001b,
    qdb_e_resource_locked               = qdb_e_origin_operation     | qdb_e_severity_error         | 0x002d,
    // check errno or GetLastError() for actual error
    qdb_e_system_remote                 = qdb_e_origin_system_remote | qdb_e_severity_unrecoverable | 0x0001,
    qdb_e_system_local                  = qdb_e_origin_system_local  | qdb_e_severity_unrecoverable | 0x0001,

    qdb_e_internal_remote               = qdb_e_origin_system_remote | qdb_e_severity_unrecoverable | 0x0002,
    qdb_e_internal_local                = qdb_e_origin_system_local  | qdb_e_severity_unrecoverable | 0x0002,
    qdb_e_no_memory_remote              = qdb_e_origin_system_remote | qdb_e_severity_unrecoverable | 0x0003,
    qdb_e_no_memory_local               = qdb_e_origin_system_local  | qdb_e_severity_unrecoverable | 0x0003,
    qdb_e_invalid_protocol              = qdb_e_origin_protocol      | qdb_e_severity_unrecoverable | 0x0004,
    qdb_e_host_not_found                = qdb_e_origin_connection    | qdb_e_severity_error         | 0x0005,
    qdb_e_buffer_too_small              = qdb_e_origin_input         | qdb_e_severity_warning       | 0x000b,
    qdb_e_not_implemented               = qdb_e_origin_system_remote | qdb_e_severity_unrecoverable | 0x0011,
    qdb_e_invalid_version               = qdb_e_origin_protocol      | qdb_e_severity_unrecoverable | 0x0016,
    qdb_e_invalid_argument              = qdb_e_origin_input         | qdb_e_severity_error         | 0x0018,
    qdb_e_invalid_handle                = qdb_e_origin_input         | qdb_e_severity_error         | 0x001c,
    qdb_e_reserved_alias                = qdb_e_origin_input         | qdb_e_severity_error         | 0x001d,
    qdb_e_unmatched_content             = qdb_e_origin_operation     | qdb_e_severity_info          | 0x001e,
    qdb_e_invalid_iterator              = qdb_e_origin_input         | qdb_e_severity_error         | 0x001f,
    qdb_e_entry_too_large               = qdb_e_origin_input         | qdb_e_severity_error         | 0x002b,
    qdb_e_transaction_partial_failure   = qdb_e_origin_operation     | qdb_e_severity_error         | 0x002c,
    qdb_e_operation_disabled            = qdb_e_origin_operation     | qdb_e_severity_error         | 0x002e,
    qdb_e_operation_not_permitted       = qdb_e_origin_operation     | qdb_e_severity_error         | 0x002f,
    qdb_e_iterator_end                  = qdb_e_origin_operation     | qdb_e_severity_info          | 0x0030,
    qdb_e_invalid_reply                 = qdb_e_origin_protocol      | qdb_e_severity_unrecoverable | 0x0031,
    qdb_e_ok_created                    = qdb_e_origin_operation     | qdb_e_severity_info          | 0x0032,
    qdb_e_no_space_left                 = qdb_e_origin_system_remote | qdb_e_severity_unrecoverable | 0x0033,
    qdb_e_quota_exceeded                = qdb_e_origin_system_remote | qdb_e_severity_error         | 0x0034,
    qdb_e_alias_too_long                = qdb_e_origin_input         | qdb_e_severity_error         | 0x0035,
    qdb_e_clock_skew                    = qdb_e_origin_system_remote | qdb_e_severity_error         | 0x0036,
    qdb_e_access_denied                 = qdb_e_origin_operation     | qdb_e_severity_error         | 0x0037,
    qdb_e_login_failed                  = qdb_e_origin_system_remote | qdb_e_severity_error         | 0x0038,
    qdb_e_column_not_found              = qdb_e_origin_operation     | qdb_e_severity_warning       | 0x0039,
    qdb_e_query_too_complex             = qdb_e_origin_operation     | qdb_e_severity_error         | 0x0040,
    qdb_e_invalid_crypto_key            = qdb_e_origin_input         | qdb_e_severity_error         | 0x0041
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


