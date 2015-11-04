enum qdb_limits_t
{
    qdb_l_alias_max_length = 1024,
    qdb_l_min_prefix_length = 3
};

#define QDB_STATUS_SUCCESS   0x00000000
#define QDB_STATUS_INFO      0x20000000
#define QDB_STATUS_TRANSIENT 0xc0000000
#define QDB_STATUS_ERROR     0xe0000000

#define QDB_SEVERITY(x)     ((x) & 0xe0000000)

#define QDB_SUCCESS(x)      ((QDB_SEVERITY(x) == QDB_STATUS_SUCCESS) || (QDB_SEVERITY(x) == QDB_STATUS_INFO))
#define QDB_TRANSIENT(x)    (QDB_SEVERITY(x) == QDB_STATUS_TRANSIENT)

#define MAKE_QDB_STATUS_CODE(Severity, Code) ((Severity) | (Code))

enum qdb_error_t
{
    qdb_e_uninitialized                 = MAKE_QDB_STATUS_CODE(QDB_STATUS_ERROR,        0xFFFF),
    qdb_e_ok                            = MAKE_QDB_STATUS_CODE(QDB_STATUS_SUCCESS,      0),
    qdb_e_alias_not_found               = MAKE_QDB_STATUS_CODE(QDB_STATUS_INFO,         8),
    qdb_e_alias_already_exists          = MAKE_QDB_STATUS_CODE(QDB_STATUS_INFO,         9),
    qdb_e_out_of_bounds                 = MAKE_QDB_STATUS_CODE(QDB_STATUS_INFO,         25),
    qdb_e_skipped                       = MAKE_QDB_STATUS_CODE(QDB_STATUS_INFO,         33),
    qdb_e_incompatible_type             = MAKE_QDB_STATUS_CODE(QDB_STATUS_INFO,         34),
    qdb_e_container_empty               = MAKE_QDB_STATUS_CODE(QDB_STATUS_INFO,         35),
    qdb_e_container_full                = MAKE_QDB_STATUS_CODE(QDB_STATUS_INFO,         36),
    qdb_e_element_not_found             = MAKE_QDB_STATUS_CODE(QDB_STATUS_INFO,         37),
    qdb_e_element_already_exists        = MAKE_QDB_STATUS_CODE(QDB_STATUS_INFO,         38),
    qdb_e_overflow                      = MAKE_QDB_STATUS_CODE(QDB_STATUS_INFO,         39),
    qdb_e_underflow                     = MAKE_QDB_STATUS_CODE(QDB_STATUS_INFO,         40),
    qdb_e_tag_already_set               = MAKE_QDB_STATUS_CODE(QDB_STATUS_INFO,         41),
    qdb_e_tag_not_set                   = MAKE_QDB_STATUS_CODE(QDB_STATUS_INFO,         42),
    qdb_e_timeout                       = MAKE_QDB_STATUS_CODE(QDB_STATUS_TRANSIENT,    10),
    qdb_e_connection_refused            = MAKE_QDB_STATUS_CODE(QDB_STATUS_TRANSIENT,    14),
    qdb_e_connection_reset              = MAKE_QDB_STATUS_CODE(QDB_STATUS_TRANSIENT,    15),
    qdb_e_unstable_cluster              = MAKE_QDB_STATUS_CODE(QDB_STATUS_TRANSIENT,    18),
    qdb_e_outdated_topology             = MAKE_QDB_STATUS_CODE(QDB_STATUS_TRANSIENT,    20),
    qdb_e_wrong_peer                    = MAKE_QDB_STATUS_CODE(QDB_STATUS_TRANSIENT,    21),
    qdb_e_try_again                     = MAKE_QDB_STATUS_CODE(QDB_STATUS_TRANSIENT,    23),
    qdb_e_conflict                      = MAKE_QDB_STATUS_CODE(QDB_STATUS_TRANSIENT,    26),
    qdb_e_not_connected                 = MAKE_QDB_STATUS_CODE(QDB_STATUS_TRANSIENT,    27),
    qdb_e_resource_locked               = MAKE_QDB_STATUS_CODE(QDB_STATUS_TRANSIENT,    45),
    qdb_e_system                        = MAKE_QDB_STATUS_CODE(QDB_STATUS_ERROR,        1), /* check errno or GetLastError() for actual error */
    qdb_e_internal                      = MAKE_QDB_STATUS_CODE(QDB_STATUS_ERROR,        2),
    qdb_e_no_memory                     = MAKE_QDB_STATUS_CODE(QDB_STATUS_ERROR,        3),
    qdb_e_invalid_protocol              = MAKE_QDB_STATUS_CODE(QDB_STATUS_ERROR,        4),
    qdb_e_host_not_found                = MAKE_QDB_STATUS_CODE(QDB_STATUS_ERROR,        5),
    qdb_e_buffer_too_small              = MAKE_QDB_STATUS_CODE(QDB_STATUS_ERROR,        11),
    qdb_e_unexpected_reply              = MAKE_QDB_STATUS_CODE(QDB_STATUS_ERROR,        16),
    qdb_e_not_implemented               = MAKE_QDB_STATUS_CODE(QDB_STATUS_ERROR,        17),
    qdb_e_protocol_error                = MAKE_QDB_STATUS_CODE(QDB_STATUS_ERROR,        19),
    qdb_e_invalid_version               = MAKE_QDB_STATUS_CODE(QDB_STATUS_ERROR,        22),
    qdb_e_invalid_argument              = MAKE_QDB_STATUS_CODE(QDB_STATUS_ERROR,        24),
    qdb_e_invalid_handle                = MAKE_QDB_STATUS_CODE(QDB_STATUS_ERROR,        28),
    qdb_e_reserved_alias                = MAKE_QDB_STATUS_CODE(QDB_STATUS_ERROR,        29),
    qdb_e_unmatched_content             = MAKE_QDB_STATUS_CODE(QDB_STATUS_ERROR,        30),
    qdb_e_invalid_iterator              = MAKE_QDB_STATUS_CODE(QDB_STATUS_ERROR,        31),
    qdb_e_entry_too_large               = MAKE_QDB_STATUS_CODE(QDB_STATUS_ERROR,        43),
    qdb_e_transaction_partial_failure   = MAKE_QDB_STATUS_CODE(QDB_STATUS_ERROR,        44),
};

enum qdb_compression_t
{
    qdb_comp_none = 0,
    qdb_comp_fast = 1, /* default */
    qdb_comp_best = 2  /* not implemented yet */
};

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
    qdb_op_blob_remove = 3,
    qdb_op_blob_cas = 4,
    qdb_op_blob_get_and_update = 5,
    qdb_op_blob_get_and_remove = 6,
    qdb_op_blob_remove_if = 7
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
