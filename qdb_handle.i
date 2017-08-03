namespace qdb
{

class handle
{
public:
    handle();
    ~handle();

private:
    handle(const handle &);

public:
    void close();
    bool connected() const;

    qdb_error_t connect(const char * uri);

    qdb_error_t set_timeout(int timeout_ms);
    qdb_error_t set_compression(qdb_compression_t comp_level);
    qdb_error_t set_encryption(qdb_encryption_t enc_level);
    qdb_error_t set_max_cardinality(qdb_uint_t max_cardinality);
    qdb_error_t set_user_credentials(const std::string & user_name, const std::string & private_key);
    qdb_error_t set_cluster_public_key(const std::string & public_key);

    qdb_error_t blob_put(const char * alias, const char * content, size_t content_length, qdb_time_t expiry_time);
    qdb_error_t blob_update(const char * alias, const char * content, size_t content_length, qdb_time_t expiry_time);

    api_buffer_ptr blob_get(const char * alias, qdb_error_t & error);
    api_buffer_ptr blob_get_and_remove(const char * alias, qdb_error_t & error);
    api_buffer_ptr blob_get_and_update(const char * alias, const char * update_content, size_t update_content_length, qdb_time_t expiry_time, qdb_error_t & error);
    api_buffer_ptr blob_compare_and_swap(const char * alias,
        const char * new_value,
        size_t new_value_length,
        const char * comparand,
        size_t comparand_length,
        qdb_time_t expiry_time,
        qdb_error_t & error);

    std::string node_status(const char * uri, qdb_error_t & error);
    std::string node_config(const char * uri, qdb_error_t & error);
    std::string node_topology(const char * uri, qdb_error_t & error);

    qdb_error_t node_stop(const char * uri, const char * reason);

    qdb_error_t remove(const char * alias);
    qdb_error_t blob_remove_if(const char * alias, const char * comparand, size_t comparand_length);
    qdb_error_t purge_all(int timeout);
    qdb_error_t trim_all(int timeout);

    qdb_error_t expires_at(const char * alias, qdb_time_t expiry_time);
    qdb_error_t expires_from_now(const char * alias, qdb_time_t expiry_delta);
    qdb_error_t get_expiry_time(const char * alias, qdb_time_t & expiry_time);

    // tags
    qdb_error_t attach_tag(const char * alias, const char * tag);
    qdb_error_t has_tag(const char * alias, const char * tag);
    qdb_error_t detach_tag(const char * alias, const char * tag);

    std::vector<std::string> get_tagged(const char * tag, qdb_error_t & error);
    std::vector<std::string> get_tags(const char * alias, qdb_error_t & error);

    qdb_uint_t get_tagged_count(const char * tag, qdb_error_t & error);

    // integer
    qdb_error_t int_get(const char * alias, qdb_int_t * number);
    qdb_error_t int_put(const char * alias, qdb_int_t number, qdb_time_t expiry_time);
    qdb_error_t int_update(const char * alias, qdb_int_t number, qdb_time_t expiry_time);
    qdb_error_t int_add(const char * alias, qdb_int_t addend, qdb_int_t * result = NULL);

    // deque
    qdb_error_t deque_push_front(const char * alias, const char * content, size_t content_length);
    qdb_error_t deque_push_back(const char * alias, const char * content, size_t content_length);
    api_buffer_ptr deque_pop_front(const char * alias, qdb_error_t & error);
    api_buffer_ptr deque_pop_back(const char * alias, qdb_error_t & error);
    api_buffer_ptr deque_front(const char * alias, qdb_error_t & error);
    api_buffer_ptr deque_back(const char * alias, qdb_error_t & error);
    qdb_error_t deque_size(const char * alias, qdb_size_t * size);

    // hset
    qdb_error_t hset_insert(const char * alias, const char * content, size_t content_length);
    qdb_error_t hset_erase(const char * alias, const char * content, size_t content_length);
    qdb_error_t hset_contains(const char * alias, const char * content, size_t content_length);

    std::vector<std::string> blob_scan(const void * pattern,
                                   qdb_size_t pattern_length,
                                   qdb_int_t max_count,
                                   qdb_error_t & error);

    std::vector<std::string> blob_scan_regex(const char * pattern,
                                             qdb_int_t max_count,
                                             qdb_error_t & error);

private:
    qdb_handle_t _handle;

};

} // namespace qdb
