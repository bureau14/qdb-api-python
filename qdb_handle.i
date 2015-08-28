namespace qdb
{

class handle
{

public:
    handle(void);
    ~handle(void);

private:
    handle(const handle &);

public:
    void close(void);
    bool connected(void) const;
    void set_timeout(int timeout);

    qdb_error_t connect(const char * uri);

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

    qdb_error_t stop_node(const char * uri, const char * reason);

    qdb_error_t remove(const char * alias);
    qdb_error_t blob_remove_if(const char * alias, const char * comparand, size_t comparand_length);
    qdb_error_t purge_all(void);
    qdb_error_t trim_all(void);

    qdb_error_t expires_at(const char * alias, qdb_time_t expiry_time);
    qdb_error_t expires_from_now(const char * alias, qdb_time_t expiry_delta);
    qdb_error_t get_expiry_time(const char * alias, qdb_time_t & expiry_time);

    // tags

    qdb_error_t add_tag(const char * alias, const char * tag);
    qdb_error_t has_tag(const char * alias, const char * tag);
    qdb_error_t remove_tag(const char * alias, const char * tag);

    std::vector<std::string> get_tagged(const char * tag, qdb_error_t & error);
    std::vector<std::string> get_tags(const char * alias, qdb_error_t & error);

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

    // hset
    qdb_error_t hset_insert(const char * alias, const char * content, size_t content_length);
    qdb_error_t hset_erase(const char * alias, const char * content, size_t content_length);
    qdb_error_t hset_contains(const char * alias, const char * content, size_t content_length);

private:
    qdb_handle_t _handle;

};

}
