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
    const_iterator begin(void);

    const_iterator end(void);

    const_reverse_iterator rbegin(void);

    const_reverse_iterator rend(void);

public:
    void close(void);
    bool connected(void) const;
    void set_timeout(int timeout);

    qdb_error_t connect(const char * uri);

    qdb_error_t put(const char * alias, const char * content, size_t content_length, qdb_time_t expiry_time);
    qdb_error_t update(const char * alias, const char * content, size_t content_length, qdb_time_t expiry_time);

    std::vector<std::string> prefix_get(const char * prefix, qdb_error_t & error);

    api_buffer_ptr get(const char * alias, qdb_error_t & error);
    api_buffer_ptr get_and_remove(const char * alias, qdb_error_t & error);
    api_buffer_ptr get_and_update(const char * alias, const char * update_content, size_t update_content_length, qdb_time_t expiry_time, qdb_error_t & error);
    api_buffer_ptr compare_and_swap(const char * alias,
        const char * new_value,
        size_t new_value_length,
        const char * comparand,
        size_t comparand_length,
        qdb_time_t expiry_time,
        qdb_error_t & error);

    std::vector<batch_result> run_batch(const std::vector<batch_request> & requests, size_t & successes_count);

    std::string node_status(const char * uri, qdb_error_t & error);
    std::string node_config(const char * uri, qdb_error_t & error);
    std::string node_topology(const char * uri, qdb_error_t & error);

    qdb_error_t stop_node(const char * uri, const char * reason);

    qdb_error_t remove(const char * alias);
    qdb_error_t remove_if(const char * alias, const char * comparand, size_t comparand_length);
    qdb_error_t purge_all(void);
    qdb_error_t trim_all(void);

    qdb_error_t expires_at(const char * alias, qdb_time_t expiry_time);
    qdb_error_t expires_from_now(const char * alias, qdb_time_t expiry_delta);
    qdb_error_t get_expiry_time(const char * alias, qdb_time_t & expiry_time);

private:
    qdb_handle_t _handle;

};

}
