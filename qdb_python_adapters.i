%inline%{

namespace qdb
{

// thanks to the definitions, content/content_length will be matched for a Python string
qdb::api_buffer_ptr make_api_buffer_ptr_from_string(handle_ptr h, const char * content, size_t content_length)
{
    if (!h || !content || !content_length)
    {
        return qdb::api_buffer_ptr();
    }

    return qdb::make_api_buffer_ptr(*h, content, content_length);
}

std::vector<std::string> prefix_get(handle_ptr h, const char * prefix, error_carrier * error)
{
    return h->prefix_get(prefix, error->error);
}

run_batch_result run_batch(handle_ptr h, const std::vector<batch_request> & requests)
{
    run_batch_result br;
    br.results = h->run_batch(requests, br.successes);
    return br;
}

api_buffer_ptr get(handle_ptr h, const char * alias, error_carrier * error)
{
    return h->get(alias, error->error);
}

api_buffer_ptr get_remove(handle_ptr h, const char * alias, error_carrier * error)
{
    return h->get_remove(alias, error->error);
}

api_buffer_ptr get_update(handle_ptr h, const char * alias, const char * update_content, size_t update_content_length, qdb_time_t expiry_time, error_carrier * error)
{
    return h->get_update(alias, update_content, update_content_length, expiry_time, error->error);
}

api_buffer_ptr compare_and_swap(handle_ptr h, const char * alias,
    const char * new_value,
    size_t new_value_length,
    const char * comparand,
    size_t comparand_length,
    qdb_time_t expiry_time,
    error_carrier * error)
{
    return h->compare_and_swap(alias, new_value, new_value_length, comparand, comparand_length, expiry_time, error->error);
}

std::string node_status(handle_ptr h, const qdb_remote_node_t * node, error_carrier * error)
{
    return h->node_status(*node, error->error);
}

std::string node_config(handle_ptr h, const qdb_remote_node_t * node, error_carrier * error)
{
    return h->node_config(*node, error->error);
}

std::string node_topology(handle_ptr h, const qdb_remote_node_t * node, error_carrier * error)
{
    return h->node_topology(*node, error->error);
}

handle_ptr connect(const qdb_remote_node_t * node, error_carrier * error)
{
    handle_ptr h(new handle());
    error->error = h->connect(*node);
    return h;
}

// operators are not supported in Python, we need this helper
void iterator_next(const_iterator * iterator)
{
    ++(*iterator);
}

void iterator_previous(const_iterator * iterator)
{
    --(*iterator);
}

std::string get_iterator_key(const const_iterator * iterator)
{
    return (*iterator)->first;
}

api_buffer_ptr get_iterator_value(const const_iterator * iterator)
{
    return (*iterator)->second;
}

qdb_time_t get_expiry_time_wrapper(handle_ptr h, const char * alias, error_carrier * error)
{
    qdb_time_t val = 0;
    error->error = h->get_expiry_time(alias, val);
    return val;
}

}
%}
