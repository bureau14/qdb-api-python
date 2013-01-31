%inline%{
namespace qdb
{

api_buffer_ptr get(handle_ptr h, const char * alias, error_carrier * error)
{
    return h->get(alias, error->error);
}

api_buffer_ptr get_update(handle_ptr h, const char * alias, const char * update_content, size_t update_content_length, error_carrier * error)
{
    return h->get_update(alias, update_content, update_content_length, error->error);
}

api_buffer_ptr compare_and_swap(handle_ptr h, const char * alias,
    const char * new_value,
    size_t new_value_length,
    const char * comparand,
    size_t comparand_length,
    error_carrier * error)
{
    return h->compare_and_swap(alias, new_value, new_value_length, comparand, comparand_length, error->error);
}

handle_ptr connect(const char * host, unsigned short port, error_carrier * error)
{
    handle_ptr h(new handle());
    error->error = h->connect(host, port);
    return h;
}

}
%}