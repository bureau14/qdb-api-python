%module(package="qdb") qdb
%{

#include <qdb/client.hpp>

%}


%include <std_shared_ptr.i>
%shared_ptr(qdb::api_buffer)
%shared_ptr(qdb::handle)

%include typemaps.i
%include cpointer.i

%apply (const char *STRING, size_t LENGTH) { (const char * content, size_t content_length) };
%apply (const char *STRING, size_t LENGTH) { (const char * update_content, size_t update_content_length) };
%apply (const char *STRING, size_t LENGTH) { (const char * new_value, size_t new_value_length) };
%apply (const char *STRING, size_t LENGTH) { (const char * comparand, size_t comparand_length) };

%rename("%(regex:/qdb_e(.*)/error\\1/)s", %$isenumitem) "";
%rename("%(regex:/qdb_o(.*)/option\\1/)s", %$isenumitem) "";
%rename("%(strip:[qdb_])s", %$isfunction) "";

enum qdb_error_t
{
    qdb_e_ok = 0,
    qdb_e_system = 1,                         /* check errno or GetLastError() for actual error */
    qdb_e_internal = 2,
    qdb_e_no_memory = 3,
    qdb_e_invalid_protocol = 4,
    qdb_e_host_not_found = 5,
    qdb_e_invalid_option = 6,
    qdb_e_alias_too_long = 7,
    qdb_e_alias_not_found = 8,
    qdb_e_alias_already_exists = 9,
    qdb_e_timeout = 10,
    qdb_e_buffer_too_small = 11,
    qdb_e_invalid_command = 12,
    qdb_e_invalid_input = 13,
    qdb_e_connection_refused = 14,
    qdb_e_connection_reset = 15,
    qdb_e_unexpected_reply = 16,
    qdb_e_not_implemented = 17,
    qdb_e_unstable_hive = 18,
    qdb_e_protocol_error = 19,
    qdb_e_outdated_topology = 20,
    qdb_e_wrong_peer = 21,
    qdb_e_invalid_version = 22,
    qdb_e_try_again = 23,
    qdb_e_invalid_argument = 24,
    qdb_e_out_of_bounds = 25,
    qdb_e_conflict = 26,
    qdb_e_not_connected = 27,
    qdb_e_invalid_handle = 28,
    qdb_e_reserved_alias = 29
};

enum qdb_option_t
{
    qdb_o_operation_timeout = 0,                /* int */
    qdb_o_stream_buffer_size
};

%rename(version) qdb_version;
const char * qdb_version();

%rename(build) qdb_build;
const char * qdb_build();

%inline%{
namespace qdb
{
struct error_carrier
{
    qdb_error_t error;
};
}
%}

namespace qdb
{

class api_buffer
{

public:
    api_buffer(qdb_handle_t h, const char * data, size_t length);
    ~api_buffer(void);

private:
    // prevent copy
    api_buffer(const api_buffer &);

public:
    const char * data(void) const;
    size_t size(void) const;

private:
    const qdb_handle_t _handle;
    const char * const _data;
    const size_t _length;
};

typedef std::shared_ptr<api_buffer> api_buffer_ptr;

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

    qdb_error_t connect(const char * host, unsigned short port);
    size_t multi_connect(const char * const * hosts, const unsigned short * ports, qdb_error_t * errors, size_t count);

    qdb_error_t put(const char * alias, const char * content, size_t content_length);
    qdb_error_t update(const char * alias, const char * content, size_t content_length);
    qdb_error_t get(const char * alias, char * content, size_t * content_length);
    api_buffer_ptr get(const char * alias, qdb_error_t & error);
    api_buffer_ptr get_update(const char * alias, const char * update_content, size_t update_content_length, qdb_error_t & error);
    api_buffer_ptr compare_and_swap(const char * alias,
        const char * new_value,
        size_t new_value_length,
        const char * comparand,
        size_t comparand_length,
        qdb_error_t & error);

    qdb_error_t remove(const char * alias);
    qdb_error_t remove_all(void);

private:
    qdb_handle_t _handle;

};

typedef std::shared_ptr<handle> handle_ptr;

}

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