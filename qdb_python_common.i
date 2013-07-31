%module(package="qdb") qdb
%{

#include <qdb/client.hpp>

%}

%include "std_string.i"
%include "std_vector.i"
%include <std_shared_ptr.i>
%shared_ptr(qdb::api_buffer)
%shared_ptr(qdb::handle)

%template(StringVec) std::vector<std::string>;

%rename("%(regex:/qdb_e(.*)/error\\1/)s", %$isenumitem) "";
%rename("%(regex:/qdb_o(.*)/option\\1/)s", %$isenumitem) "";
%rename("%(strip:[qdb_])s", %$isfunction) "";

%include "../qdb_enum.i"
%include "../qdb_struct.i"
%include "../qdb_iterator.i"

%include typemaps.i
%include cpointer.i

%apply (const char *STRING, size_t LENGTH) { (const char * content, size_t content_length) };
%apply (const char *STRING, size_t LENGTH) { (const char * update_content, size_t update_content_length) };
%apply (const char *STRING, size_t LENGTH) { (const char * new_value, size_t new_value_length) };
%apply (const char *STRING, size_t LENGTH) { (const char * comparand, size_t comparand_length) };

%typemap(in) qdb_time_t { $1 = PyLong_AsSsize_t($input); }

%typemap(out) qdb_time_t { $result = PyLong_FromSsize_t($1); }

%rename(version) qdb_version;
const char * qdb_version();

%rename(build) qdb_build;
const char * qdb_build();

%rename(version) qdb_version;
const char * qdb_version();

%rename(build) qdb_build;
const char * qdb_build();

namespace qdb
{
std::string make_error_string(qdb_error_t error);
}

