%module(package="qdb") qdb
#define SwigPyIterator qdbSwigPyIterator
#define SwigPyIterator_T qdbSwigPyIterator_T
#define SwigPyIteratorOpen_T qdbSwigPyIteratorOpen_T
#define SwigPyIteratorClosed_T qdbSwigPyIteratorClosed_T
#define SwigPySequence_Ref qdbSwigPySequence_Ref
#define SwigPySequence_ArrowProxy qdbSwigPySequence_ArrowProxy
#define SwigPySequence_InputIterator qdbSwigPySequence_InputIterator
#define SwigPySequence_Cont qdbSwigPySequence_Cont
%{

#include <qdb/client.hpp>

%}

%include "std_string.i"
%include "std_vector.i"

%include <std_shared_ptr.i>

%shared_ptr(qdb::api_buffer)
%shared_ptr(qdb::handle)

%template(StringVec) std::vector<std::string>;
%template(BatchReqVec) std::vector<qdb::batch_request>;
%template(BatchResVec) std::vector<qdb::batch_result>;

%rename("%(regex:/qdb_e_(.*)/error_\\1/)s", %$isenumitem) "";
%rename("%(regex:/qdb_o_(.*)/option_\\1/)s", %$isenumitem) "";
%rename("%(regex:/qdb_p_(.*)/protocol_\\1/)s", %$isenumitem) "";
%rename("%(regex:/qdb_op_(.*)/operation_\\1/)s", %$isenumitem) "";
%rename("%(strip:[qdb_])s", %$isfunction) "";

%typemap(in) qdb_time_t { $1 = PyLong_AsSsize_t($input); }
%typemap(out) qdb_time_t { $result = PyLong_FromSsize_t($1); }

%include typemaps.i
%include cpointer.i

%apply (const char *STRING, size_t LENGTH) { (const char * content, size_t content_length) };
%apply (const char *STRING, size_t LENGTH) { (const char * update_content, size_t update_content_length) };
%apply (const char *STRING, size_t LENGTH) { (const char * new_value, size_t new_value_length) };
%apply (const char *STRING, size_t LENGTH) { (const char * comparand, size_t comparand_length) };

%include "../qdb_enum.i"
%include "../qdb_struct.i"
%include "../qdb_iterator.i"

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

