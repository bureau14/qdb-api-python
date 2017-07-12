%module(package="quasardb") qdb
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
#include <qdb/log.h>
#include <qdb/query.h>
#include <qdb/ts.h>
%}

%include "std_string.i"
%include "std_vector.i"

%include <std_shared_ptr.i>

%shared_ptr(qdb::api_buffer)
%shared_ptr(qdb::handle)

%rename("%(regex:/qdb_e_(.*)/error_\\1/)s", %$isenumitem) "";
%rename("%(regex:/qdb_o_(.*)/option_\\1/)s", %$isenumitem) "";
%rename("%(regex:/qdb_p_(.*)/protocol_\\1/)s", %$isenumitem) "";
%rename("%(regex:/qdb_op_(.*)/operation_\\1/)s", %$isenumitem) "";
%rename("%(regex:/qdb_agg_(.*)/aggregation_\\1/)s", %$isenumitem) "";
%rename("%(regex:/qdb_ts_column_(.*)/column_\\1/)s", %$isenumitem) "";
%rename("%(strip:[qdb_])s", %$isfunction) "";

%typemap(in) qdb_time_t { $1 = PyLong_AsLongLong($input); }
%typemap(out) qdb_time_t { $result = PyLong_FromLongLong($1); }

%typemap(in) qdb_int_t { $1 = PyLong_AsLongLong($input); }
%typemap(out) qdb_int_t { $result = PyLong_FromLongLong($1); }

%typemap(in) qdb_uint_t { $1 = PyLong_AsUnsignedLongLong($input); }
%typemap(out) qdb_uint_t { $result = PyLong_FromUnsignedLongLong($1); }

%include typemaps.i
%include cpointer.i

%apply size_t { qdb_size_t };
%apply (const char *STRING, size_t LENGTH) { (const char * content, size_t content_length) };
%apply (const char *STRING, size_t LENGTH) { (const char * update_content, size_t update_content_length) };
%apply (const char *STRING, size_t LENGTH) { (const char * new_value, size_t new_value_length) };
%apply (const char *STRING, size_t LENGTH) { (const char * comparand, size_t comparand_length) };
%apply (const char *STRING, size_t LENGTH) { (const void * pattern, qdb_size_t pattern_length) };

%include "qdb_struct.i"
%include "qdb_enum.i"

%template(StringVec) std::vector<std::string>;
%template(RangeVec) std::vector<qdb_ts_range_t>;
%template(DoublePointVec) std::vector<qdb_ts_double_point>;
%template(BlobPointVec) std::vector<wrap_ts_blob_point>;
%template(BlobAggVec) std::vector<qdb_ts_blob_aggregation_t>;
%template(DoubleAggVec) std::vector<qdb_ts_double_aggregation_t>;
%template(TSColsVec) std::vector<wrap_ts_column>;

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
