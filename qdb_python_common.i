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

typedef struct qdb_local_table_internal * qdb_local_table_t;

%rename("%(regex:/qdb_d_(.*)/duration_\\1/)s",  %$isenumitem) "";
%rename("%(regex:/qdb_e_(.*)/error_\\1/)s", %$isenumitem) "";
%rename("%(regex:/qdb_o_(.*)/option_\\1/)s", %$isenumitem) "";
%rename("%(regex:/qdb_p_(.*)/protocol_\\1/)s", %$isenumitem) "";
%rename("%(regex:/qdb_op_(.*)/operation_\\1/)s", %$isenumitem) "";
%rename("%(regex:/qdb_agg_(.*)/aggregation_\\1/)s", %$isenumitem) "";
%rename("%(regex:/qdb_ts_column_(.*)/column_\\1/)s", %$isenumitem) "";
%rename("%(regex:/qdb_ts_filter_(.*)/filter_\\1/)s", %$isenumitem) "";
%rename("%(regex:/qdb_comp_(.*)/compression_\\1/)s",  %$isenumitem) "";
%rename("%(regex:/qdb_crypt_(.*)/encryption_\\1/)s",  %$isenumitem) "";
%rename("%(strip:[qdb_])s", %$isfunction) "";

%typemap(in) qdb_time_t { $1 = PyLong_AsLongLong($input); }
%typemap(out) qdb_time_t { $result = PyLong_FromLongLong($1); }

%include typemaps.i
%include cpointer.i

%apply long long { qdb_int_t };
%apply unsigned long long { qdb_uint_t };

%apply size_t { qdb_size_t };
%apply (const char *STRING, size_t LENGTH) { (const char * content, size_t content_length) };
%apply (const char *STRING, size_t LENGTH) { (const void * content, qdb_size_t content_length) };
%apply (const char *STRING, size_t LENGTH) { (const char * update_content, size_t update_content_length) };
%apply (const char *STRING, size_t LENGTH) { (const char * new_value, size_t new_value_length) };
%apply (const char *STRING, size_t LENGTH) { (const char * comparand, size_t comparand_length) };
%apply (const char *STRING, size_t LENGTH) { (const void * pattern, qdb_size_t pattern_length) };

%include "qdb_enum.i"
%include "qdb_struct.i"

%template(StringVec) std::vector<std::string>;
%template(RangeVec) std::vector<qdb_ts_range_t>;
%template(FilteredRangeVec) std::vector<qdb_ts_filtered_range_t>;
%template(DoublePointVec) std::vector<qdb_ts_double_point>;
%template(Int64PointVec) std::vector<qdb_ts_int64_point>;
%template(TimestampPointVec) std::vector<qdb_ts_timestamp_point>;
%template(BlobPointVec) std::vector<wrap_ts_blob_point>;
%template(Int64AggVec) std::vector<qdb_ts_int64_aggregation_t>;
%template(TimestampAggVec) std::vector<qdb_ts_timestamp_aggregation_t>;
%template(BlobAggVec) std::vector<qdb_ts_blob_aggregation_t>;
%template(DoubleAggVec) std::vector<qdb_ts_double_aggregation_t>;
%template(TSColsVec) std::vector<wrap_ts_column>;
%template(TableResultVec) std::vector<wrap_qdb_table_result_t>;
%template(QueryPointResultVec) std::vector<qdb_point_result_t>;
%template(QueryPointResultVecofVec) std::vector< std::vector<qdb_point_result_t> >;

%rename(version) qdb_version;
const char * qdb_version();

%rename(build) qdb_build;
const char * qdb_build();

%rename(version) qdb_version;
const char * qdb_version();

%rename(build) qdb_build;
const char * qdb_build();

qdb_error_t qdb_ts_row_set_double(qdb_local_table_t table,
                                  qdb_size_t col_index,
                                  double value);

qdb_error_t qdb_ts_row_set_blob(qdb_local_table_t table,
                                qdb_size_t col_index,
                                const void * content,
                                qdb_size_t content_length);

qdb_error_t qdb_ts_push(qdb_local_table_t table);

namespace qdb
{
std::string make_error_string(qdb_error_t error);
}
