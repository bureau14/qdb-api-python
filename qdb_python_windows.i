// On Windows we compile the Python API with Visual Studio 2008, we need the TR1
#define SWIG_SHARED_PTR_SUBNAMESPACE tr1

%include "qdb_python_common.i"

%include "qdb_api_buffer.i"

namespace qdb
{
typedef std::tr1::shared_ptr<api_buffer> api_buffer_ptr;
}

%include "qdb_handle.i"

namespace qdb
{
typedef std::tr1::shared_ptr<handle> handle_ptr;
}


%include "qdb_python_adapters.i"
