%include "qdb_python_common.i"

%include "qdb_api_buffer.i"

namespace qdb
{
typedef std::shared_ptr<api_buffer> api_buffer_ptr;
}

%include "qdb_handle.i"

namespace qdb
{
typedef std::shared_ptr<handle> handle_ptr;
}


%include "qdb_python_adapters.i"

