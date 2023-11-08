#include "handle.hpp"
#include "metrics.hpp"

namespace qdb
{

void handle::connect(const std::string & uri)
{
    qdb_error_t err;
    {
        metrics::scoped_capture{"qdb_connect"};
        err = qdb_connect(handle_, uri.c_str());
    }
    qdb::qdb_throw_if_error(handle_, err);
}

void handle::close()
{
    if (handle_ != nullptr)
    {
        metrics::scoped_capture{"qdb_close"};
        qdb_close(handle_);
        handle_ = nullptr;
    }

    assert(handle_ == nullptr);
}

}; // namespace qdb
