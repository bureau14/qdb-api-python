#pragma once

#include "error.hpp"
#include <qdb/client.h>
#include <memory>
#include <string>

namespace qdb
{

class handle
{
public:
    handle() noexcept
    {}

    explicit handle(qdb_handle_t h) noexcept
        : _handle{h}
    {}

    ~handle()
    {
        if (_handle)
        {
            qdb_close(_handle);
            _handle = nullptr;
        }
    }

    void connect(const std::string & uri)
    {
        QDB_THROW_IF_ERROR(qdb_connect(_handle, uri.c_str()));
    }

    operator qdb_handle_t() const noexcept
    {
        return _handle;
    }

private:
    qdb_handle_t _handle{nullptr};
};

using handle_ptr = std::shared_ptr<handle>;

static inline handle_ptr make_handle_ptr()
{
    return std::make_shared<qdb::handle>(qdb_open_tcp());
}

} // namespace qdb
