#pragma once

#include <qdb/error.h>

namespace qdb
{

class exception
{
public:
    exception() noexcept
    {}

    explicit exception(qdb_error_t err) noexcept
        : _error{err}
    {}

    const char * what() const noexcept
    {
        return qdb_error(_error);
    }

private:
    qdb_error_t _error{qdb_e_ok};
};

#define QDB_THROW_IF_ERROR(x)                                                          \
    {                                                                                  \
        auto err = x;                                                                  \
        if ((err != qdb_e_ok) && (err != qdb_e_ok_created)) throw qdb::exception{err}; \
    }

} // namespace qdb
