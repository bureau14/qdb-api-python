#include "logger.hpp"

namespace py = pybind11;

static std::vector<qdb::native::message_t> _buffer;
static std::mutex _buffer_lock;
static qdb_log_callback_id local_callback_id;
static qdb::logger _logger;

void qdb::native::swap_callback()
{
    // Potential race condition avoidance!
    std::lock_guard<std::mutex> guard(_buffer_lock);
    qdb_error_t error;

    error = qdb_log_remove_callback(local_callback_id);
    if (error)
    {
        // This error is quite common the first time a callback is set, because we did
        // not have a callback yet.
    }

    error = qdb_log_add_callback(_callback, &local_callback_id);
    if (error)
    {
        // fprintf(stderr, "unable to add new callback: %s (%#x)\n", qdb_error(error), error);
        // fflush(stderr);
    }

    _logger = qdb::logger("quasardb.native");
}

void qdb::native::_callback(qdb_log_level_t log_level,
    const unsigned long * date,
    unsigned long pid,
    unsigned long tid,
    const char * message_buffer,
    size_t message_size)
{
    message_t x{log_level,
        {static_cast<int>(date[0]), static_cast<int>(date[1]), static_cast<int>(date[2]), static_cast<int>(date[3]),
            static_cast<int>(date[4]), static_cast<int>(date[5])},
        static_cast<int>(pid), static_cast<int>(tid), std::string(message_buffer, message_size)};

    std::lock_guard<std::mutex> guard(_buffer_lock);
    _buffer.push_back(x);
}

void qdb::native::flush()
{

    // TODO(leon): it would be much more appropriate to use a read/write lock here,
    //             since the buffer is typically empty.
    std::lock_guard<std::mutex> guard(_buffer_lock);

    // BUG(leon): I don't think it's practically possible, but there might be a case
    //            where our _buffer is somehow non-empty, but we haven't set our
    //            _logger yet. This will cause a segfault in _do_flush().
    if (!_buffer.empty())
    {
        qdb::native::_do_flush();
    }
}

void qdb::native::_do_flush()
{
    // NOTE: this assumes a lock to _buffer-lock has been acquired, never call this
    //       function directly!
    assert(!_buffer_lock.try_lock());

    // TODO(leon): we can improve native <> python vm context switching by folding all
    //             invocations below into a single call.
    for (auto i = _buffer.begin(); i != _buffer.end(); ++i)
    {
        message_t const & m = *i;

        switch (m.level)
        {
        case qdb_log_detailed:
        case qdb_log_debug:
            _logger.debug(m.message);
            break;

        case qdb_log_info:
            _logger.info(m.message);
            break;

        case qdb_log_warning:
            _logger.warn(m.message);
            break;

        case qdb_log_error:
            _logger.error(m.message);
            break;

        case qdb_log_panic:
            _logger.critical(m.message);
            break;
        }
    }

    _buffer.clear();
}
