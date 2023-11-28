#include "cluster.hpp"
#include "metrics.hpp"
#include <chrono>
#include <thread>

namespace qdb
{

cluster::cluster(const std::string & uri,
    const std::string & user_name,
    const std::string & user_private_key,
    const std::string & cluster_public_key,
    const std::string & user_security_file,
    const std::string & cluster_public_key_file,
    std::chrono::milliseconds timeout,
    bool do_version_check)
    : _uri{uri}
    , _handle{make_handle_ptr()}
    , _json_loads{pybind11::module::import("json").attr("loads")}
    , _logger("quasardb.cluster")
{
    if (do_version_check == true)
    {
        _logger.warn(
            "do_version_check parameter has been deprecated and a no-op. It will be removed from a "
            "future release");
    }

    options().apply_credentials(user_name, user_private_key, cluster_public_key, //
        user_security_file, cluster_public_key_file);

    options().set_timeout(timeout);

    // HACKS(leon): we need to ensure there is always one callback active
    //              for qdb. Callbacks can be lost when the last active session
    //              gets closed. As such, the most pragmatic place to 'check'
    //              for this callback is when establishing a new connection.
    qdb::native::swap_callback();

    _logger.info("Connecting to cluster %s", _uri);
    _handle->connect(_uri);
}

void cluster::close()
{
    _logger.info("Closing connection to cluster");

    try
    {
        if (is_open() == true) [[likely]]
        {
            _handle->close();
        }
    }
    catch (qdb::invalid_handle_exception const & e)
    {
        // This can happen if, for example, we call close() after an error occured; in those
        // circumstances, we fully expect the connection to already be invalid, and we should
        // not care if this specific exception is raised.
        _logger.warn("Connection already closed");
    }

    _handle.reset();

    assert(is_open() == false);
}

void cluster::wait_for_compaction()
{

    // We define this function in the .cpp file so we can avoid including chrono and thread
    // in the header file.

    using namespace std::chrono_literals;

    for (;;)
    {
        std::uint64_t progress = compact_progress();

        if (progress == 0) [[unlikely]]
        {
            break;
        }

        std::this_thread::sleep_for(100ms);
    }
}

}; // namespace qdb
