#include "cluster.hpp"
#include "metrics.hpp"
#include "table.hpp"
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
    bool do_version_check,
    bool enable_encryption,
    qdb_compression_t compression_mode,
    std::size_t client_max_parallelism)
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
    options().set_compression(compression_mode);

    if (client_max_parallelism != 0)
    {
        options().set_client_max_parallelism(client_max_parallelism);
    }

    if (enable_encryption == true)
    {
        options().set_encryption(qdb_crypt_aegis_256);
    }

    // Sets the default properties
    properties().clear();

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

qdb::table_ptr cluster::table(const std::string & alias)
{
    check_open();

    return qdb::make_table_ptr(_handle, alias);
}

void register_cluster(py::module_ & m)
{
    namespace py = pybind11;

    py::class_<qdb::cluster>(m, "Cluster",
        "Represents a connection to the QuasarDB cluster.")
        .def(py::init<const std::string &, const std::string &, const std::string &,
                 const std::string &, const std::string &, const std::string &,
                 std::chrono::milliseconds, bool, bool, qdb_compression_t, std::size_t>(),
            py::arg("uri"),
            py::arg("user_name")          = std::string{},
            py::arg("user_private_key")   = std::string{},
            py::arg("cluster_public_key") = std::string{},
            py::kw_only(),
            py::arg("user_security_file")      = std::string{},
            py::arg("cluster_public_key_file") = std::string{},
            py::arg("timeout")                 = std::chrono::minutes{1},
            py::arg("do_version_check")        = false,
            py::arg("enable_encryption")       = false,
            py::arg("compression_mode")        = qdb_comp_balanced,
            py::arg("client_max_parallelism")  = std::size_t{0}
            )
        .def("__enter__", &qdb::cluster::enter)
        .def("__exit__", &qdb::cluster::exit)
        .def("tidy_memory", &qdb::cluster::tidy_memory)
        .def("get_memory_info", &qdb::cluster::get_memory_info)
        .def("is_open", &qdb::cluster::is_open)
        .def("uri", &qdb::cluster::uri)
        .def("node", &qdb::cluster::node)
        .def("options", &qdb::cluster::options)
        .def("properties", &qdb::cluster::properties)
        .def("perf", &qdb::cluster::perf)
        .def("node_status", &qdb::cluster::node_status)
        .def("node_config", &qdb::cluster::node_config)
        .def("node_topology", &qdb::cluster::node_topology)
        .def("tag", &qdb::cluster::tag)
        .def("blob", &qdb::cluster::blob)
        .def("string", &qdb::cluster::string)
        .def("integer", &qdb::cluster::integer)
        .def("double", &qdb::cluster::double_)
        .def("timestamp", &qdb::cluster::timestamp)
        .def("ts", &qdb::cluster::table)
        .def("table", &qdb::cluster::table)
        .def("ts_batch", &qdb::cluster::inserter)
        .def("inserter", &qdb::cluster::inserter)
        .def("reader", &qdb::cluster::reader,
            py::arg("table_names"),
            py::kw_only(),
            py::arg("column_names") = std::vector<std::string>{},
            py::arg("batch_size")   = std::size_t{0},
            py::arg("ranges")       = std::vector<py::tuple>{}
            )
        .def("pinned_writer", &qdb::cluster::pinned_writer)
        .def("writer", &qdb::cluster::writer)
        .def("find", &qdb::cluster::find)
        .def("query", &qdb::cluster::query,
            py::arg("query"),
            py::arg("blobs") = false)
        .def("query_numpy", &qdb::cluster::query_numpy,
            py::arg("query"))
        .def("query_continuous_full", &qdb::cluster::query_continuous_full,
            py::arg("query"),
            py::arg("pace"),
            py::arg("blobs") = false)
        .def("query_continuous_new_values", &qdb::cluster::query_continuous_new_values,
            py::arg("query"),
            py::arg("pace"),
            py::arg("blobs") = false)
        .def("prefix_get", &qdb::cluster::prefix_get)
        .def("prefix_count", &qdb::cluster::prefix_count)
        .def("suffix_get", &qdb::cluster::suffix_get)
        .def("suffix_count", &qdb::cluster::suffix_count)
        .def("close", &qdb::cluster::close)
        .def("purge_all", &qdb::cluster::purge_all)
        .def("trim_all", &qdb::cluster::trim_all)
        .def("purge_cache", &qdb::cluster::purge_cache)
        .def("compact_full", &qdb::cluster::compact_full)
        .def("compact_progress", &qdb::cluster::compact_progress)
        .def("compact_abort", &qdb::cluster::compact_abort)
        .def("wait_for_compaction", &qdb::cluster::wait_for_compaction)
        .def("endpoints", &qdb::cluster::endpoints)
        .def("validate_query", &qdb::cluster::validate_query)
        .def("split_query_range", &qdb::cluster::split_query_range);
}

}; // namespace qdb
