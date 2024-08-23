#include "module.hpp"
#include "cluster.hpp"
#include "metrics.hpp"
#include "node.hpp"
#include "reader.hpp"
#include "writer.hpp"
#include <functional>
#include <list>

#ifndef QDB_TESTS_ENABLED
#    include "detail/sleep.hpp"
#    include "detail/writer.hpp"
using push_strategy_t  = qdb::detail::default_writer_push_strategy;
using sleep_strategy_t = qdb::detail::default_sleep_strategy<>;
#else
#    include "../tests/detail/sleep.hpp"
#    include "../tests/detail/writer.hpp"
using push_strategy_t  = qdb::detail::mock_failure_writer_push_strategy;
using sleep_strategy_t = qdb::detail::mock_sleep_strategy<>;
#endif

namespace qdb
{

/**
 * This approach is inspired from pybind11, where we keep a global static variable
 * of initializers for components/modules that which to register with our global
 * quasardb module.
 *
 * It has the downside of module initialization order essentially being random, which
 * is fine for our use case. It allows decoupling of code in many places.
 */
std::list<std::function<void(py::module_ &)>> & initializers()
{
    static std::list<std::function<void(py::module_ &)>> inits;
    return inits;
}

submodule_initializer::submodule_initializer(initialize_fn init)
{
    initializers().emplace_back(init);
}

submodule_initializer::submodule_initializer(const char * submodule_name, initialize_fn init)
{
    initializers().emplace_back([=](py::module_ & parent) { init(parent); });
}

}; // namespace qdb

PYBIND11_MODULE(quasardb, m)
{
    m.doc() = "QuasarDB Official Python API";
    m.def("version", &qdb_version, "Return version number");
    m.def("build", &qdb_build, "Return build number");
    m.attr("never_expires") = std::chrono::system_clock::time_point{};

    qdb::register_errors(m);
    qdb::register_cluster(m);
    qdb::register_node(m);
    qdb::register_options(m);
    qdb::register_properties(m);
    qdb::register_perf(m);
    qdb::register_entry(m);
    qdb::register_double(m);
    qdb::register_integer(m);
    qdb::register_blob(m);
    qdb::register_string(m);
    qdb::register_timestamp(m);
    qdb::register_direct_blob(m);
    qdb::register_direct_integer(m);
    qdb::register_tag(m);
    qdb::register_query(m);
    qdb::register_continuous(m);
    qdb::register_table(m);
    qdb::register_batch_column(m);
    qdb::register_batch_inserter(m);
    qdb::register_masked_array(m);
    qdb::register_reader(m);

    qdb::register_writer<push_strategy_t, sleep_strategy_t>(m);

    qdb::register_metrics(m);

    qdb::detail::register_ts_column(m);
    qdb::detail::register_retry_options(m);

    for (const auto & initializer : qdb::initializers())
    {
        initializer(m);
    }
}
