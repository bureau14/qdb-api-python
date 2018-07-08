#include "cluster.hpp"
#include <pybind11/pybind11.h>

namespace py = pybind11;

PYBIND11_MODULE(quasardb, m)
{
    try
    {
        py::module::import("numpy");
    }
    catch (...)
    {
        return;
    }

    py::register_exception<qdb::exception>(m, "Error");

    m.doc() = "QuasarDB Official Python API";

    m.def("version", &qdb_version, "Return version number");
    m.def("build", &qdb_build, "Return build number");

    m.attr("never_expires") = std::chrono::system_clock::time_point{};

    qdb::register_cluster(m);
    qdb::register_options(m);
    qdb::register_entry(m);
    qdb::register_blob(m);
    qdb::register_tag(m);
    qdb::register_query(m);
    qdb::register_ts(m);
    qdb::register_ts_batch(m);
}
