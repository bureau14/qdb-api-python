#include "mock_failure.hpp"
#include <module.hpp>

namespace qdb::detail
{

QDB_REGISTER_MODULE(mock_failure_options, m)
{
    py::class_<mock_failure_options>{m, "MockFailureOptions"}                 //
                                                                              //
        .def(py::init<std::size_t>(),                                         //
            py::arg("failures") = std::size_t{0}                              //
            )                                                                 //
                                                                              //
        .def_readwrite("failures_left", &mock_failure_options::failures_left) //
                                                                              //
        .def("has_next", &mock_failure_options::has_next)                     //
        .def("next", &mock_failure_options::next)                             //
        ;
}

}; // namespace qdb::detail
