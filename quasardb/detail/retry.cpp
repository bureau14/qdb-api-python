#include "retry.hpp"
#include <pybind11/chrono.h>

namespace qdb::detail
{

void register_retry_options(py::module_ & m)
{
    namespace py = pybind11;

    py::class_<retry_options>{m, "RetryOptions"}                                      //
        .def(py::init<std::size_t, std::chrono::milliseconds, std::size_t, double>(), //
            py::arg("retries") = std::size_t{3},                                      //
            py::kw_only(),                                                            //
            py::arg("delay")    = std::chrono::milliseconds{3000},                    //
            py::arg("exponent") = std::size_t{2},                                     //
            py::arg("jitter")   = double{0.1}                                         //
            )                                                                         //
                                                                                      //
        .def_readwrite("retries_left", &retry_options::retries_left)                  //
        .def_readwrite("delay", &retry_options::delay)                                //
        .def_readwrite("exponent", &retry_options::exponent)                          //
        .def_readwrite("jitter", &retry_options::jitter)                              //
                                                                                      //
        .def("has_next", &retry_options::has_next)                                    //
        .def("next", &retry_options::next)                                            //
        ;
}

}; // namespace qdb::detail
