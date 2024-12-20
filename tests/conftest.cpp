#include "conftest.hpp"
#include <module.hpp>

namespace qdb
{

QDB_REGISTER_MODULE(conftest, m)
{
    auto exc_assertion_error =
        py::register_exception<assertion_error>(m, "AssertionError", PyExc_AssertionError);
    auto exc_assertion_error_check =
        py::register_exception<assertion_error_check>(m, "AssertionErrorCheck", exc_assertion_error);
    auto exc_assertion_error_check_equal = py::register_exception<assertion_error_check_equal>(
        m, "AssertionErrorCheckEqual", exc_assertion_error_check);
    auto exc_assertion_error_check_not_equal = py::register_exception<assertion_error_check_not_equal>(
        m, "AssertionErrorCheckNotEqual", exc_assertion_error_check);
    auto exc_assertion_error_check_gte = py::register_exception<assertion_error_check_gte>(
        m, "AssertionErrorCheckGTE", exc_assertion_error_check);
}

}; // namespace qdb
