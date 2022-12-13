#pragma once

#include <pybind11/pybind11.h>

namespace qdb
{

namespace py = pybind11;

class submodule_initializer
{
    using initialize_fn = void (*)(py::module_ &);

public:
    explicit submodule_initializer(initialize_fn init);
    submodule_initializer(const char * submodule_name, initialize_fn init);
};

}; // namespace qdb

#define QDB_REGISTER_MODULE(name, variable)                  \
    void qdb_submodule_##name(py::module_ &);                \
    submodule_initializer name(#name, qdb_submodule_##name); \
    void qdb_submodule_##name(py::module_ &(variable))
