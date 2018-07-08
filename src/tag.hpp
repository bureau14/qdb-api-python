#pragma once

#include "entry.hpp"

namespace qdb
{

class tag : public entry
{
public:
    tag(qdb::handle_ptr h, const std::string & alias)
        : entry{h, alias}
    {}

public:
    std::vector<std::string> get_entries()
    {
        const char ** aliases = nullptr;
        size_t count          = 0;

        QDB_THROW_IF_ERROR(qdb_get_tagged(*_handle, _alias.c_str(), &aliases, &count));

        return convert_strings_and_release(_handle, aliases, count);
    }

    qdb_uint_t count()
    {
        qdb_uint_t count = 0;

        QDB_THROW_IF_ERROR(qdb_get_tagged_count(*_handle, _alias.c_str(), &count));

        return count;
    }
};

template <typename Module>
static inline void register_tag(Module & m)
{
    namespace py = pybind11;

    py::class_<qdb::tag, qdb::entry>(m, "Tag")         //
        .def(py::init<qdb::handle_ptr, std::string>()) //
        .def("get_entries", &qdb::tag::get_entries)    //
        .def("count", &qdb::tag::count);               //
}

} // namespace qdb
