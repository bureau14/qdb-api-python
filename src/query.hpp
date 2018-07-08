#pragma once

#include "handle.hpp"
#include "utils.hpp"
#include <qdb/query.h>
#include <pybind11/numpy.h>
#include <string>
#include <unordered_map>
#include <vector>

namespace qdb
{

class base_query
{
public:
    base_query(qdb::handle_ptr h, const std::string & query_string)
        : _handle{h}
        , _query_string{query_string}
    {}

protected:
    qdb::handle_ptr _handle;
    std::string _query_string;
};

class find_query : public base_query
{
public:
    find_query(qdb::handle_ptr h, const std::string & query_string)
        : base_query{h, query_string}
    {}

public:
    std::vector<std::string> run()
    {
        const char ** aliases = nullptr;
        size_t count          = 0;

        QDB_THROW_IF_ERROR(qdb_query_find(*_handle, _query_string.c_str(), &aliases, &count));

        return convert_strings_and_release(_handle, aliases, count);
    }
};

class query : public base_query
{
public:
    query(qdb::handle_ptr h, std::string query_string)
        : base_query{h, query_string}
    {}

public:
    struct column_result
    {
        std::string name;
        pybind11::array data;
    };

    using table_result = std::vector<column_result>;

    struct query_result
    {
        qdb_size_t scanned_rows_count{0};
        std::unordered_map<std::string, table_result> tables;
    };

public:
    // return a list of numpy arrays
    query_result run();
};

template <typename Module>
static inline void register_query(Module & m)
{
    namespace py = pybind11;

    py::class_<qdb::base_query>{m, "BaseQuery"}                 //
        .def(py::init<qdb::handle_ptr, const std::string &>()); //

    py::class_<qdb::find_query, qdb::base_query>{m, "FindQuery"} //
        .def(py::init<qdb::handle_ptr, const std::string &>())   //
        .def("run", &qdb::find_query::run);                      //

    py::class_<qdb::query, qdb::base_query> q{m, "Query"}; //

    py::class_<qdb::query::column_result>{q, "ColumnResult"}    //
        .def_readonly("name", &qdb::query::column_result::name) //
        .def_readonly("data", &qdb::query::column_result::data);

    py::class_<qdb::query::query_result>{q, "QueryResult"}                                 //
        .def_readonly("scanned_rows_count", &qdb::query::query_result::scanned_rows_count) //
        .def_readonly("tables", &qdb::query::query_result::tables);

    q.def(py::init<qdb::handle_ptr, const std::string &>()) //
        .def("run", &qdb::query::run);                      //
}

} // namespace qdb
