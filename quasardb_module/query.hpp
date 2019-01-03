/*
 *
 * Official Python API
 *
 * Copyright (c) 2009-2019, quasardb SAS
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *    * Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above copyright
 *      notice, this list of conditions and the following disclaimer in the
 *      documentation and/or other materials provided with the distribution.
 *    * Neither the name of quasardb nor the names of its contributors may
 *      be used to endorse or promote products derived from this software
 *      without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY QUASARDB AND CONTRIBUTORS ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE REGENTS AND CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
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

        qdb::qdb_throw_if_error(qdb_query_find(*_handle, _query_string.c_str(), &aliases, &count));

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
        qdb_size_t scanned_point_count{0};
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

    py::class_<qdb::query::query_result>{q, "QueryResult"}                                   //
        .def_readonly("scanned_point_count", &qdb::query::query_result::scanned_point_count) //
        .def_readonly("tables", &qdb::query::query_result::tables);

    q.def(py::init<qdb::handle_ptr, const std::string &>()) //
        .def("run", &qdb::query::run);                      //
}

} // namespace qdb
