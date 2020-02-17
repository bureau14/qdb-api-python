/*
 *
 * Official Python API
 *
 * Copyright (c) 2009-2020, quasardb SAS. All rights reserved.
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
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

namespace py = pybind11;

namespace qdb
{

class find_query
{
public:
    find_query(qdb::handle_ptr h, const std::string & query_string)
        : _handle{h}
        , _query_string{query_string}
    {}

public:
    std::vector<std::string> run()
    {
        const char ** aliases = nullptr;
        size_t count          = 0;

        qdb::qdb_throw_if_error(*_handle, qdb_query_find(*_handle, _query_string.c_str(), &aliases, &count));

        return convert_strings_and_release(_handle, aliases, count);
    }

private:
    qdb::handle_ptr _handle;
    std::string _query_string;
};

typedef std::vector<std::map<std::string, py::handle>> dict_query_result_t;
dict_query_result_t dict_query(qdb::handle_ptr h, std::string const & query, const py::object & blobs);

template <typename Module>
static inline void register_query(Module & m)
{
    namespace py = pybind11;

    py::class_<qdb::find_query>{m, "FindQuery"}                //
        .def(py::init<qdb::handle_ptr, const std::string &>()) //
        .def("run", &qdb::find_query::run);                    //

    m.def("dict_query", &qdb::dict_query);
}

} // namespace qdb
