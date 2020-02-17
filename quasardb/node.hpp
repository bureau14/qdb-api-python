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

#include "direct_blob.hpp"
#include "direct_handle.hpp"
#include "direct_integer.hpp"
#include "utils.hpp"
#include <iostream>
#include <memory>

namespace qdb
{

class node
{
public:
    node(const handle_ptr h, const std::string & node_uri)
        : _uri{node_uri}
        , _handle{h}
        , _direct_handle{make_direct_handle_ptr()}
    {
        _direct_handle->connect(_handle, node_uri);
    }
    ~node()
    {}

    void close()
    {
        _handle.reset();
        _direct_handle.reset();
    }

    std::vector<std::string> prefix_get(const std::string & prefix, qdb_int_t max_count)
    {
        const char ** result = nullptr;
        size_t count         = 0;

        // don't throw if no prefix is found
        const qdb_error_t err = qdb_direct_prefix_get(*_direct_handle, prefix.c_str(), max_count, &result, &count);
        qdb_throw_if_error(*_handle, err);

        return convert_strings_and_release(_handle, result, count);
    }

    qdb::direct_blob_entry blob(const std::string & alias)
    {
        return qdb::direct_blob_entry{_handle, _direct_handle, alias};
    }

    qdb::direct_integer_entry integer(const std::string & alias)
    {
        return qdb::direct_integer_entry{_handle, _direct_handle, alias};
    }

private:
    std::string _uri;
    handle_ptr _handle;
    direct_handle_ptr _direct_handle;
};

template <typename Module>
static inline void register_node(Module & m)
{
    namespace py = pybind11;

    py::class_<qdb::node>(m, "Node") //
                                     // no constructor: use quasardb.Cluster(uri).node(node_uri) to initialise
        .def("prefix_get", &qdb::node::prefix_get)
        .def("blob", &qdb::node::blob)
        .def("integer", &qdb::node::integer);
}

} // namespace qdb
