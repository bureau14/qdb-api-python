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

#include "blob.hpp"
#include "error.hpp"
#include "handle.hpp"
#include "integer.hpp"
#include "options.hpp"
#include "query.hpp"
#include "tag.hpp"
#include "ts.hpp"
#include "ts_batch.hpp"
#include "ts_reader.hpp"
#include "utils.hpp"
#include "version.hpp"
#include <qdb/node.h>
#include <qdb/prefix.h>
#include <qdb/suffix.h>
#include <pybind11/chrono.h>
#include <chrono>

namespace qdb
{

class cluster
{
public:
    cluster(const std::string & uri,
        const std::string & user_name          = {},
        const std::string & user_private_key   = {},
        const std::string & cluster_public_key = {},
        std::chrono::milliseconds timeout      = std::chrono::minutes{1})
        : _uri{uri}
        , _handle{make_handle_ptr()}
        , _json_loads{pybind11::module::import("json").attr("loads")}
    {
        // Check that the C API version installed on the target system matches
        // the one used during the build
        check_qdb_c_api_version(qdb_version());
        // must specify everything or nothing
        if (user_name.empty() != user_private_key.empty()) throw qdb::exception{qdb_e_invalid_argument};
        if (user_name.empty() != cluster_public_key.empty()) throw qdb::exception{qdb_e_invalid_argument};

        if (!user_name.empty())
        {
            options().set_user_credentials(user_name, user_private_key);
            options().set_cluster_public_key(cluster_public_key);
        }

        options().set_timeout(timeout);

        _handle->connect(_uri);
    }

public:
    void close()
    {
        _handle.reset();
    }

private:
    pybind11::object convert_to_json_and_release(const char * content)
    {
        const auto result = _json_loads(content);
        qdb_release(*_handle, content);
        return result;
    }

public:
    pybind11::object node_config(const std::string & uri)
    {
        const char * content      = nullptr;
        qdb_size_t content_length = 0;

        qdb::qdb_throw_if_error(qdb_node_config(*_handle, uri.c_str(), &content, &content_length));

        return convert_to_json_and_release(content);
    }

    pybind11::object node_status(const std::string & uri)
    {
        const char * content      = nullptr;
        qdb_size_t content_length = 0;

        qdb::qdb_throw_if_error(qdb_node_status(*_handle, uri.c_str(), &content, &content_length));

        return convert_to_json_and_release(content);
    }

    pybind11::object node_topology(const std::string & uri)
    {
        const char * content      = nullptr;
        qdb_size_t content_length = 0;

        qdb::qdb_throw_if_error(qdb_node_topology(*_handle, uri.c_str(), &content, &content_length));

        return convert_to_json_and_release(content);
    }

public:
    qdb::tag tag(const std::string & alias)
    {
        return qdb::tag{_handle, alias};
    }

    qdb::blob_entry blob(const std::string & alias)
    {
        return qdb::blob_entry{_handle, alias};
    }

    qdb::integer_entry integer(const std::string & alias)
    {
        return qdb::integer_entry{_handle, alias};
    }

    qdb::ts ts(const std::string & alias)
    {
        return qdb::ts{_handle, alias};
    }

    // the ts_batch_ptr is non-copyable
    qdb::ts_batch_ptr ts_batch(const std::vector<batch_column_info> & ci)
    {
        return std::make_unique<qdb::ts_batch>(_handle, ci);
    }

    qdb::options options()
    {
        return qdb::options{_handle};
    }

public:
    std::vector<std::string> prefix_get(const std::string & prefix, qdb_int_t max_count)
    {
        const char ** result = nullptr;
        size_t count         = 0;

        // don't throw if no prefix is found
        const qdb_error_t err = qdb_prefix_get(*_handle, prefix.c_str(), max_count, &result, &count);
        if (QDB_FAILURE(err) && (err != qdb_e_alias_not_found)) throw qdb::exception{err};

        return convert_strings_and_release(_handle, result, count);
    }

    qdb_uint_t prefix_count(const std::string & prefix)
    {
        qdb_uint_t count = 0;

        const qdb_error_t err = qdb_prefix_count(*_handle, prefix.c_str(), &count);
        if (QDB_FAILURE(err) && (err != qdb_e_alias_not_found)) throw qdb::exception{err};

        return count;
    }

public:
    qdb::find_query find(const std::string & query_string)
    {
        return qdb::find_query{_handle, query_string};
    }

    qdb::query query(const std::string & query_string)
    {
        return qdb::query{_handle, query_string};
    }

public:
    std::vector<std::string> suffix_get(const std::string & suffix, qdb_int_t max_count)
    {
        const char ** result = nullptr;
        size_t count         = 0;

        const qdb_error_t err = qdb_suffix_get(*_handle, suffix.c_str(), max_count, &result, &count);
        if (QDB_FAILURE(err) && (err != qdb_e_alias_not_found)) throw qdb::exception{err};

        return convert_strings_and_release(_handle, result, count);
    }

    qdb_uint_t suffix_count(const std::string & suffix)
    {
        qdb_uint_t count = 0;

        const qdb_error_t err = qdb_suffix_count(*_handle, suffix.c_str(), &count);
        if (QDB_FAILURE(err) && (err != qdb_e_alias_not_found)) throw qdb::exception{err};

        return count;
    }

public:
    void purge_all(std::chrono::milliseconds timeout_ms)
    {
        qdb::qdb_throw_if_error(qdb_purge_all(*_handle, static_cast<int>(timeout_ms.count())));
    }

    void purge_cache(std::chrono::milliseconds timeout_ms)
    {
        qdb::qdb_throw_if_error(qdb_purge_cache(*_handle, static_cast<int>(timeout_ms.count())));
    }

    void wait_for_stabilization(std::chrono::milliseconds timeout_ms)
    {
        qdb::qdb_throw_if_error(qdb_wait_for_stabilization(*_handle, static_cast<int>(timeout_ms.count())));
    }

    void trim_all(std::chrono::milliseconds timeout_ms)
    {
        qdb::qdb_throw_if_error(qdb_trim_all(*_handle, static_cast<int>(timeout_ms.count())));
    }

private:
    std::string _uri;
    handle_ptr _handle;
    pybind11::object _json_loads;
};

template <typename Module>
static inline void register_cluster(Module & m)
{
    namespace py = pybind11;

    py::class_<qdb::cluster>(m, "Cluster")                                                                                              //
        .def(py::init<const std::string &, const std::string &, const std::string &, const std::string &, std::chrono::milliseconds>(), //
            py::arg("uri"),                                                                                                             //
            py::arg("user_name")          = std::string{},                                                                              //
            py::arg("user_private_key")   = std::string{},                                                                              //
            py::arg("cluster_public_key") = std::string{},                                                                              //
            py::arg("timeout")            = std::chrono::minutes{1})                                                                    //
        .def("options", &qdb::cluster::options)                                                                                         //
        .def("node_status", &qdb::cluster::node_status)                                                                                 //
        .def("node_config", &qdb::cluster::node_config)                                                                                 //
        .def("node_topology", &qdb::cluster::node_topology)                                                                             //
        .def("tag", &qdb::cluster::tag)                                                                                                 //
        .def("blob", &qdb::cluster::blob)                                                                                               //
        .def("integer", &qdb::cluster::integer)                                                                                         //
        .def("ts", &qdb::cluster::ts)                                                                                                   //
        .def("ts_batch", &qdb::cluster::ts_batch)                                                                                       //
        .def("find", &qdb::cluster::find)                                                                                               //
        .def("query", &qdb::cluster::query)                                                                                             //
        .def("prefix_get", &qdb::cluster::prefix_get)                                                                                   //
        .def("prefix_count", &qdb::cluster::prefix_count)                                                                               //
        .def("suffix_get", &qdb::cluster::suffix_get)                                                                                   //
        .def("suffix_count", &qdb::cluster::suffix_count)                                                                               //
        .def("close", &qdb::cluster::close)                                                                                             //
        .def("purge_all", &qdb::cluster::purge_all)                                                                                     //
        .def("trim_all", &qdb::cluster::trim_all)                                                                                       //
        .def("purge_cache", &qdb::cluster::purge_cache);                                                                                //
}

} // namespace qdb
