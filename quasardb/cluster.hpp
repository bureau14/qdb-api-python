/*
 *
 * Official Python API
 *
 * Copyright (c) 2009-2024, quasardb SAS. All rights reserved.
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

#include "batch_inserter.hpp"
#include "blob.hpp"
#include "continuous.hpp"
#include "double.hpp"
#include "error.hpp"
#include "handle.hpp"
#include "integer.hpp"
#include "logger.hpp"
#include "node.hpp"
#include "options.hpp"
#include "perf.hpp"
#include "query.hpp"
#include "string.hpp"
#include "table.hpp"
#include "table_reader.hpp"
#include "tag.hpp"
#include "timestamp.hpp"
#include "utils.hpp"
#include "writer.hpp"
#include <qdb/node.h>
#include <qdb/prefix.h>
#include <qdb/suffix.h>
#include "detail/qdb_resource.hpp"
#include <pybind11/chrono.h>
#include <pybind11/operators.h>
#include <pybind11/stl.h>
#include <chrono>
#include <iostream>

namespace qdb
{

class cluster
{
public:
    cluster(const std::string & uri,
        const std::string & user_name               = {},
        const std::string & user_private_key        = {},
        const std::string & cluster_public_key      = {},
        const std::string & user_security_file      = {},
        const std::string & cluster_public_key_file = {},
        std::chrono::milliseconds timeout           = std::chrono::minutes{1},
        bool do_version_check                       = false);

public:
    void close();

    bool is_open() const
    {
        return _handle.get() != nullptr && _handle->is_open();
    }

    /**
     * Throws exception if the connection is not open. Should be invoked before any operation
     * is done on the handle, as the QuasarDB C API only checks for a canary presence in the
     * handle's memory arena. If a compiler is optimizing enough, the handle can be closed but
     * the canary still present in memory, so it's UB.
     *
     * As such, we should check on a higher level.
     */
    void check_open() const
    {
        if (is_open() == false) [[unlikely]]
        {
            throw qdb::invalid_handle_exception{};
        }
    }

    void tidy_memory()
    {
        if (_handle)
        {
            _logger.info("Tidying memory");
            qdb_option_client_tidy_memory(*_handle);
        }
    }

    std::string get_memory_info()
    {
        std::string result;
        if (_handle)
        {
            char const * buf;
            qdb_size_t n;
            qdb_option_client_get_memory_info(*_handle, &buf, &n);

            result = std::string{buf, n};

            qdb_release(*_handle, buf);
        }

        return result;
    }

public:
    qdb::node node(const std::string & uri)
    {
        return qdb::node(uri, _handle);
    }

private:
    pybind11::object convert_to_json_and_release(const char * content)
    {
        const auto result = _json_loads(content);
        qdb_release(*_handle, content);
        return result;
    }

public:
    cluster enter()
    {
        // No-op, all initialization is done in the constructor.
        return *this;
    }

    void exit(pybind11::object type, pybind11::object value, pybind11::object traceback)
    {
        return close();
    }

    pybind11::object node_config(const std::string & uri)
    {
        check_open();

        const char * content      = nullptr;
        qdb_size_t content_length = 0;

        qdb::qdb_throw_if_error(
            *_handle, qdb_node_config(*_handle, uri.c_str(), &content, &content_length));

        return convert_to_json_and_release(content);
    }

    pybind11::object node_status(const std::string & uri)
    {
        check_open();

        const char * content      = nullptr;
        qdb_size_t content_length = 0;

        qdb::qdb_throw_if_error(
            *_handle, qdb_node_status(*_handle, uri.c_str(), &content, &content_length));

        return convert_to_json_and_release(content);
    }

    pybind11::object node_topology(const std::string & uri)
    {
        check_open();

        const char * content      = nullptr;
        qdb_size_t content_length = 0;

        qdb::qdb_throw_if_error(
            *_handle, qdb_node_topology(*_handle, uri.c_str(), &content, &content_length));

        return convert_to_json_and_release(content);
    }

public:
    qdb::tag tag(const std::string & alias)
    {
        check_open();

        return qdb::tag{_handle, alias};
    }

    qdb::blob_entry blob(const std::string & alias)
    {
        check_open();

        return qdb::blob_entry{_handle, alias};
    }

    qdb::string_entry string(const std::string & alias)
    {
        check_open();

        return qdb::string_entry{_handle, alias};
    }

    qdb::integer_entry integer(const std::string & alias)
    {
        check_open();

        return qdb::integer_entry{_handle, alias};
    }

    qdb::double_entry double_(const std::string & alias)
    {
        check_open();

        return qdb::double_entry{_handle, alias};
    }

    qdb::timestamp_entry timestamp(const std::string & alias)
    {
        check_open();

        return qdb::timestamp_entry{_handle, alias};
    }

    qdb::table table(const std::string & alias)
    {
        check_open();

        return qdb::table{_handle, alias};
    }

    // the batch_inserter_ptr is non-copyable
    qdb::batch_inserter_ptr inserter(const std::vector<batch_column_info> & ci)
    {
        check_open();

        return std::make_unique<qdb::batch_inserter>(_handle, ci);
    }

    // the batch_inserter_ptr is non-copyable
    qdb::writer_ptr writer()
    {
        check_open();

        return std::make_unique<qdb::writer>(_handle);
    }

    // the batch_inserter_ptr is non-copyable
    qdb::writer_ptr pinned_writer()
    {
        check_open();

        return writer();
    }

    qdb::options options()
    {
        check_open();

        return qdb::options{_handle};
    }

    qdb::perf perf()
    {
        check_open();

        return qdb::perf{_handle};
    }

    const std::string & uri()
    {
        return _uri;
    }

public:
    std::vector<std::string> prefix_get(const std::string & prefix, qdb_int_t max_count)
    {
        check_open();

        const char ** result = nullptr;
        size_t count         = 0;

        const qdb_error_t err = qdb_prefix_get(*_handle, prefix.c_str(), max_count, &result, &count);
        // don't throw if no prefix is found
        if (err != qdb_e_alias_not_found)
        {
            qdb_throw_if_error(*_handle, err);
        }

        return convert_strings_and_release(_handle, result, count);
    }

    qdb_uint_t prefix_count(const std::string & prefix)
    {
        check_open();

        qdb_uint_t count = 0;

        const qdb_error_t err = qdb_prefix_count(*_handle, prefix.c_str(), &count);
        qdb_throw_if_error(*_handle, err);

        return count;
    }

public:
    qdb::find_query find(const std::string & query_string)
    {
        check_open();

        return qdb::find_query{_handle, query_string};
    }

    py::object query(const std::string & query_string, const py::object & blobs)
    {
        check_open();

        return py::cast(qdb::dict_query(_handle, query_string, blobs));
    }

    py::object query_numpy(const std::string & query_string)
    {
        check_open();

        return py::cast(qdb::numpy_query(_handle, query_string));
    }

    std::shared_ptr<qdb::query_continuous> query_continuous_full(
        const std::string & query_string, std::chrono::milliseconds pace, const py::object & blobs)
    {
        check_open();

        return std::make_shared<qdb::query_continuous>(
            _handle, qdb_query_continuous_full, pace, query_string, blobs);
    }

    std::shared_ptr<qdb::query_continuous> query_continuous_new_values(
        const std::string & query_string, std::chrono::milliseconds pace, const py::object & blobs)
    {
        check_open();

        return std::make_shared<qdb::query_continuous>(
            _handle, qdb_query_continuous_new_values_only, pace, query_string, blobs);
    }

public:
    std::vector<std::string> suffix_get(const std::string & suffix, qdb_int_t max_count)
    {
        check_open();

        const char ** result = nullptr;
        size_t count         = 0;

        const qdb_error_t err = qdb_suffix_get(*_handle, suffix.c_str(), max_count, &result, &count);
        // don't throw if no suffix is found
        if (err != qdb_e_alias_not_found)
        {
            qdb_throw_if_error(*_handle, err);
        }

        return convert_strings_and_release(_handle, result, count);
    }

    qdb_uint_t suffix_count(const std::string & suffix)
    {
        check_open();

        qdb_uint_t count = 0;

        const qdb_error_t err = qdb_suffix_count(*_handle, suffix.c_str(), &count);
        qdb_throw_if_error(*_handle, err);

        return count;
    }

public:
    void purge_all(std::chrono::milliseconds timeout_ms)
    {
        check_open();

        qdb::qdb_throw_if_error(
            *_handle, qdb_purge_all(*_handle, static_cast<int>(timeout_ms.count())));
    }

    void purge_cache(std::chrono::milliseconds timeout_ms)
    {
        check_open();

        qdb::qdb_throw_if_error(
            *_handle, qdb_purge_cache(*_handle, static_cast<int>(timeout_ms.count())));
    }

    void wait_for_stabilization(std::chrono::milliseconds timeout_ms)
    {
        check_open();

        qdb::qdb_throw_if_error(
            *_handle, qdb_wait_for_stabilization(*_handle, static_cast<int>(timeout_ms.count())));
    }

    void trim_all(std::chrono::milliseconds pause_ms, std::chrono::milliseconds timeout_ms)
    {
        check_open();

        qdb::qdb_throw_if_error(*_handle, qdb_trim_all(*_handle, static_cast<int>(pause_ms.count()),
                                              static_cast<int>(timeout_ms.count())));
    }

    void compact_full()
    {
        check_open();

        qdb_compact_params_t params{};
        params.options = qdb_compact_full;

        qdb::qdb_throw_if_error(*_handle, qdb_cluster_compact(*_handle, &params));
    }

    // Returns 0 when finished / no compaction running
    std::uint64_t compact_progress()
    {
        check_open();

        std::uint64_t progress;

        qdb::qdb_throw_if_error(*_handle, qdb_cluster_get_compact_progress(*_handle, &progress));

        return progress;
    }

    void compact_abort()
    {
        check_open();

        qdb::qdb_throw_if_error(*_handle, qdb_cluster_abort_compact(*_handle));
    }

    void wait_for_compaction();

public:
    std::vector<std::string> endpoints()
    {
        check_open();

        qdb_remote_node_t * endpoints = nullptr;
        qdb_size_t count              = 0;

        const qdb_error_t err = qdb_cluster_endpoints(*_handle, &endpoints, &count);
        qdb_throw_if_error(*_handle, err);

        std::vector<std::string> results;
        results.resize(count);

        std::transform(endpoints, endpoints + count, std::begin(results), [](auto const & endpoint) {
            return std::string{endpoint.address} + ":" + std::to_string(endpoint.port);
        });

        qdb_release(*_handle, endpoints);

        return results;
    }

private:
    std::string _uri;
    handle_ptr _handle;
    pybind11::object _json_loads;

    qdb::logger _logger;
};

template <typename Module>
static inline void register_cluster(Module & m)
{
    namespace py = pybind11;

    py::class_<qdb::cluster>(m, "Cluster",
        "Represents a connection to the QuasarDB cluster. ") //
        .def(
            py::init<const std::string &, const std::string &, const std::string &, const std::string &,
                const std::string &, const std::string &, std::chrono::milliseconds, bool>(), //
            py::arg("uri"),                                                                   //
            py::arg("user_name")               = std::string{},                               //
            py::arg("user_private_key")        = std::string{},                               //
            py::arg("cluster_public_key")      = std::string{},                               //
            py::arg("user_security_file")      = std::string{},                               //
            py::arg("cluster_public_key_file") = std::string{},                               //
            py::arg("timeout")                 = std::chrono::minutes{1},                     //
            py::arg("do_version_check")        = false)                                              //
        .def("__enter__", &qdb::cluster::enter)                                               //
        .def("__exit__", &qdb::cluster::exit)                                                 //
        .def("tidy_memory", &qdb::cluster::tidy_memory)                                       //
        .def("get_memory_info", &qdb::cluster::get_memory_info)                               //
        .def("is_open", &qdb::cluster::is_open)                                               //
        .def("uri", &qdb::cluster::uri)                                                       //
        .def("node", &qdb::cluster::node)                                                     //
        .def("options", &qdb::cluster::options)                                               //
        .def("perf", &qdb::cluster::perf)                                                     //
        .def("node_status", &qdb::cluster::node_status)                                       //
        .def("node_config", &qdb::cluster::node_config)                                       //
        .def("node_topology", &qdb::cluster::node_topology)                                   //
        .def("tag", &qdb::cluster::tag)                                                       //
        .def("blob", &qdb::cluster::blob)                                                     //
        .def("string", &qdb::cluster::string)                                                 //
        .def("integer", &qdb::cluster::integer)                                               //
        .def("double", &qdb::cluster::double_)                                                //
        .def("timestamp", &qdb::cluster::timestamp)                                           //
        .def("ts", &qdb::cluster::table)                                                      //
        .def("table", &qdb::cluster::table)                                                   //
        .def("ts_batch", &qdb::cluster::inserter)                                             //
        .def("inserter", &qdb::cluster::inserter)                                             //
        .def("pinned_writer", &qdb::cluster::pinned_writer)                                   //
        .def("writer", &qdb::cluster::writer)                                                 //
        .def("find", &qdb::cluster::find)                                                     //
        .def("query", &qdb::cluster::query,                                                   //
            py::arg("query"),                                                                 //
            py::arg("blobs") = false)                                                         //
        .def("query_numpy", &qdb::cluster::query_numpy,                                       //
            py::arg("query"))                                                                 //
        .def("query_continuous_full", &qdb::cluster::query_continuous_full,                   //
            py::arg("query"),                                                                 //
            py::arg("pace"),                                                                  //
            py::arg("blobs") = false)                                                         //
        .def("query_continuous_new_values", &qdb::cluster::query_continuous_new_values,       //
            py::arg("query"),                                                                 //
            py::arg("pace"),                                                                  //
            py::arg("blobs") = false)                                                         //
        .def("prefix_get", &qdb::cluster::prefix_get)                                         //
        .def("prefix_count", &qdb::cluster::prefix_count)                                     //
        .def("suffix_get", &qdb::cluster::suffix_get)                                         //
        .def("suffix_count", &qdb::cluster::suffix_count)                                     //
        .def("close", &qdb::cluster::close)                                                   //
        .def("purge_all", &qdb::cluster::purge_all)                                           //
        .def("trim_all", &qdb::cluster::trim_all)                                             //
        .def("purge_cache", &qdb::cluster::purge_cache)                                       //
        .def("compact_full", &qdb::cluster::compact_full)                                     //
        .def("compact_progress", &qdb::cluster::compact_progress)                             //
        .def("compact_abort", &qdb::cluster::compact_abort)                                   //
        .def("wait_for_compaction", &qdb::cluster::wait_for_compaction)                       //
        .def("endpoints", &qdb::cluster::endpoints);                                          //
}

} // namespace qdb
