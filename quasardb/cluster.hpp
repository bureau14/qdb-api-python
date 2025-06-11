/*
 *
 * Official Python API
 *
 * Copyright (c) 2009-2021, quasardb SAS. All rights reserved.
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
#include "properties.hpp"
#include "query.hpp"
#include "reader.hpp"
#include "string.hpp"
#include "table_fwd.hpp"
#include "tag.hpp"
#include "timestamp.hpp"
#include "utils.hpp"
#include "writer.hpp"
#include <qdb/node.h>
#include <qdb/option.h>
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
        bool do_version_check                       = false,
        bool enable_encryption                      = false,
        qdb_compression_t compression_mode          = qdb_comp_balanced,
        std::size_t client_max_parallelism          = 0);

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

    qdb::table_ptr table(const std::string & alias);

    // the reader_ptr is non-copyable
    qdb::reader_ptr reader(                            //
        std::vector<std::string> const & table_names,  //
        std::vector<std::string> const & column_names, //
        std::size_t batch_size,                        //
        std::vector<py::tuple> const & ranges)         //
    {
        check_open();

        return make_reader_ptr(_handle, table_names, column_names, batch_size, ranges);
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

    qdb::properties properties()
    {
        check_open();

        return qdb::properties{_handle};
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
    std::vector<std::string> find(const std::string & query_string)
    {
        check_open();

	auto o = std::make_shared<qdb::find_query>(_handle, query_string);
	return o->run();
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

    std::shared_ptr<qdb::query_continuous> query_continuous(qdb_query_continuous_mode_type_t mode,
        const std::string & query_string,
        std::chrono::milliseconds pace,
        const py::object & blobs)
    {
        check_open();

        auto o = std::make_shared<qdb::query_continuous>(_handle, blobs);

        o->run(mode, pace, query_string);

        return o;
    }

    std::shared_ptr<qdb::query_continuous> query_continuous_full(
        const std::string & query_string, std::chrono::milliseconds pace, const py::object & blobs)
    {
        return query_continuous(qdb_query_continuous_full, query_string, pace, blobs);
    }

    std::shared_ptr<qdb::query_continuous> query_continuous_new_values(
        const std::string & query_string, std::chrono::milliseconds pace, const py::object & blobs)
    {
        return query_continuous(qdb_query_continuous_new_values_only, query_string, pace, blobs);
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

    py::object validate_query(const std::string & query_string)
    {
        check_open();

        std::string query = query_string;
        const std::string limit_string = "LIMIT 1";
        query += " " + limit_string;
      
        // TODO:
        // should return dict of column names and dtypes
        // currently returns numpy masked arrays
        return py::cast(qdb::numpy_query(_handle, query));
    }

    py::object split_query_range(std::chrono::system_clock::time_point start, std::chrono::system_clock::time_point end, std::chrono::milliseconds delta)
    {
        std::vector<std::pair<std::chrono::system_clock::time_point, std::chrono::system_clock::time_point>> ranges;

        for (auto current_start = start; current_start < end; ) {
            auto current_end = current_start + delta;
            if (current_end > end) {
                current_end = end;
            }
            ranges.emplace_back(current_start, current_end);
            current_start = current_end;
        }
        return py::cast(ranges);
    }


private:
    std::string _uri;
    handle_ptr _handle;
    pybind11::object _json_loads;

    qdb::logger _logger;
};

void register_cluster(py::module_ & m);

} // namespace qdb
