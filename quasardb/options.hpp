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

#include "handle.hpp"
#include <qdb/option.h>
#include "detail/qdb_resource.hpp"
#include <chrono>

namespace qdb
{

class options
{
public:
    explicit options(qdb::handle_ptr h)
        : _handle{h}
    {}

public:
    /**
     * Applies credentials if provided, throws exception when credentials are invalid.
     */
    void apply_credentials(const std::string & user_name,
        const std::string & user_private_key,
        const std::string & cluster_public_key,
        const std::string & user_security_file,
        const std::string & cluster_public_key_file)
    {
        // must specify keys or files or nothing
        auto empty_keys  = user_name.empty() && user_private_key.empty() && cluster_public_key.empty();
        auto empty_files = user_security_file.empty() && cluster_public_key_file.empty();

        if (!empty_keys && !empty_files)
            throw qdb::exception{qdb_e_invalid_argument,
                "Either key or file security settings must be provided, or none at all"};

        if (!empty_keys)
        {
            if (user_name.empty() || user_private_key.empty() || cluster_public_key.empty())
                throw qdb::exception{qdb_e_invalid_argument,
                    "Either all keys security settings must be provided, or none at all"};

            set_user_credentials(user_name, user_private_key);
            set_cluster_public_key(cluster_public_key);
        }
        else if (!empty_files)
        {
            if (user_security_file.empty() || cluster_public_key_file.empty())
                throw qdb::exception{qdb_e_invalid_argument,
                    "Either all files security settings must be provided, or none at all"};

            set_file_credential(user_security_file, cluster_public_key_file);
        }
    };

    void set_timezone(std::string const & tz)
    {
        qdb::qdb_throw_if_error(*_handle, qdb_option_set_timezone(*_handle, tz.c_str()));
    }

    std::string get_timezone()
    {
        detail::qdb_resource<char const> tz{*_handle};
        qdb::qdb_throw_if_error(*_handle, qdb_option_get_timezone(*_handle, &tz));

        std::string ret{tz.get()};

        return ret;
    }

    void set_timeout(std::chrono::milliseconds ms)
    {
        qdb::qdb_throw_if_error(
            *_handle, qdb_option_set_timeout(*_handle, static_cast<int>(ms.count())));
    }

    std::chrono::milliseconds get_timeout()
    {
        int ms = 0;

        qdb::qdb_throw_if_error(*_handle, qdb_option_get_timeout(*_handle, &ms));

        return std::chrono::milliseconds{ms};
    }

    void enable_user_properties()
    {
        qdb::qdb_throw_if_error(*_handle, qdb_option_enable_user_properties(*_handle));
    }

    void disable_user_properties()
    {
        qdb::qdb_throw_if_error(*_handle, qdb_option_disable_user_properties(*_handle));
    }

    void set_client_soft_memory_limit(std::size_t limit)
    {
        qdb::qdb_throw_if_error(*_handle,
            qdb_option_set_client_soft_memory_limit(*_handle, static_cast<qdb_uint_t>(limit)));
    }

    void set_stabilization_max_wait(std::chrono::milliseconds ms)
    {
        qdb::qdb_throw_if_error(
            *_handle, qdb_option_set_stabilization_max_wait(*_handle, static_cast<int>(ms.count())));
    }

    std::chrono::milliseconds get_stabilization_max_wait()
    {
        int ms = 0;

        qdb::qdb_throw_if_error(*_handle, qdb_option_get_stabilization_max_wait(*_handle, &ms));

        return std::chrono::milliseconds{ms};
    }

    void set_client_max_batch_load(std::size_t shard_count)
    {
        qdb::qdb_throw_if_error(*_handle,
            qdb_option_set_client_max_batch_load(*_handle, static_cast<qdb_size_t>(shard_count)));
    }

    qdb_size_t get_client_max_batch_load()
    {
        qdb_size_t shard_count{0};
        qdb::qdb_throw_if_error(*_handle, qdb_option_get_client_max_batch_load(*_handle, &shard_count));
        return shard_count;
    }

    void set_connection_per_address_soft_limit(std::size_t max_count)
    {
        qdb::qdb_throw_if_error(*_handle, qdb_option_set_connection_per_address_soft_limit(
                                              *_handle, static_cast<qdb_size_t>(max_count)));
    }

    qdb_size_t get_connection_per_address_soft_limit()
    {
        qdb_size_t max_count{0};
        qdb::qdb_throw_if_error(
            *_handle, qdb_option_get_connection_per_address_soft_limit(*_handle, &max_count));
        return max_count;
    }

    void set_max_cardinality(qdb_uint_t cardinality)
    {
        qdb::qdb_throw_if_error(*_handle, qdb_option_set_max_cardinality(*_handle, cardinality));
    }

    void set_compression(qdb_compression_t level)
    {
        qdb::qdb_throw_if_error(*_handle, qdb_option_set_compression(*_handle, level));
    }

    void set_encryption(qdb_encryption_t algo)
    {
        qdb::qdb_throw_if_error(*_handle, qdb_option_set_encryption(*_handle, algo));
    }

    void set_cluster_public_key(const std::string & key)
    {
        qdb::qdb_throw_if_error(*_handle, qdb_option_set_cluster_public_key(*_handle, key.c_str()));
    }

    void set_user_credentials(const std::string & user, const std::string & private_key)
    {
        qdb::qdb_throw_if_error(
            *_handle, qdb_option_set_user_credentials(*_handle, user.c_str(), private_key.c_str()));
    }

    void set_file_credential(
        const std::string & user_security_file, const std::string & cluster_public_key_file)
    {
        qdb::qdb_throw_if_error(
            *_handle, qdb_option_load_security_files(
                          *_handle, cluster_public_key_file.c_str(), user_security_file.c_str()));
    }

    void set_client_max_in_buf_size(size_t max_size)
    {
        qdb::qdb_throw_if_error(*_handle, qdb_option_set_client_max_in_buf_size(*_handle, max_size));
    }

    size_t get_client_max_in_buf_size()
    {
        size_t buf_size = 0;
        qdb::qdb_throw_if_error(*_handle, qdb_option_get_client_max_in_buf_size(*_handle, &buf_size));
        return buf_size;
    }

    size_t get_cluster_max_in_buf_size()
    {
        size_t buf_size = 0;
        qdb::qdb_throw_if_error(*_handle, qdb_option_get_cluster_max_in_buf_size(*_handle, &buf_size));
        return buf_size;
    }

    void set_client_max_parallelism(size_t max_parallelism)
    {
        qdb::qdb_throw_if_error(
            *_handle, qdb_option_set_client_max_parallelism(*_handle, max_parallelism));
    }

    size_t get_client_max_parallelism()
    {
        size_t max_parallelism = 0;
        qdb::qdb_throw_if_error(
            *_handle, qdb_option_get_client_max_parallelism(*_handle, &max_parallelism));
        return max_parallelism;
    }

    void set_query_max_length(size_t query_max_length)
    {
        qdb::qdb_throw_if_error(*_handle, qdb_option_set_query_max_length(*_handle, query_max_length));
    }

    size_t get_query_max_length()
    {
        size_t query_max_length = 0;
        qdb::qdb_throw_if_error(*_handle, qdb_option_get_query_max_length(*_handle, &query_max_length));
        return query_max_length;
    }

private:
    qdb::handle_ptr _handle;
};

template <typename Module>
static inline void register_options(Module & m)
{
    namespace py = pybind11;

    py::class_<qdb::options> o(m, "Options"); //

    // None is reserved keyword in Python
    py::enum_<qdb_compression_t>{o, "Compression", py::arithmetic(), "Compression mode"} //
        .value("Disabled", qdb_comp_none)                                                //
        .value("Best", qdb_comp_best)                                                    //
        .value("Balanced", qdb_comp_balanced);                                           //

    py::enum_<qdb_encryption_t>{o, "Encryption", py::arithmetic(), "Encryption type"}    //
        .value("Disabled", qdb_crypt_none)                                               //
        .value("AES256GCM", qdb_crypt_aes_gcm_256);                                      //

    o.def(py::init<qdb::handle_ptr>())                                                    //
        .def("set_timeout", &qdb::options::set_timeout)                                   //
        .def("get_timeout", &qdb::options::get_timeout)                                   //
        .def("set_timezone", &qdb::options::set_timezone)                                 //
        .def("get_timezone", &qdb::options::get_timezone)                                 //
        .def("enable_user_properties", &qdb::options::enable_user_properties)             //
        .def("disable_user_properties", &qdb::options::disable_user_properties)           //
        .def("set_stabilization_max_wait", &qdb::options::set_stabilization_max_wait)     //
        .def("get_stabilization_max_wait", &qdb::options::get_stabilization_max_wait)     //
        .def("set_max_cardinality", &qdb::options::set_max_cardinality)                   //
        .def("set_encryption", &qdb::options::set_encryption)                             //
        .def("set_cluster_public_key", &qdb::options::set_cluster_public_key)             //
        .def("set_user_credentials", &qdb::options::set_user_credentials)                 //
        .def("set_client_max_in_buf_size", &qdb::options::set_client_max_in_buf_size)     //
        .def("get_client_max_in_buf_size", &qdb::options::get_client_max_in_buf_size)     //
        .def("set_client_max_batch_load", &qdb::options::set_client_max_batch_load,       //
            "Adjust the number of shards per thread used for the batch writer.")          //
        .def("get_client_max_batch_load", &qdb::options::get_client_max_batch_load,       //
            "Get the number of shards per thread used for the batch writer.")             //
        .def("set_connection_per_address_soft_limit",                                     //
            &qdb::options::set_connection_per_address_soft_limit,                         //
            "Adjust the maximum number of connections per qdbd node")                     //
        .def("get_connection_per_address_soft_limit",                                     //
            &qdb::options::get_connection_per_address_soft_limit,                         //
            "Get the maximum number of connections per qdbd node")                        //
        .def("get_cluster_max_in_buf_size", &qdb::options::get_cluster_max_in_buf_size)   //
        .def("get_client_max_parallelism", &qdb::options::get_client_max_parallelism)     //
        .def("set_query_max_length", &qdb::options::set_query_max_length,                 //
            py::arg("query_max_length"))                                                  //
        .def("get_query_max_length", &qdb::options::get_query_max_length)                 //
        .def("set_client_soft_memory_limit", &qdb::options::set_client_soft_memory_limit, //
            py::arg("limit"))                                                             //
        ;
}

} // namespace qdb
