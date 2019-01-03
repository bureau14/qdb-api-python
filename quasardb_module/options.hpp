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
#include <qdb/option.h>
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
    void set_timeout(std::chrono::milliseconds ms)
    {
        qdb::qdb_throw_if_error(qdb_option_set_timeout(*_handle, static_cast<int>(ms.count())));
    }

    std::chrono::milliseconds get_timeout()
    {
        int ms = 0;

        qdb::qdb_throw_if_error(qdb_option_get_timeout(*_handle, &ms));

        return std::chrono::milliseconds{ms};
    }

    void set_stabilization_max_wait(std::chrono::milliseconds ms)
    {
        qdb::qdb_throw_if_error(qdb_option_set_stabilization_max_wait(*_handle, static_cast<int>(ms.count())));
    }

    std::chrono::milliseconds get_stabilization_max_wait()
    {
        int ms = 0;

        qdb::qdb_throw_if_error(qdb_option_get_stabilization_max_wait(*_handle, &ms));

        return std::chrono::milliseconds{ms};
    }

    void set_max_cardinality(qdb_uint_t cardinality)
    {
        qdb::qdb_throw_if_error(qdb_option_set_max_cardinality(*_handle, cardinality));
    }

    void set_compression(qdb_compression_t level)
    {
        qdb::qdb_throw_if_error(qdb_option_set_compression(*_handle, level));
    }

    void set_encryption(qdb_encryption_t algo)
    {
        qdb::qdb_throw_if_error(qdb_option_set_encryption(*_handle, algo));
    }

    void set_cluster_public_key(const std::string & key)
    {
        qdb::qdb_throw_if_error(qdb_option_set_cluster_public_key(*_handle, key.c_str()));
    }

    void set_user_credentials(const std::string & user, const std::string & private_key)
    {
        qdb::qdb_throw_if_error(qdb_option_set_user_credentials(*_handle, user.c_str(), private_key.c_str()));
    }

    void set_client_max_in_buf_size(size_t max_size)
    {
        qdb::qdb_throw_if_error(qdb_option_set_client_max_in_buf_size(*_handle, max_size));
    }

    size_t get_client_max_in_buf_size()
    {
        size_t buf_size = 0;
        qdb::qdb_throw_if_error(qdb_option_get_client_max_in_buf_size(*_handle, &buf_size));
        return buf_size;
    }

    size_t get_cluster_max_in_buf_size()
    {
        size_t buf_size = 0;
        qdb::qdb_throw_if_error(qdb_option_get_cluster_max_in_buf_size(*_handle, &buf_size));
        return buf_size;
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
    py::enum_<qdb_compression_t>{o, "Compression", py::arithmetic(), "Compression type"} //
        .value("Disabled", qdb_comp_none)                                                //
        .value("Fast", qdb_comp_fast)                                                    //
        .value("Best", qdb_comp_best);                                                   //

    py::enum_<qdb_encryption_t>{o, "Encryption", py::arithmetic(), "Encryption type"} //
        .value("Disabled", qdb_crypt_none)                                            //
        .value("AES256GCM", qdb_crypt_aes_gcm_256);                                   //

    o.def(py::init<qdb::handle_ptr>())                                                    //
        .def("set_timeout", &qdb::options::set_timeout)                                   //
        .def("get_timeout", &qdb::options::get_timeout)                                   //
        .def("set_stabilization_max_wait", &qdb::options::set_stabilization_max_wait)     //
        .def("get_stabilization_max_wait", &qdb::options::get_stabilization_max_wait)     //
        .def("set_max_cardinality", &qdb::options::set_max_cardinality)                   //
        .def("set_compression", &qdb::options::set_compression)                           //
        .def("set_encryption", &qdb::options::set_encryption)                             //
        .def("set_cluster_public_key", &qdb::options::set_cluster_public_key)             //
        .def("set_user_credentials", &qdb::options::set_user_credentials)                 //
        .def("set_client_max_in_buf_size", &qdb::options::set_client_max_in_buf_size)     //
        .def("get_client_max_in_buf_size", &qdb::options::get_client_max_in_buf_size)     //
        .def("get_cluster_max_in_buf_size", &qdb::options::get_cluster_max_in_buf_size);  //
}

} // namespace qdb
