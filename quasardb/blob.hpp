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

#include "entry.hpp"
#include <qdb/blob.h>

namespace qdb
{

class blob_entry : public expirable_entry
{
public:
    blob_entry(handle_ptr h, std::string a) noexcept
        : expirable_entry{h, a}
    {}

private:
    pybind11::bytes convert_and_release_content(const void * content, qdb_size_t content_length)
    {
        if (!content || !content_length) return pybind11::bytes{};

        std::string res(static_cast<const char *>(content), content_length);

        qdb_release(*_handle, content);

        return pybind11::bytes(res);
    }

public:
    pybind11::bytes get()
    {
        const void * content      = nullptr;
        qdb_size_t content_length = 0;

        qdb::qdb_throw_if_error(qdb_blob_get(*_handle, _alias.c_str(), &content, &content_length));

        return convert_and_release_content(content, content_length);
    }

    void put(const std::string & data, std::chrono::system_clock::time_point expiry = std::chrono::system_clock::time_point{})
    {
        qdb::qdb_throw_if_error(qdb_blob_put(*_handle, _alias.c_str(), data.data(), data.size(), expirable_entry::from_time_point(expiry)));
    }

    void update(const std::string & data, std::chrono::system_clock::time_point expiry = std::chrono::system_clock::time_point{})
    {
        qdb::qdb_throw_if_error(qdb_blob_update(*_handle, _alias.c_str(), data.data(), data.size(), expirable_entry::from_time_point(expiry)));
    }

    void remove_if(const std::string & comparand)
    {
        qdb::qdb_throw_if_error(qdb_blob_remove_if(*_handle, _alias.c_str(), comparand.data(), comparand.size()));
    }

    pybind11::bytes get_and_remove()
    {
        const void * content      = nullptr;
        qdb_size_t content_length = 0;

        qdb::qdb_throw_if_error(qdb_blob_get_and_remove(*_handle, _alias.c_str(), &content, &content_length));

        return convert_and_release_content(content, content_length);
    }

    pybind11::bytes get_and_update(
        const std::string & data, std::chrono::system_clock::time_point expiry = std::chrono::system_clock::time_point{})
    {
        const void * content      = nullptr;
        qdb_size_t content_length = 0;

        qdb::qdb_throw_if_error(qdb_blob_get_and_update(
            *_handle, _alias.c_str(), data.data(), data.size(), expirable_entry::from_time_point(expiry), &content, &content_length));

        return convert_and_release_content(content, content_length);
    }

    pybind11::bytes compare_and_swap(const std::string & new_value,
        const std::string & comparand,
        std::chrono::system_clock::time_point expiry = std::chrono::system_clock::time_point{})
    {
        const void * content      = nullptr;
        qdb_size_t content_length = 0;

        qdb_error_t err = qdb_blob_compare_and_swap(*_handle, _alias.c_str(), new_value.data(), new_value.size(), comparand.data(),
            comparand.size(), expirable_entry::from_time_point(expiry), &content, &content_length);

        // we don't want to throw on "unmatching content", so we don't use the qdb::qdb_throw_if_error function
        if (QDB_FAILURE(err)) throw qdb::exception{err};

        return convert_and_release_content(content, content_length);
    }
};

template <typename Module>
static inline void register_blob(Module & m)
{
    namespace py = pybind11;

    py::class_<qdb::blob_entry, qdb::expirable_entry>(m, "Blob")                                                               //
        .def(py::init<qdb::handle_ptr, std::string>())                                                                         //
        .def("get", &qdb::blob_entry::get)                                                                                     //
        .def("put", &qdb::blob_entry::put, py::arg("data"), py::arg("expiry") = std::chrono::system_clock::time_point{})       //
        .def("update", &qdb::blob_entry::update, py::arg("data"), py::arg("expiry") = std::chrono::system_clock::time_point{}) //
        .def("remove_if", &qdb::blob_entry::remove_if, py::arg("comparand"))                                                   //
        .def("get_and_remove", &qdb::blob_entry::get_and_remove)                                                               //
        .def("get_and_update", &qdb::blob_entry::get_and_update,                                                               //
            py::arg("data"), py::arg("expiry") = std::chrono::system_clock::time_point{})                                      //
        .def("compare_and_swap", &qdb::blob_entry::compare_and_swap,                                                           //
            py::arg("new_content"), py::arg("comparand"), py::arg("expiry") = std::chrono::system_clock::time_point{});        //
}

} // namespace qdb
