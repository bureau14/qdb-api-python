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

#include "error.hpp"
#include "handle.hpp"
#include "direct_handle.hpp"
#include <qdb/direct.h>

namespace qdb
{

class direct_blob_entry
{
public:
    direct_blob_entry(handle_ptr h, direct_handle_ptr dh, std::string a) noexcept
        : _handle{h}
        , _direct_handle{dh}
        , _alias{a}
    {}

    pybind11::bytes get()
    {
        const void * content      = nullptr;
        qdb_size_t content_length = 0;

        qdb::qdb_throw_if_error(qdb_direct_blob_get(*_direct_handle, _alias.c_str(), &content, &content_length));

        return convert_and_release_content(content, content_length);
    }
private:
    pybind11::bytes convert_and_release_content(const void * content, qdb_size_t content_length)
    {
        if (!content || !content_length) return pybind11::bytes{};

        pybind11::bytes res(static_cast<const char *>(content), content_length);

        qdb_release(*_handle, content);

        return res;
    }

    handle_ptr _handle;
    direct_handle_ptr _direct_handle;
    std::string _alias;
};

template <typename Module>
static inline void register_direct_blob(Module & m)
{
    namespace py = pybind11;

    py::class_<qdb::direct_blob_entry>(m, "DirectBlob")
        .def(py::init<qdb::handle_ptr, qdb::direct_handle_ptr, std::string>())
        .def("get", &qdb::direct_blob_entry::get);
}

} // namespace qdb
