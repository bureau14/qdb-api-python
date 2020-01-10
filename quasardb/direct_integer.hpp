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

#include "direct_handle.hpp"
#include <qdb/integer.h>

namespace qdb
{

class direct_integer_entry
{
public:
    direct_integer_entry(direct_handle_ptr dh, std::string a) noexcept
        : _direct_handle{dh}
        , _alias{a}
    {}

public:
    qdb_int_t get()
    {
        qdb_int_t result;
        qdb::qdb_throw_if_error(qdb_direct_int_get(*_direct_handle, _alias.c_str(), &result));
        return result;
    }

private:
    direct_handle_ptr _direct_handle;
    std::string _alias;
};

template <typename Module>
static inline void register_direct_integer(Module & m)
{
    namespace py = pybind11;

    py::class_<qdb::direct_integer_entry>(m, "DirectInteger")
        .def(py::init<qdb::direct_handle_ptr, std::string>())
        .def("get", &qdb::direct_integer_entry::get);                                                                                                                            //
}

} // namespace qdb
