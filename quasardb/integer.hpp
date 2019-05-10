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
#include <qdb/integer.h>

namespace qdb
{

class integer_entry : public expirable_entry
{
public:
    integer_entry(handle_ptr h, std::string a) noexcept
        : expirable_entry{h, a}
    {}

public:
    qdb_int_t get()
    {
        qdb_int_t result;
        qdb::qdb_throw_if_error(qdb_int_get(*_handle, _alias.c_str(), &result));
        return result;
    }

    void put(qdb_int_t integer, std::chrono::system_clock::time_point expiry = std::chrono::system_clock::time_point{})
    {
        qdb::qdb_throw_if_error(qdb_int_put(*_handle, _alias.c_str(), integer, expirable_entry::from_time_point(expiry)));
    }

    void update(qdb_int_t integer, std::chrono::system_clock::time_point expiry = std::chrono::system_clock::time_point{})
    {
        qdb::qdb_throw_if_error(qdb_int_update(*_handle, _alias.c_str(), integer, expirable_entry::from_time_point(expiry)));
    }

    qdb_int_t add(qdb_int_t integer)
    {
        qdb_int_t result;
        qdb::qdb_throw_if_error(qdb_int_add(*_handle, _alias.c_str(), integer, &result));
        return result;
    }
};

template <typename Module>
static inline void register_integer(Module & m)
{
    namespace py = pybind11;

    py::class_<qdb::integer_entry, qdb::expirable_entry>(m, "Integer")                                                               //
        .def(py::init<qdb::handle_ptr, std::string>())                                                                               //
        .def("get", &qdb::integer_entry::get)                                                                                        //
        .def("put", &qdb::integer_entry::put, py::arg("integer"), py::arg("expiry") = std::chrono::system_clock::time_point{})       //
        .def("update", &qdb::integer_entry::update, py::arg("integer"), py::arg("expiry") = std::chrono::system_clock::time_point{}) //
        .def("add", &qdb::integer_entry::add, py::arg("addend"))                                                                     //
        ;                                                                                                                            //
}

} // namespace qdb
