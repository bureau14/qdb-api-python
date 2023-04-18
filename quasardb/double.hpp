/*
 *
 * Official Python API
 *
 * Copyright (c) 2009-2023, quasardb SAS. All rights reserved.
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
#include <qdb/double.h>

namespace qdb
{

class double_entry : public expirable_entry
{
public:
    double_entry(handle_ptr h, std::string a) noexcept
        : expirable_entry{h, a}
    {}

public:
    double get()
    {
        double result;
        qdb::qdb_throw_if_error(*_handle, qdb_double_get(*_handle, _alias.c_str(), &result));
        return result;
    }

    void put(double val)
    {
        qdb::qdb_throw_if_error(*_handle, qdb_double_put(*_handle, _alias.c_str(), val, qdb_time_t{0}));
    }

    void update(double val)
    {
        qdb::qdb_throw_if_error(
            *_handle, qdb_double_update(*_handle, _alias.c_str(), val, qdb_time_t{0}));
    }

    double add(double val)
    {
        double result;
        qdb::qdb_throw_if_error(*_handle, qdb_double_add(*_handle, _alias.c_str(), val, &result));
        return result;
    }
};

template <typename Module>
static inline void register_double(Module & m)
{
    namespace py = pybind11;

    py::class_<qdb::double_entry, qdb::expirable_entry>(m, "Double")  //
        .def(py::init<qdb::handle_ptr, std::string>())                //
        .def("get", &qdb::double_entry::get)                          //
        .def("put", &qdb::double_entry::put, py::arg("double"))       //
        .def("update", &qdb::double_entry::update, py::arg("double")) //
        .def("add", &qdb::double_entry::add, py::arg("addend"))       //
        ;                                                             //
}

} // namespace qdb
