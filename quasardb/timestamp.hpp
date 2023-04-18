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
#include "pytypes.hpp"
#include <qdb/timestamp.h>
#include "convert/value.hpp"
#include <chrono>

namespace qdb
{

class timestamp_entry : public expirable_entry
{
    using clock_t = std::chrono::system_clock;

public:
    timestamp_entry(handle_ptr h, std::string a) noexcept
        : expirable_entry{h, a}
    {}

public:
    qdb::pydatetime get()
    {
        qdb_timespec_t result;
        qdb::qdb_throw_if_error(*_handle, qdb_timestamp_get(*_handle, _alias.c_str(), &result));
        return qdb::convert::value<qdb_timespec_t, qdb::pydatetime>(result);
    }

    void put(qdb::pydatetime val)
    {
        qdb_timespec_t val_ = qdb::convert::value<qdb::pydatetime, qdb_timespec_t>(val);
        qdb::qdb_throw_if_error(
            *_handle, qdb_timestamp_put(*_handle, _alias.c_str(), &val_, qdb_time_t{0}));
    }

    void update(qdb::pydatetime val)
    {
        qdb_timespec_t val_ = qdb::convert::value<qdb::pydatetime, qdb_timespec_t>(val);
        qdb::qdb_throw_if_error(
            *_handle, qdb_timestamp_update(*_handle, _alias.c_str(), &val_, qdb_time_t{0}));
    }

    qdb::pydatetime add(qdb::pydatetime val)
    {
        qdb_timespec_t val_ = qdb::convert::value<qdb::pydatetime, qdb_timespec_t>(val);
        qdb_timespec_t result;
        qdb::qdb_throw_if_error(*_handle, qdb_timestamp_add(*_handle, _alias.c_str(), &val_, &result));

        return qdb::convert::value<qdb_timespec_t, qdb::pydatetime>(result);
    }
};

template <typename Module>
static inline void register_timestamp(Module & m)
{
    namespace py = pybind11;

    py::class_<qdb::timestamp_entry, qdb::expirable_entry>(m, "Timestamp")  //
        .def(py::init<qdb::handle_ptr, std::string>())                      //
        .def("get", &qdb::timestamp_entry::get)                             //
        .def("put", &qdb::timestamp_entry::put, py::arg("timestamp"))       //
        .def("update", &qdb::timestamp_entry::update, py::arg("timestamp")) //
        .def("add", &qdb::timestamp_entry::add, py::arg("addend"))          //
        ;                                                                   //
}

} // namespace qdb
