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

namespace qdb
{

class tag : public entry
{
public:
    tag(qdb::handle_ptr h, const std::string & alias)
        : entry{h, alias}
    {}

public:
    std::vector<std::string> get_entries()
    {
        const char ** aliases = nullptr;
        size_t count          = 0;

        qdb::qdb_throw_if_error(qdb_get_tagged(*_handle, _alias.c_str(), &aliases, &count));

        return convert_strings_and_release(_handle, aliases, count);
    }

    qdb_uint_t count()
    {
        qdb_uint_t count = 0;

        qdb::qdb_throw_if_error(qdb_get_tagged_count(*_handle, _alias.c_str(), &count));

        return count;
    }
};

template <typename Module>
static inline void register_tag(Module & m)
{
    namespace py = pybind11;

    py::class_<qdb::tag, qdb::entry>(m, "Tag")         //
        .def(py::init<qdb::handle_ptr, std::string>()) //
        .def("get_entries", &qdb::tag::get_entries)    //
        .def("count", &qdb::tag::count);               //
}

} // namespace qdb
