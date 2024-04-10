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
#include <pybind11/stl.h>
#include <string>

namespace qdb
{
namespace py = pybind11;

class properties
{
public:
    explicit properties(qdb::handle_ptr h)
        : handle_{h}
    {}

public:
    /**
     * Returns value of property with key `key`, or None if not found.
     */
    std::optional<std::string> get(std::string const & key);

    /**
     * Sets the value of `key` to `value`. If the `key` already exists, an error will
     * be thrown.
     */
    void put(std::string const & key, std::string const & value);

    /**
     * Removes a single property.
     */
    void remove(std::string const & key);

    /**
     * Clears all previously set properties.
     */
    void clear();

private:
    qdb::handle_ptr handle_;
};

static inline void register_properties(py::module_ & m)
{
    py::class_<qdb::properties> p(m, "Properties"); //

    p.def(py::init<qdb::handle_ptr>()) //
        .def("get", &qdb::properties::get)
        .def("put", &qdb::properties::put)
        .def("remove", &qdb::properties::remove)
        .def("clear", &qdb::properties::clear);
}

} // namespace qdb
