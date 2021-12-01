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

#include "table.hpp"
#include <qdb/ts.h>
#include <pybind11/pybind11.h>
#include <string>

namespace qdb
{
struct batch_column_info
{
    batch_column_info() = default;
    batch_column_info(const std::string & ts_name, const std::string & col_name, qdb_size_t size_hint = 0)
        : timeseries{ts_name}
        , column{col_name}
        , elements_count_hint{size_hint}
    {}

    operator qdb_ts_batch_column_info_t() const noexcept
    {
        qdb_ts_batch_column_info_t res;

        res.timeseries          = timeseries.c_str();
        res.column              = column.c_str();
        res.elements_count_hint = elements_count_hint;
        return res;
    }

    std::string timeseries;
    std::string column;
    qdb_size_t elements_count_hint{0};
};

template <typename Module>
static inline void register_batch_column(Module & m)
{
    namespace py = pybind11;

    py::class_<qdb::batch_column_info>{m, "BatchColumnInfo"}                                 //
        .def(py::init<const std::string &, const std::string &, qdb_size_t>(),               //
            py::arg("ts_name"),                                                              //
            py::arg("col_name"),                                                             //
            py::arg("size_hint") = 0)                                                        //
        .def_readwrite("timeseries", &qdb::batch_column_info::timeseries)                    //
        .def_readwrite("column", &qdb::batch_column_info::column)                            //
        .def_readwrite("elements_count_hint", &qdb::batch_column_info::elements_count_hint); //
}

} // namespace qdb
