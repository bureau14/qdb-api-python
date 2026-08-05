/*
 *
 * Official Python API
 *
 * Copyright (c) 2009-2026, quasardb SAS. All rights reserved.
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

#include "detail/ts_column.hpp"
#include <pybind11/chrono.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <chrono>
#include <utility>
#include <vector>

namespace qdb
{

class table_schema
{
public:
    table_schema(std::vector<detail::column_info> columns,
        std::chrono::milliseconds shard_size = std::chrono::hours{24},
        std::chrono::milliseconds ttl        = std::chrono::milliseconds::zero())
        : columns{std::move(columns)}
        , shard_size{shard_size}
        , ttl{ttl}
    {}

    table_schema(table_schema const & other)
        : columns{other.columns}
        , shard_size{other.shard_size}
        , ttl{other.ttl}
    {}

    table_schema(table_schema && other) noexcept
        : columns{std::move(other.columns)}
        , shard_size{other.shard_size}
        , ttl{other.ttl}
    {}

    table_schema & operator=(table_schema const & other)
    {
        columns    = other.columns;
        shard_size = other.shard_size;
        ttl        = other.ttl;
        _column_schema.clear();
        return *this;
    }

    table_schema & operator=(table_schema && other) noexcept
    {
        columns    = std::move(other.columns);
        shard_size = other.shard_size;
        ttl        = other.ttl;
        _column_schema.clear();
        return *this;
    }

    void prepare(qdb_exp_batch_push_table_schema_t & table_schema)
    {
        _column_schema.clear();
        _column_schema.reserve(columns.size());

        for (std::size_t index = 0; index < columns.size(); ++index)
        {
            detail::column_info const & column_info = columns[index];

            qdb_exp_batch_push_column_schema_t column{};
            column.column_type = column_info.type;
            column.index       = static_cast<qdb_ts_column_index_t>(index);
            column.symtable    = column_info.symtable.empty() ? nullptr : column_info.symtable.c_str();
            column.name        = column_info.name.c_str();

            _column_schema.push_back(column);
        }

        table_schema.shard_size   = static_cast<qdb_duration_t>(shard_size.count());
        table_schema.ttl          = ttl == std::chrono::milliseconds::zero()
                                        ? qdb_ttl_disabled
                                        : static_cast<qdb_duration_t>(ttl.count());
        table_schema.columns      = _column_schema.data();
        table_schema.column_count = _column_schema.size();
    }

public:
    std::vector<detail::column_info> columns;
    std::chrono::milliseconds shard_size;
    std::chrono::milliseconds ttl;

private:
    std::vector<qdb_exp_batch_push_column_schema_t> _column_schema;
};

template <typename Module>
static inline void register_table_schema(Module & m)
{
    namespace py = pybind11;

    py::class_<table_schema>{
        m, "TableSchema", "Schema used to create a missing time-series table during a writer push."}
        .def(py::init<std::vector<detail::column_info>, std::chrono::milliseconds,
                 std::chrono::milliseconds>(),
            py::arg("columns"), py::arg("shard_size") = std::chrono::hours{24},
            py::arg("ttl") = std::chrono::milliseconds::zero())
        .def_readwrite("columns", &table_schema::columns)
        .def_readwrite("shard_size", &table_schema::shard_size)
        .def_readwrite("ttl", &table_schema::ttl);
}

} // namespace qdb
