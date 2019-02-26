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

#include <qdb/ts.h>

namespace qdb
{

namespace detail
{

struct column_info
{
    column_info() = default;

    column_info(qdb_ts_column_type_t t, const std::string & n)
        : type{t}
        , name{n}
    {}

    column_info(const qdb_ts_column_info_t & ci)
        : column_info{ci.type, ci.name}
    {}

    operator qdb_ts_column_info_t() const noexcept
    {
        qdb_ts_column_info_t res;

        res.type = type;
        res.name = name.c_str();

        return res;
    }

    qdb_ts_column_type_t type{qdb_ts_column_uninitialized};
    std::string name;
};

static std::vector<qdb_ts_column_info_t> convert_columns(const std::vector<column_info> & columns)
{
    std::vector<qdb_ts_column_info_t> c_columns(columns.size());

    std::transform(columns.cbegin(), columns.cend(), c_columns.begin(), [](const column_info & ci) -> qdb_ts_column_info_t { return ci; });

    return c_columns;
}

static std::vector<column_info> convert_columns(const qdb_ts_column_info_t * columns, size_t count)
{
    std::vector<column_info> c_columns(count);

    std::transform(columns, columns + count, c_columns.begin(), [](const qdb_ts_column_info_t & ci) { return column_info{ci}; });

    return c_columns;
}

static std::vector<std::string> column_list_to_strings(const std::vector<column_info> & columns)
{
    std::vector<std::string> s_columns(columns.size());

    std::transform(columns.cbegin(), columns.cend(), s_columns.begin(), [](const column_info & ci) -> std::string { return ci.name; });

    return s_columns;
}

typedef std::map<std::string, std::pair<qdb_ts_column_type_t, qdb_size_t>> indexed_columns_t;

template <typename ColumnType>
static indexed_columns_t index_columns(const std::vector<ColumnType> & columns)
{
    indexed_columns_t i_columns;
    for (qdb_size_t i = 0; i < columns.size(); ++i)
    {
        i_columns.insert(indexed_columns_t::value_type(columns[i].name, std::make_pair(columns[i].type, i)));
    }

    return i_columns;
}

template <typename Module>
static inline void register_ts_column(Module & m)
{
    namespace py = pybind11;

    py::class_<column_info>{m, "ColumnInfo"}                        //
        .def(py::init<qdb_ts_column_type_t, const std::string &>()) //
        .def_readwrite("type", &column_info::type)                  //
        .def_readwrite("name", &column_info::name);                 //
}

} // namespace detail

} // namespace qdb
