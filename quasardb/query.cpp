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

#include "query.hpp"
#include "numpy.hpp"
#include "ts_convert.hpp"
#include "utils.hpp"
#include <pybind11/stl.h>
#include <iostream>
#include <set>
#include <string>

namespace py = pybind11;

namespace qdb
{

/**
 * Options that define whether or not to return blobs as bytearrays or string. Defaults to
 * strings.
 */
typedef enum query_blobs_type_t
{
    query_blobs_type_none    = 0,
    query_blobs_type_all     = 1,
    query_blobs_type_columns = 2
} qdb_blobs_type_t;

typedef struct
{
    query_blobs_type_t type;
    std::vector<std::string> columns;
} query_blobs_t;

/**
 * Blobs can be provided in a boolean (blobs=True or blobs=False) or as as specific array
 * (blobs=['packet', 'other_packet']).
 *
 * Takes a python object and an array of column names, and returns a bitmap which denotes
 * whether a column needs to be returned as a blob (True) or as a string (False).
 */
static std::vector<bool> coerce_blobs_opt(const std::vector<std::string> & column_names, const py::object & opts)
{
    // First try the most common case, a boolean
    try
    {
        bool all_blobs = py::cast<bool>(opts);
        return std::vector<bool>(column_names.size(), all_blobs);
    }
    catch (const std::runtime_error & /*_*/)
    {
        std::vector<std::string> specific_blobs = py::cast<std::vector<std::string>>(opts);
        std::vector<bool> ret;
        ret.reserve(column_names.size());

        for (auto const & col : column_names)
        {
            ret.push_back(std::find(specific_blobs.begin(), specific_blobs.end(), col) != specific_blobs.end());
        }

        return ret;
    }
}

static py::handle coerce_point(qdb_point_result_t p, bool parse_blob)
{
    switch (p.type)
    {
    case qdb_query_result_none:
        return Py_None;

    case qdb_query_result_double:
        return PyFloat_FromDouble(p.payload.double_.value);

    case qdb_query_result_blob: {
        return PyBytes_FromStringAndSize(
            static_cast<char const *>(p.payload.blob.content), static_cast<Py_ssize_t>(p.payload.blob.content_length));
    }

    case qdb_query_result_string:
        return PyUnicode_FromStringAndSize(
            static_cast<char const *>(p.payload.string.content), static_cast<Py_ssize_t>(p.payload.string.content_length));

    case qdb_query_result_symbol:
        return PyUnicode_FromStringAndSize(
            static_cast<char const *>(p.payload.symbol.content), static_cast<Py_ssize_t>(p.payload.symbol.content_length));

    case qdb_query_result_int64:
        return PyLong_FromLongLong(p.payload.int64_.value);

    case qdb_query_result_count:
        return PyLong_FromLongLong(p.payload.count.value);

    case qdb_query_result_timestamp:
        return qdb::numpy::datetime64(p.payload.timestamp.value);
    }

    throw std::runtime_error("Unable to cast QuasarDB type to Python type");
}

static std::vector<std::string> coerce_column_names(const qdb_query_result_t & r)
{
    std::vector<std::string> xs;
    xs.reserve(r.column_count);

    for (qdb_size_t i = 0; i < r.column_count; ++i)
    {
        xs.push_back(qdb::to_string(r.column_names[i]));
    }

    return xs;
}

static dict_query_result_t convert_query_results(
    const qdb_query_result_t * r, const std::vector<std::string> & column_names, const std::vector<bool> & parse_blobs)
{
    qdb::dict_query_result_t ret;

    for (qdb_size_t i = 0; i < r->row_count; ++i)
    {
        std::map<std::string, py::handle> row;

        for (qdb_size_t j = 0; j < r->column_count; ++j)
        {
            const auto & column_name = column_names[j];
            auto value               = coerce_point(r->rows[i][j], parse_blobs[j]);

            row[column_name] = value;
        }

        ret.push_back(row);
    }

    return ret;
}

dict_query_result_t convert_query_results(const qdb_query_result_t * r, const py::object & blobs)
{
    if (!r) return dict_query_result_t{};
    const std::vector<std::string> column_names = coerce_column_names(*r);
    const std::vector<bool> parse_blobs         = coerce_blobs_opt(column_names, blobs);
    return convert_query_results(r, column_names, parse_blobs);
}

dict_query_result_t dict_query(qdb::handle_ptr h, const std::string & q, const py::object & blobs)
{
    qdb_query_result_t * r = nullptr;
    qdb_error_t err = qdb_query(*h, q.c_str(), &r);
    if (QDB_FAILURE(err))
    {
        if (err == qdb_e_network_inbuf_too_small)
        {
            throw qdb::input_buffer_too_small_exception{};
        }
        auto error_message = std::string{qdb_error(err)};
        if (r != nullptr)
        {
            error_message += ' ';
            error_message += std::string{r->error_message.data, r->error_message.length};
            qdb_release(*h, r);
        }
        throw qdb::exception{err, error_message.data()};
    }
    auto res = convert_query_results(r, blobs);
    qdb_release(*h, r);
    return res;
}

} // namespace qdb
