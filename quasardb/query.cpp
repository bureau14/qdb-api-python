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
#include "masked_array.hpp"
#include "metrics.hpp"
#include "numpy.hpp"
#include "traits.hpp"
#include "utils.hpp"
#include "convert/array.hpp"
#include "convert/value.hpp"
#include "detail/qdb_resource.hpp"
#include <pybind11/stl.h>
#include <range/v3/view/counted.hpp>
#include <range/v3/view/transform.hpp>
#include <iostream>
#include <set>
#include <sstream>
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
static std::vector<bool> coerce_blobs_opt(
    const std::vector<std::string> & column_names, const py::object & opts)
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
            ret.push_back(
                std::find(specific_blobs.begin(), specific_blobs.end(), col) != specific_blobs.end());
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
        return PyBytes_FromStringAndSize(static_cast<char const *>(p.payload.blob.content),
            static_cast<Py_ssize_t>(p.payload.blob.content_length));
    }

    case qdb_query_result_string:
        return PyUnicode_FromStringAndSize(static_cast<char const *>(p.payload.string.content),
            static_cast<Py_ssize_t>(p.payload.string.content_length));

    case qdb_query_result_int64:
        return PyLong_FromLongLong(p.payload.int64_.value);

    case qdb_query_result_count:
        return PyLong_FromLongLong(p.payload.count.value);

    case qdb_query_result_timestamp:
        return qdb::numpy::datetime64(p.payload.timestamp.value);

    case qdb_query_result_array_double:
    case qdb_query_result_array_int64:
    case qdb_query_result_array_blob:
    case qdb_query_result_array_timestamp:
    case qdb_query_result_array_string:
        break;
    }

    throw std::runtime_error("Unable to cast QuasarDB type to Python type");
}

std::vector<std::string> coerce_column_names(const qdb_query_result_t & r)
{
    std::vector<std::string> xs;
    xs.reserve(r.column_count);

    for (qdb_size_t i = 0; i < r.column_count; ++i)
    {
        xs.push_back(qdb::to_string(r.column_names[i]));
    }

    return xs;
}

static dict_query_result_t convert_query_results(const qdb_query_result_t * r,
    const std::vector<std::string> & column_names,
    const std::vector<bool> & parse_blobs)
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

/**
 * An all-null column carries no type information (the result has per-cell
 * tags only, no column schema); keep the historical fully-masked float64
 * NaN array.
 */
qdb::masked_array numpy_null_array(qdb_size_t row_count)
{
    py::array::ShapeContainer shape{row_count};
    auto data = qdb::numpy::array::initialize<std::double_t>(
        shape, std::numeric_limits<std::double_t>::quiet_NaN());

    return qdb::masked_array::masked_all(data);
}

/**
 * Maps a query result tag to the qdb primitive stored in the union payload.
 */
template <qdb_query_result_value_type_t Tag>
struct point_result_traits;

template <>
struct point_result_traits<qdb_query_result_double>
{
    using primitive = double;

    static constexpr primitive get(qdb_point_result_t const & p) noexcept
    {
        return p.payload.double_.value;
    }
};

template <>
struct point_result_traits<qdb_query_result_int64>
{
    using primitive = qdb_int_t;

    static constexpr primitive get(qdb_point_result_t const & p) noexcept
    {
        return p.payload.int64_.value;
    }
};

/**
 * count() carries an unsigned qdb_size_t payload; the convert framework only
 * knows the signed qdb_int_t primitive, so narrow explicitly. A count >= 2^63
 * would alias the int64 null sentinel.
 */
template <>
struct point_result_traits<qdb_query_result_count>
{
    using primitive = qdb_int_t;

    static constexpr primitive get(qdb_point_result_t const & p) noexcept
    {
        return static_cast<qdb_int_t>(p.payload.count.value);
    }
};

template <>
struct point_result_traits<qdb_query_result_timestamp>
{
    using primitive = qdb_timespec_t;

    static constexpr primitive get(qdb_point_result_t const & p) noexcept
    {
        return p.payload.timestamp.value;
    }
};

/**
 * The string payload is layout-identical to qdb_string_t but a distinct type,
 * so re-wrap instead of casting. The result borrows C-result-owned bytes;
 * conversion copies them into numpy-owned buffers before release.
 */
template <>
struct point_result_traits<qdb_query_result_string>
{
    using primitive = qdb_string_t;

    static constexpr primitive get(qdb_point_result_t const & p) noexcept
    {
        return qdb_string_t{p.payload.string.content, p.payload.string.content_length};
    }
};

/**
 * The blob payload is layout-identical to qdb_blob_t but a distinct type, so
 * re-wrap instead of casting. The result borrows C-result-owned bytes;
 * conversion copies them into py::bytes before release.
 */
template <>
struct point_result_traits<qdb_query_result_blob>
{
    using primitive = qdb_blob_t;

    static constexpr primitive get(qdb_point_result_t const & p) noexcept
    {
        return qdb_blob_t{p.payload.blob.content, p.payload.blob.content_length};
    }
};

/**
 * Tag-checked projection of one result cell to its qdb primitive: none becomes
 * the primitive's null sentinel, a tag mismatch raises IncompatibleTypeError
 * instead of reinterpreting the union payload.
 */
template <qdb_query_result_value_type_t Tag>
struct project_point
{
    using primitive = typename point_result_traits<Tag>::primitive;

    qdb_size_t column;

    inline primitive operator()(qdb_point_result_t const * row) const
    {
        qdb_point_result_t const & p = row[column];

        if (p.type == qdb_query_result_none) [[unlikely]]
        {
            return traits::qdb_value<primitive>::null_value();
        }

        if (p.type != Tag) [[unlikely]]
        {
            std::stringstream ss;
            ss << "query column contains mixed types: expected type " << Tag << ", got "
               << p.type;
            throw qdb::incompatible_type_exception{ss.str()};
        }

        return point_result_traits<Tag>::get(p);
    }
};

qdb_query_result_value_type_t probe_column_type(qdb_query_result_t const & r, qdb_size_t column)
{
    // Probe a column for its value type, by returning the type of the first non-null
    // value.
    for (qdb_size_t row = 0; row < r.row_count; ++row)
    {
        if (r.rows[row][column].type != qdb_query_result_none)
        {
            return r.rows[row][column].type;
        }
    }

    // No non-null values were part of the dataset.
    return qdb_query_result_none;
}

/**
 * Converts one result column over a row subrange. `tag` is passed in, not
 * probed here: the streaming reader probes once over the full result at open;
 * re-probing per batch would let an all-null batch change dtype mid-stream.
 */
qdb::masked_array numpy_query_array(qdb_point_result_t const * const * rows,
    qdb_size_t row_count,
    qdb_size_t column,
    qdb_query_result_value_type_t tag)
{

    switch (tag)
    {

    case qdb_query_result_double:
        return convert::masked_array<double, traits::float64_dtype>(
            ranges::views::counted(rows, row_count)
            | ranges::views::transform(project_point<qdb_query_result_double>{column}));

    case qdb_query_result_int64:
        return convert::masked_array<qdb_int_t, traits::int64_dtype>(
            ranges::views::counted(rows, row_count)
            | ranges::views::transform(project_point<qdb_query_result_int64>{column}));

    case qdb_query_result_count:
        return convert::masked_array<qdb_int_t, traits::int64_dtype>(
            ranges::views::counted(rows, row_count)
            | ranges::views::transform(project_point<qdb_query_result_count>{column}));

    case qdb_query_result_timestamp:
        return convert::masked_array<qdb_timespec_t, traits::datetime64_ns_dtype>(
            ranges::views::counted(rows, row_count)
            | ranges::views::transform(project_point<qdb_query_result_timestamp>{column}));

    /**
     * Strings become numpy "U" arrays (matching the bulk reader). An empty
     * string aliases the null sentinel (length == 0) and comes back masked.
     * The variable-width to_array traverses the range twice, so the
     * projection must be idempotent.
     */
    case qdb_query_result_string:
        return convert::masked_array<qdb_string_t, traits::unicode_dtype>(
            ranges::views::counted(rows, row_count)
            | ranges::views::transform(project_point<qdb_query_result_string>{column}));

    /**
     * Blobs become numpy object arrays of py::bytes (matching the bulk
     * reader). An empty blob aliases the null sentinel (content_length == 0)
     * and comes back masked None.
     */
    case qdb_query_result_blob:
        return convert::masked_array<qdb_blob_t, traits::pyobject_dtype>(
            ranges::views::counted(rows, row_count)
            | ranges::views::transform(project_point<qdb_query_result_blob>{column}));

    /* Dtype choice for typeless columns: see numpy_null_array. */
    case qdb_query_result_none:
        return numpy_null_array(row_count);

    default: {
        std::stringstream ss;
        ss << "unrecognized query result column type: " << tag;
        throw qdb::incompatible_type_exception(ss.str());
    }
    };
}

qdb::masked_array numpy_query_array(qdb_query_result_t const & r, qdb_size_t column)
{
    return numpy_query_array(r.rows, r.row_count, column, probe_column_type(r, column));
}

numpy_query_column_t numpy_query_column(qdb_query_result_t const & r, qdb_size_t column)
{

    qdb::numpy_query_column_t ret;
    ret.first  = qdb::to_string(r.column_names[column]);
    ret.second = py::cast(numpy_query_array(r, column));
    return ret;
}

numpy_query_result_t numpy_query_results(qdb_query_result_t const & r)
{
    qdb::numpy_query_result_t ret{};
    ret.reserve(r.column_count);

    // First initialize the result vector. This means storing the column names,
    // and pre-allocating the column result arrays with data points for each .
    for (qdb_size_t j = 0; j < r.column_count; ++j)
    {
        ret.push_back(numpy_query_column(r, j));
    }

    return ret;
}

numpy_query_result_t numpy_query_results(const qdb_query_result_t * r)
{
    if (!r || r->column_count == 0 || r->row_count == 0)
    {
        return numpy_query_result_t{};
    }

    const std::vector<std::string> column_names = coerce_column_names(*r);
    return numpy_query_results(*r);
}

dict_query_result_t dict_query(qdb::handle_ptr h, const std::string & q, const py::object & blobs)
{
    detail::qdb_resource<qdb_query_result_t> r{*h};

    qdb_error_t err;
    {
        metrics::scoped_capture capture{"qdb_query"};
        err = qdb_query(*h, q.c_str(), &r);
    }

    qdb::qdb_throw_if_query_error(*h, err, r.get());

    return convert_query_results(r, blobs);
}

numpy_query_result_t numpy_query(qdb::handle_ptr h, const std::string & q)
{
    detail::qdb_resource<qdb_query_result_t> r{*h};

    qdb_error_t err;
    {
        metrics::scoped_capture capture{"qdb_query"};
        err = qdb_query(*h, q.c_str(), &r);
    }
    qdb::qdb_throw_if_query_error(*h, err, r.get());

    return numpy_query_results(r);
}

} // namespace qdb
