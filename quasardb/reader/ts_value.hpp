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

#include "../numpy.hpp"
#include <qdb/ts.h>
#include <pybind11/numpy.h>
#include <pybind11/stl_bind.h>

namespace py = pybind11;

namespace qdb
{
namespace reader
{

/**
 * Our value class points to a specific index in a local table, and provides
 * the necessary conversion functions. It does not hold any value of itself.
 */
class ts_value
{
public:
    ts_value() noexcept
        : _local_table{nullptr}
        , _index{-1}
        , _type{qdb_ts_column_uninitialized}
    {}

    ts_value(qdb_local_table_t local_table, int64_t index, qdb_ts_column_type_t type) noexcept
        : _local_table{local_table}
        , _index{index}
        , _type{type}
    {}

    ts_value(const ts_value & rhs) noexcept
        : _local_table{rhs._local_table}
        , _index{rhs._index}
        , _type{rhs._type}
    {}

    ts_value(ts_value && rhs) noexcept
        : _local_table{rhs._local_table}
        , _index{rhs._index}
        , _type{rhs._type} {};

    /**
     * Coerce this value to a Python value, used by the automatic type caster
     * defined at the bottom.
     */
    py::handle cast() const
    {
        switch (_type)
        {
        case qdb_ts_column_double:
            return double_();
        case qdb_ts_column_blob:
            return blob();
        case qdb_ts_column_int64:
            return int64();
        case qdb_ts_column_timestamp:
            return timestamp();
        };

        throw std::runtime_error("Unable to cast QuasarDB type to Python type");
    }

private:
    py::handle int64() const
    {
        std::int64_t v;

        auto res = qdb_ts_row_get_int64(_local_table, _index, &v);
        if (res == qdb_e_element_not_found)
        {
            Py_RETURN_NONE;
            // notreached
        }

        qdb::qdb_throw_if_error(res);

        return PyLong_FromLongLong(v);
    }

    py::handle blob() const
    {
        const void * v = nullptr;
        qdb_size_t l   = 0;

        auto res = qdb_ts_row_get_blob(_local_table, _index, &v, &l);
        if (res == qdb_e_element_not_found)
        {
            Py_RETURN_NONE;
            // notreached
        }

        qdb::qdb_throw_if_error(res);
        return PyByteArray_FromStringAndSize(static_cast<const char *>(v), static_cast<Py_ssize_t>(l));
    }

    py::handle double_() const
    {
        double v = 0.0;
        auto res = qdb_ts_row_get_double(_local_table, _index, &v);
        if (res == qdb_e_element_not_found)
        {
            Py_RETURN_NONE;
            // notreached
        }

        qdb::qdb_throw_if_error(res);
        return PyFloat_FromDouble(v);
    }

    py::handle timestamp() const
    {
        qdb_timespec_t v;
        auto res = qdb_ts_row_get_timestamp(_local_table, _index, &v);
        if (res == qdb_e_element_not_found)
        {
            Py_RETURN_NONE;
            // notreached
        }

        qdb::qdb_throw_if_error(res);
        return qdb::numpy::datetime64(v);
    }

private:
    qdb_local_table_t _local_table;
    int64_t _index;
    qdb_ts_column_type_t _type;
};

template <typename Module>
static inline void register_ts_value(Module & m)
{
    py::class_<ts_value>{m, "TimeSeriesReaderValue"};
}

} // namespace reader
} // namespace qdb

namespace pybind11
{
namespace detail
{

/**
 * Implements custom type caster for our ts_value class, so that conversion
 * to and from native python types is completely transparent.
 */
template <>
struct type_caster<qdb::reader::ts_value>
{
public:
    /**
     * Note that this macro magically sets a member variable called 'value'.
     */
    PYBIND11_TYPE_CASTER(qdb::reader::ts_value, _("qdb::reader::ts_value"));

    /**
     * We do not support Python->C++
     */
    bool load(handle src, bool) const noexcept
    {
        return false;
    }

    /**
     * C++->Python
     */
    static handle cast(qdb::reader::ts_value src, return_value_policy /* policy */, handle /* parent */)
    {
        return src.cast();
    }
};

} // namespace detail
} // namespace pybind11
