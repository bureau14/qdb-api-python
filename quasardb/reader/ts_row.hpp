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

#include "../detail/ts_column.hpp"
#include "../numpy.hpp"
#include "../ts_convert.hpp"
#include "ts_value.hpp"
#include <qdb/ts.h>
#include <pybind11/numpy.h>
#include <pybind11/stl_bind.h>

namespace py = pybind11;

namespace qdb
{

namespace reader
{

typedef std::vector<detail::column_info> ts_columns_t;

class ts_row
{
public:
    // We need a default constructor to due being copied as part of an iterator.
    ts_row()
        : _local_table(nullptr)
    {}

    ts_row(qdb_local_table_t local_table)
        : _local_table(local_table)
    {}

    bool operator==(ts_row const & rhs) const noexcept
    {
      auto tie = [] (const auto & ts) { return std::tie(ts.tv_sec, ts.tv_nsec); };

      // Since our row doesn't hold any intrinsic data itself and is merely
      // an indirection to the data in the local table, it doesn't make a lot
      // of sense to compare it with another other than comparing the timestamps
      // and the local table references.
      return (tie(_timestamp) == tie(rhs._timestamp)) && (_local_table == rhs._local_table);
    }

    py::handle timestamp() const noexcept
    {
        return numpy::to_datetime64(_timestamp);
    }

    /**
     * Not exposed through Python, but allows the read_row operation to write the underlying
     * timestamp object directly.
     */
    qdb_timespec_t & mutable_timestamp()
    {
        return _timestamp;
    }

protected:
    qdb_local_table_t _local_table;
    qdb_timespec_t _timestamp;
};

/**
 * Our 'fast' row is a pure, lazy list-type row that uses a column's offset for
 * constant-time access to the column values. E.g. row[0], row[1] which will return
 * a (short-lived) ts_value.
 */
class ts_fast_row : public ts_row
{
public:
    ts_fast_row()
        : ts_row()
    {}

    ts_fast_row(qdb_local_table_t local_table, ts_columns_t const & columns)
        : ts_row(local_table)
        , _columns(columns)
    {}

    std::vector<py::object> copy() const
    {
        std::vector<py::object> res;
        res.reserve(_columns.size());

        for (auto index = 0; index < _columns.size(); ++index)
        {
            // By first casting the value, we create a copy of the concrete type
            // (e.g. the int64 or the blob) rather than the reference into the local
            // table.
            res.push_back(py::cast(get_item(index), py::return_value_policy::move));
        }

        return res;
    }

    ts_value get_item(int64_t index) const
    {
        // TODO(leon): we construct this object every time; should be without any heap
        // allocations,but can we do without this?
        return ts_value(_local_table, index, _columns.at(index).type);
    }

    void set_item(int64_t /* index */, int64_t /* value */)
    {
        // not implemented
    }

    std::string repr() const
    {
        // This is inefficient because we first cast everything to Python objects, get the
        // string representation and then cast back to c++ strings, but it's correct and
        // effective, and it's used for debugging purposes only.
        auto xs         = copy();
        std::string kvs = std::accumulate(xs.cbegin(), xs.cend(), std::string(), [](std::string & acc, auto x) {
            std::string s = py::str(x);
            return acc.empty() ? s : acc + ", " + s;
        });

        return "[" + kvs + "]";
    }

private:
    ts_columns_t _columns;
};

/**
 * Our 'dict' row is a much slower, dict-based row type that provides very convenient
 * access to the columns by their name. It does, however,  index each and every row's
 * columns eagerly, and as such will exhibit much worse performance.
 */
class ts_dict_row : public ts_row
{
public:
    ts_dict_row()
        : ts_row()
    {}

    ts_dict_row(qdb_local_table_t local_table, ts_columns_t const & columns)
        : ts_row(local_table)
        , _indexed_columns(detail::index_columns(columns))
    {}

    ts_dict_row(const ts_dict_row & rhs)
        : ts_row(rhs._local_table)
        , _indexed_columns(rhs._indexed_columns)
    {}

    std::map<std::string, py::object> copy() const
    {
        typedef std::map<std::string, py::object> map_type;
        map_type res;

        for (auto c : _indexed_columns)
        {
            qdb_size_t index = c.second.second;

            // By first casting the value, we create a copy of the concrete type
            // (e.g. the int64 or the blob) rather than the reference into the local
            // table.
            res.insert(map_type::value_type(c.first, py::cast(ts_value(_local_table, index, c.second.first))));
        }

        return res;
    }

    ts_value get_item(std::string const & alias) const
    {
        auto c = _indexed_columns.find(alias);
        if (c == _indexed_columns.end())
        {
            throw pybind11::key_error();
        }

        qdb_size_t index = c->second.second;
        return ts_value(_local_table, index, c->second.first);
    }

    void set_item(std::string const & /* alias */, std::string const & /* value */)
    {
        // not implemented
    }

    std::string repr() const
    {
        // Same as ts_fast_row::repr, this is inefficient but it's ok
        auto xs         = copy();
        std::string kvs = std::accumulate(xs.cbegin(), xs.cend(), std::string(), [](const std::string & acc, const auto & x) {
            std::string s = "'" + x.first + "': " + std::string(py::str(x.second));
            return acc.empty() ? s : acc + ", " + s;
        });

        return "{" + kvs + "}";
    }

private:
    detail::indexed_columns_t _indexed_columns;
};

template <typename Module>
static inline void register_ts_row(Module & m)
{
    py::bind_vector<std::vector<py::object>>(m, "VectorObject");

    py::class_<ts_fast_row>{m, "TimeSeriesReaderFastRow"}
        .def("__repr__", &ts_fast_row::repr)
        .def("__getitem__", &ts_fast_row::get_item, py::return_value_policy::move)
        .def("__setitem__", &ts_fast_row::set_item)
        .def("timestamp", &ts_fast_row::timestamp)
        .def("copy", &ts_fast_row::copy);

    py::class_<ts_dict_row>{m, "TimeSeriesReaderDictRow"}
        .def("__repr__", &ts_dict_row::repr)
        .def("__getitem__", &ts_dict_row::get_item, py::return_value_policy::move)
        .def("__setitem__", &ts_dict_row::set_item)
        .def("timestamp", &ts_dict_row::timestamp)
        .def("copy", &ts_dict_row::copy);
}

} // namespace reader
} // namespace qdb
