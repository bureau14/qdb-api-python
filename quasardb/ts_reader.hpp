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

#include <pybind11/numpy.h>
#include <pybind11/stl_bind.h>
#include <qdb/ts.h>

#include "numpy.hpp"
#include "ts_convert.hpp"
#include "detail/ts_column.hpp"

namespace py = pybind11;

namespace qdb
{

  typedef std::vector<detail::column_info> ts_columns_t;

/**
 * Our value class points to a specific index in a local table, and provides
 * the necessary conversion functions. It does not hold any value of itself.
 */
class ts_reader_value
{
public:
    ts_reader_value()
        : _local_table(nullptr)
        , _index(-1)
        , _type(qdb_ts_column_uninitialized)
    {}

    ts_reader_value(qdb_local_table_t local_table, int64_t index, qdb_ts_column_type_t type)
        : _local_table(local_table)
        , _index(index)
        , _type(type)
    {}

    ts_reader_value(const ts_reader_value & rhs)
        : _local_table(rhs._local_table)
        , _index(rhs._index)
        , _type(rhs._type)
    {}

    ts_reader_value(ts_reader_value && rhs)
        : _local_table(rhs._local_table)
        , _index(rhs._index)
        , _type(rhs._type)
    {
        rhs._local_table = nullptr;
        rhs._index       = -1;
        rhs._type        = qdb_ts_column_uninitialized;
    };

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
        qdb::qdb_throw_if_error(qdb_ts_row_get_int64(_local_table, _index, &v));
        return PyLong_FromLongLong(v);
    }

    py::handle blob() const
    {
        void const * v = nullptr;
        qdb_size_t l   = 0;

        qdb::qdb_throw_if_error(qdb_ts_row_get_blob(_local_table, _index, &v, &l));
        return PyByteArray_FromStringAndSize(static_cast<char const *>(v), static_cast<Py_ssize_t>(l));
    }

    py::handle double_() const
    {
        double v = 0.0;
        qdb::qdb_throw_if_error(qdb_ts_row_get_double(_local_table, _index, &v));
        return PyFloat_FromDouble(v);
    }

    py::handle timestamp() const
    {
        qdb_timespec_t v;
        qdb::qdb_throw_if_error(qdb_ts_row_get_timestamp(_local_table, _index, &v));

        return qdb::numpy::to_datetime64(v);
    }

private:
    qdb_local_table_t _local_table;
    int64_t _index;
    qdb_ts_column_type_t _type;
};

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
        // Since our row doesn't hold any intrinsic data itself and is merely
        // an indirection to the data in the local table, it doesn't make a lot
        // of sense to compare it with another other than comparing the timestamps
        // and the local table references.
        return _timestamp.tv_sec == rhs._timestamp.tv_sec && _timestamp.tv_nsec == rhs._timestamp.tv_nsec
               && _local_table == rhs._local_table;
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

    ts_reader_value get_item(int64_t index) const
    {
        // TODO: we construct this object every time; should be without any heap allocations,
        // but can we do without this?
        return ts_reader_value(_local_table, index, _columns.at(index).type);
    }

    void set_item(int64_t index, int64_t value)
    {
        // not implemented
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

    ts_reader_value get_item(std::string const & alias) const
    {
        auto c = _indexed_columns.find(alias);
        if (c == _indexed_columns.end())
        {
            throw pybind11::key_error();
        }

        qdb_size_t index = c->second.second;
        return ts_reader_value(_local_table, index, c->second.first);
    }

    void set_item(std::string const & alias, std::string const & value)
    {
        // not implemented
    }

private:
  detail::indexed_columns_t _indexed_columns;
};

template <typename RowType>
class ts_reader_iterator
{
public:
    using value_type        = RowType;
    using difference_type   = std::ptrdiff_t;
    using pointer           = const value_type *;
    using reference         = const value_type &;
    using iterator_category = std::forward_iterator_tag;

public:
    ts_reader_iterator()
        : _local_table(nullptr)
        , _the_row(_local_table, _columns)
    {}

    ts_reader_iterator(qdb_local_table_t local_table, ts_columns_t const & columns)
        : _local_table(local_table)
        , _columns(columns)
        , _the_row(_local_table, _columns)
    {
        // Immediately try to move to the first row
        ++(*this);
    }

    bool operator==(ts_reader_iterator const & rhs) const noexcept
    {
        // Our .end() iterator is recognized by a null local table, and we'll
        // ignore the actual row object.
        if (rhs._local_table == nullptr || _local_table == nullptr)
        {
            return _local_table == rhs._local_table;
        }
        else
        {
            return _local_table == rhs._local_table && _the_row == rhs._the_row;
        }
    }
    bool operator!=(ts_reader_iterator const & rhs) const noexcept
    {
        return !(*this == rhs);
    }

    pointer operator->() const noexcept
    {
        return &_the_row;
    }

    reference operator*() const noexcept
    {
        return _the_row;
    }

    ts_reader_iterator & operator++() noexcept
    {
        qdb_error_t err = qdb_ts_table_next_row(_local_table, &_the_row.mutable_timestamp());

        if (err == qdb_e_iterator_end)
        {
            // As seen in the default constructor and operator==, an empty _local_table
            // designates an end-iterator.
            _local_table = nullptr;
        }
        else
        {
            qdb::qdb_throw_if_error(err);
        }

        return *this;
    }

private:
    qdb_local_table_t _local_table;
    ts_columns_t _columns;
    value_type _the_row;
};

template <typename RowType>
class ts_reader
{
public:
    typedef ts_reader_iterator<RowType> iterator;

public:
    ts_reader(qdb::handle_ptr h, const std::string & t, const ts_columns_t & c, const std::vector<qdb_ts_range_t> & r)
        : _handle(h)
        , _columns(c)
        , _local_table(nullptr)
    {
        auto c_columns = convert_columns(c);

        qdb::qdb_throw_if_error(qdb_ts_local_table_init(*_handle, t.c_str(), c_columns.data(), c_columns.size(), &_local_table));

        qdb::qdb_throw_if_error(qdb_ts_table_get_ranges(_local_table, r.data(), r.size()));
    }

    // since our reader models a stateful generator, we prevent copies
    ts_reader(const ts_reader &) = delete;

    // since our reader models a stateful generator, we prevent moves
    ts_reader(ts_reader && rhs) = delete;

    ~ts_reader()
    {
        if (_handle && _local_table)
        {
            qdb_release(*_handle, _local_table);
            _local_table = nullptr;
        }
    }

    iterator begin()
    {
        return iterator(_local_table, _columns);
    }

    iterator end()
    {
        return iterator();
    }

private:
    qdb::handle_ptr _handle;
    ts_columns_t _columns;
    qdb_local_table_t _local_table;
};

template <typename Module>
static inline void register_ts_reader(Module & m)
{
    py::class_<qdb::ts_reader_value>{m, "TimeSeriesValue"};

    py::bind_vector<std::vector<py::object>>(m, "VectorObject");

    py::class_<qdb::ts_fast_row>{m, "TimeSeriesFastRow"}
        .def("__getitem__", &qdb::ts_fast_row::get_item, py::return_value_policy::move)
        .def("__setitem__", &qdb::ts_fast_row::set_item)
        .def("timestamp", &qdb::ts_fast_row::timestamp)
        .def("copy", &qdb::ts_fast_row::copy);

    py::class_<qdb::ts_dict_row>{m, "TimeSeriesDictRow"}
        .def("__getitem__", &qdb::ts_dict_row::get_item, py::return_value_policy::move)
        .def("__setitem__", &qdb::ts_dict_row::set_item)
        .def("timestamp", &qdb::ts_dict_row::timestamp);

    py::class_<qdb::ts_reader<qdb::ts_fast_row>>{m, "TimeSeriesFastReader"}
        .def(py::init<qdb::handle_ptr, const std::string &, const ts_columns_t &, const std::vector<qdb_ts_range_t> &>())

        .def("__iter__", [](ts_reader<qdb::ts_fast_row> & r) { return py::make_iterator(r.begin(), r.end()); }, py::keep_alive<0, 1>());

    py::class_<qdb::ts_reader<qdb::ts_dict_row>>{m, "TimeSeriesDictReader"}
        .def(py::init<qdb::handle_ptr, const std::string &, const ts_columns_t &, const std::vector<qdb_ts_range_t> &>())

        .def("__iter__", [](ts_reader<qdb::ts_dict_row> & r) { return py::make_iterator(r.begin(), r.end()); }, py::keep_alive<0, 1>());
}

} // namespace qdb

namespace pybind11
{
namespace detail
{

/**
 * Implements custom type caster for our ts_reader_value class, so that conversion
 * to and from native python types is completely transparent.
 */
template <>
struct type_caster<qdb::ts_reader_value>
{
public:
    /**
     * Note that this macro magically sets a member variable called 'value'.
     */
    PYBIND11_TYPE_CASTER(qdb::ts_reader_value, _("qdb::ts_reader_value"));

    /**
     * We do not support Python->C++
     */
    bool load(handle src, bool)
    {
        return false;
    }

    /**
     * C++->Python
     */
    static handle cast(qdb::ts_reader_value src, return_value_policy /* policy */, handle /* parent */)
    {
        return src.cast();
    }
};

}; // namespace detail
}; // namespace pybind11
