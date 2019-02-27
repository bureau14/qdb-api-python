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

#include "numpy.hpp"
#include "ts_convert.hpp"
#include <qdb/ts.h>
#include "detail/ts_column.hpp"
#include "reader/ts_row.hpp"
#include <pybind11/numpy.h>
#include <pybind11/stl_bind.h>

namespace py = pybind11;

namespace qdb
{

using ts_columns_t = std::vector<detail::column_info>;

template <typename RowType>
class ts_reader_iterator;

template <typename RowType>
bool operator==(const ts_reader_iterator<RowType> & lhs, const ts_reader_iterator<RowType> & rhs) noexcept;

template <typename RowType>
class ts_reader_iterator
{
public:
    using value_type = RowType;
    using reference  = const value_type &;

    friend bool operator==<RowType>(const ts_reader_iterator<RowType> &, const ts_reader_iterator<RowType> &) noexcept;

public:
    ts_reader_iterator()
        : _local_table{nullptr}
        , _the_row{_local_table, _columns}
    {}

    ts_reader_iterator(qdb_local_table_t local_table, const ts_columns_t & columns)
        : _local_table{local_table}
        , _columns{columns}
        , _the_row{_local_table, _columns}
    {
        // Immediately try to move to the first row
        ++(*this);
    }

    bool operator!=(const ts_reader_iterator & rhs) const noexcept
    {
        return !(*this == rhs);
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
bool operator==(const ts_reader_iterator<RowType> & lhs, const ts_reader_iterator<RowType> & rhs) noexcept
{
    // Our .end() iterator is recognized by a null local table, and we'll
    // ignore the actual row object.
    if (lhs._local_table == nullptr || rhs._local_table == nullptr)
    {
        return lhs._local_table == rhs._local_table;
    }
    else
    {
        return lhs._local_table == rhs._local_table && lhs._the_row == rhs._the_row;
    }
}

template <typename RowType>
class ts_reader
{
public:
    using iterator = ts_reader_iterator<RowType>;

public:
    ts_reader(qdb::handle_ptr h, const std::string & t, const ts_columns_t & c, const std::vector<qdb_ts_range_t> & r)
        : _handle{h}
        , _columns{c}
        , _local_table{nullptr}
    {
        auto c_columns = convert_columns(c);

        qdb::qdb_throw_if_error(qdb_ts_local_table_init(*_handle, t.c_str(), c_columns.data(), c_columns.size(), &_local_table));

        qdb::qdb_throw_if_error(qdb_ts_table_get_ranges(_local_table, r.data(), r.size()));
    }

    // since our reader models a stateful generator, we prevent copies
    ts_reader(const ts_reader &) = delete;

    // since our reader models a stateful generator, we prevent moves
    ts_reader(ts_reader && rhs)
        : _handle(rhs._handle)
        , _columns(rhs._columns)
        , _local_table(rhs._local_table)
    {
        rhs._handle      = nullptr;
        rhs._local_table = nullptr;
        rhs._columns.clear();
    }

    ~ts_reader()
    {
        if (_handle && _local_table)
        {
            qdb_release(*_handle, _local_table);
            _local_table = nullptr;
        }
    }

    constexpr iterator begin() const
    {
        return iterator(_local_table, _columns);
    }

    constexpr iterator end() const
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
    py::class_<qdb::ts_reader<reader::ts_fast_row>>{m, "TimeSeriesFastReader"}
        .def(py::init<qdb::handle_ptr, const std::string &, const ts_columns_t &, const std::vector<qdb_ts_range_t> &>())

        .def("__iter__", [](ts_reader<reader::ts_fast_row> & r) { return py::make_iterator(r.begin(), r.end()); }, py::keep_alive<0, 1>());

    py::class_<qdb::ts_reader<reader::ts_dict_row>>{m, "TimeSeriesDictReader"}
        .def(py::init<qdb::handle_ptr, const std::string &, const ts_columns_t &, const std::vector<qdb_ts_range_t> &>())

        .def("__iter__", [](ts_reader<reader::ts_dict_row> & r) { return py::make_iterator(r.begin(), r.end()); }, py::keep_alive<0, 1>());
}

} // namespace qdb
