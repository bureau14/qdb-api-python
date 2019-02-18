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
#include <iostream>

namespace qdb
{

  class ts_row {
  public:
    ts_row() {
    }

    bool operator==(ts_row const & rhs) const noexcept {
      return true;
    }

    bool operator!=(ts_row const & rhs) const noexcept {
      return false;
    }

  };

  class ts_reader_iterator {
  public:
    using value_type        = ts_row;
    using difference_type   = std::ptrdiff_t;
    using pointer           = const value_type*;
    using reference         = const value_type&;
    using iterator_category = std::forward_iterator_tag;

  public:
    ts_reader_iterator()
      : _local_table(nullptr) {
    }

    ts_reader_iterator(qdb_local_table_t local_table)
      : _local_table(local_table) {
    }

    bool operator==(ts_reader_iterator const& rhs) const noexcept {
      // Our .end() iterator is recognized by a null local table.
      if (rhs._local_table == nullptr || _local_table == nullptr) {
        return _local_table == rhs._local_table;
      } else {
        return _the_row == rhs._the_row;
      }
    }
    bool operator!=(ts_reader_iterator const& rhs) const noexcept {
      return !(*this == rhs);
    }

    pointer operator->() const noexcept {
      return &_the_row;
    }

    reference operator*() const noexcept {
      return _the_row;
    }

    ts_reader_iterator & operator++() noexcept {
      _local_table = nullptr;
      return *this;
    }

  private:
    ts_row _the_row;

    qdb_local_table_t _local_table;
  };

class ts_reader
{
public:
  typedef ts_reader_iterator iterator;

public:
  ts_reader(qdb::handle_ptr h, const std::string & t, const std::vector<qdb_ts_column_info_t> & c)
    : _handle{h},
      _local_table(nullptr)
    {
      qdb::qdb_throw_if_error(qdb_ts_local_table_init(*_handle,
                                                      t.c_str(),
                                                      c.data(),
                                                      c.size(),
                                                      &_local_table));
    }

    // since our reader models a stateful generator, we prevent copies
    ts_reader(const ts_reader &) = delete;

    ~ts_reader()
    {
        if (_handle && _local_table)
        {
            qdb_release(*_handle, _local_table);
            _local_table = nullptr;
        }
    }

    iterator
    begin() {
      return iterator(_local_table);
    }

    iterator
    end() {
      return iterator();
    }

private:
    qdb::handle_ptr _handle;
    qdb_local_table_t _local_table;
};

using ts_reader_ptr = std::unique_ptr<ts_reader>;

template <typename Module>
static inline void register_ts_reader(Module & m)
{
    namespace py = pybind11;

    py::class_<qdb::ts_row>{m, "TimeSeriesRow"}
    .def(py::init<>());

    py::class_<qdb::ts_reader>{m, "TimeSeriesReader"}
    .def(py::init<qdb::handle_ptr,
                  const std::string &,
                  const std::vector<qdb_ts_column_info_t>>())


       .def("__iter__", [](ts_reader & r) {
                          return py::make_iterator(r.begin(), r.end());
                        },py::keep_alive<0, 1>());
}

} // namespace qdb
