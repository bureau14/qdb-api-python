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
#include "query.hpp"
#include "utils.hpp"
#include "numpy.hpp"

namespace py = pybind11;

namespace qdb
{

  py::handle coerce_point(qdb_point_result_t p) {

  switch (p.type) {
  case qdb_query_result_none:
    return Py_None;

  case qdb_query_result_double:
    return PyFloat_FromDouble(p.payload.double_.value);

  case qdb_query_result_blob:
    return PyUnicode_FromStringAndSize(static_cast<char const *>(p.payload.blob.content),
                                       static_cast<Py_ssize_t>(p.payload.blob.content_length));

  case qdb_query_result_int64:
    return PyLong_FromLongLong(p.payload.int64_.value);

  case qdb_query_result_timestamp:
    return qdb::numpy::datetime64(p.payload.timestamp.value);
  }

  throw std::runtime_error("Unable to cast QuasarDB type to Python type");
  }


/* static */ query::result_t query::run(qdb::handle_ptr h, std::string const & q) {
  qdb_query_result_t * r;
  qdb::qdb_throw_if_error(qdb_query(*h, q.c_str(), &r));


  // Coerce the results

  result_t ret;

  for (qdb_size_t i = 0; i < r->row_count; ++i) {
    std::map<std::string, py::handle> row;


    for (qdb_size_t j = 0; j < r->column_count; ++j) {
      auto column_name = qdb::to_string(r->column_names[j]);
      auto value = coerce_point(r->rows[i][j]);

      row[column_name] = value;
    }

    ret.push_back(row);

  }


  return ret;
}

} // namespace qdb
