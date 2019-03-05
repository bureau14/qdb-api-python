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

#include "ts_convert.hpp"
#include <pybind11/numpy.h>


// A datetime64 in numpy is modeled as a scalar array, which is not integrated
// into pybind's adapters of numpy.
//
// In order to still be able to natively create numpy datetime64 instances, the
// code below proxies the data structures that live inside the numpy code. This
// will allow us to interact with the objects natively.
//
// This works both ways: we can accepts numpy.datetime64 as arguments, but also
// create/return them.
//
// Sourced from:
//   https://raw.githubusercontent.com/numpy/numpy/master/numpy/core/include/numpy/arrayscalars.h
//   https://raw.githubusercontent.com/numpy/numpy/master/numpy/core/include/numpy/ndarraytypes.h
//   https://github.com/numpy/numpy/blob/master/numpy/core/include/numpy/npy_common.h#L1077
//
// Begin numpy proxy
//
// From:
//   https://raw.githubusercontent.com/numpy/numpy/master/numpy/core/include/numpy/ndarraytypes.h
typedef enum
{
    /* Force signed enum type, must be -1 for code compatibility */
    NPY_FR_ERROR = -1, /* error or undetermined */

    /* Start of valid units */
    NPY_FR_Y = 0, /* Years */
    NPY_FR_M = 1, /* Months */
    NPY_FR_W = 2, /* Weeks */
    /* Gap where 1.6 NPY_FR_B (value 3) was */
    NPY_FR_D       = 4,  /* Days */
    NPY_FR_h       = 5,  /* hours */
    NPY_FR_m       = 6,  /* minutes */
    NPY_FR_s       = 7,  /* seconds */
    NPY_FR_ms      = 8,  /* milliseconds */
    NPY_FR_us      = 9,  /* microseconds */
    NPY_FR_ns      = 10, /* nanoseconds */
    NPY_FR_ps      = 11, /* picoseconds */
    NPY_FR_fs      = 12, /* femtoseconds */
    NPY_FR_as      = 13, /* attoseconds */
    NPY_FR_GENERIC = 14  /* unbound units, can convert to anything */
} NPY_DATETIMEUNIT;

// From:
//   https://raw.githubusercontent.com/numpy/numpy/master/numpy/core/include/numpy/ndarraytypes.h
typedef struct
{
    NPY_DATETIMEUNIT base;
    int num;
} PyArray_DatetimeMetaData;

// numpy uses their own npy_int64, the definition of which I would like to omit (probing
// these things is complex).
//
// For simplicity's sake, we typedef it as a std::int64_t, because the intention is for it
// to be a 64bit int anyway.
//
// See also:
//   https://github.com/numpy/numpy/blob/master/numpy/core/include/numpy/npy_common.h#L1077
typedef std::int64_t npy_datetime;

// From:
//   https://raw.githubusercontent.com/numpy/numpy/master/numpy/core/include/numpy/arrayscalars.h
typedef struct
{
    PyObject_HEAD npy_datetime obval;
    PyArray_DatetimeMetaData obmeta;
} PyDatetimeScalarObject;

// End numpy proxy

namespace py = pybind11;

namespace qdb
{
namespace numpy
{

// Everything below is custom code

namespace detail
{

inline static PyTypeObject * get_datetime64_type() noexcept
{
    // Note that this type is not shipped with pybind by default, and we made
    // local modifications to the pybind/numpy.hpp code to look up this
    // type at runtime.
    return py::detail::npy_api::get().PyDatetimeArrType_;
}

/**
 * Allocate a new numpy.datetime64 python object. This invokes the numpy code
 * dynamically loaded at runtime.
 */
inline static PyDatetimeScalarObject * new_datetime64()
{
    PyTypeObject * type = detail::get_datetime64_type();

    // Allocate memory
    PyDatetimeScalarObject * res = reinterpret_cast<PyDatetimeScalarObject *>(type->tp_alloc(type, 1));

    // Call constructor.
    return PyObject_INIT_VAR(res, type, sizeof(PyDatetimeScalarObject));
}

inline bool PyDatetime64_Check(PyObject * o) {
  // TODO(leon): validate that object is actually a PyDatetime64ScalarObject (how?)
  return true;
}

/**
 * Convert nanoseconds int64 to a numpy datetime. Returns a new reference to a PyObject *.
 */
inline static PyObject * to_datetime64(std::int64_t ts)
{
    PyDatetimeScalarObject * res = detail::new_datetime64();

    res->obmeta.num  = 1;         // refcount ?
    res->obmeta.base = NPY_FR_ns; // our timestamps are always in ns
    res->obval       = ts;

    // Ensure that we create a new reference for the caller
    Py_INCREF(res);

    return reinterpret_cast <PyObject *> (res);
}

} // namespace detail

class datetime64 : public py::object {
public:
  PYBIND11_OBJECT_DEFAULT(datetime64, object, detail::PyDatetime64_Check)

  datetime64(std::int64_t ts)
  : py::object(py::reinterpret_steal<py::object>(detail::to_datetime64(ts)))
  {
  }

  datetime64(const qdb_timespec_t & ts)
    : datetime64(convert_timestamp(ts))
  {
  }
};

} // namespace numpy
} // namespace qdb
