/*
 *
 * Official Python API
 *
 * Copyright (c) 2009-2023, quasardb SAS. All rights reserved.
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

#include "concepts.hpp"
#include "error.hpp"
#include "traits.hpp"
#include "qdb/ts.h"
#include <pybind11/chrono.h>
#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <time.h>

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

namespace qdb
{
namespace numpy
{
namespace detail
{
inline std::time_t mkgmtime(std::tm * t) noexcept
{
#ifdef _WIN32
    return _mkgmtime(t);
#else
    return ::timegm(t);
#endif
}

inline std::string to_string(py::dtype const & dt) noexcept
{
    return dt.attr("name").cast<py::str>();
}

inline std::string to_string(py::type const & t) noexcept
{
    return t.cast<py::str>();
}

}; // namespace detail

namespace py = pybind11;

static inline std::int64_t datetime64_to_int64(py::object v)
{
    if (v.is_none())
    {
        throw qdb::invalid_argument_exception{"Unable to convert None object time datetime64"};
    };

    using namespace std::chrono;
    try
    {
        // Starting version 3.8, Python does not allow implicit casting from numpy.datetime64
        // to an int, so we explicitly do it here.
        return v.cast<std::int64_t>();
    }
    catch (py::cast_error const & /*e*/)
    {
        throw qdb::invalid_datetime_exception{v};
    }
}

// Takes a `py::dtype` and converts it to our own internal dtype tag
inline decltype(auto) dtype_object_to_tag(py::dtype dt){

};

// Everything below is custom code
namespace array
{

template <concepts::dtype T>
using value_type_t = typename T::value_type;

/**
 * Ensures that an array matches a certain dtype, raises an exception if not.
 */
template <concepts::dtype T>
py::array ensure(py::array const & xs)
{
    py::dtype dt = xs.dtype();

    if (T::is_dtype(dt) == false) [[unlikely]]
    {
        std::string msg = std::string{"Provided np.ndarray dtype '"} + detail::to_string(dt)
                          + std::string{"' incompatbile with expected dtype '"}
                          + detail::to_string(T::dtype()) + std::string{"'"};
        throw qdb::incompatible_type_exception{msg};
    }

    return xs;
};

template <concepts::dtype T>
[[nodiscard]] py::array ensure(py::handle const & h)
{
    if (py::isinstance<py::array>(h)) [[likely]]
    {
        return ensure<T>(py::array::ensure(h));
    }
    else if (py::isinstance<py::list>(h))
    {
        return ensure<T>(py::cast<py::array>(h));
    }

    throw qdb::incompatible_type_exception{
        "Expected a numpy.ndarray or list, got: " + detail::to_string(py::type::of(h))};
};

/**
 * Fixed width dtypes, length is fixed based on the dtype. This does *not* mean that every
 * dtype has the same width, it can still be that this loop is used for float16 and int64
 * and whatnot.
 */
template <concepts::dtype T>
    requires(concepts::fixed_width_dtype<T>)
inline value_type_t<T> * fill_with_mask(value_type_t<T> const * input,
    bool const * mask,
    std::size_t size,
    std::size_t /* itemsize */,
    value_type_t<T> fill_value,
    value_type_t<T> * dst)
{
    value_type_t<T> const * end = input + size;

    // XXX(leon): *HOT* loop, can we get rid of the conditional branch?
    for (auto cur = input; cur != end; ++cur, ++mask, ++dst)
    {
        *dst = *mask ? fill_value : *cur;
    }

    return dst;
};

/**
 * Variable-length encoding: significantly more tricky, since every array has a different
 * "length" for all items.
 */
template <concepts::dtype T>
    requires(concepts::variable_width_dtype<T>)
inline value_type_t<T> * fill_with_mask(value_type_t<T> const * input,
    bool const * mask,
    std::size_t size,
    std::size_t itemsize,
    value_type_t<T> fill_value,
    value_type_t<T> * dst)
{
    // code_point == 4 for e.g. UTF-32, which implies "4 bytes per char". Because in such a
    // case, we are iterating using a wchar_t (which is already 4 bytes), we need to reduce
    // our "stride" size by this factor.
    std::size_t stride_size = itemsize / T::code_point_size;

    // pre-fill a vector with our fill value, which we will be copying into all the right
    // places into the resulting data.
    std::vector<value_type_t<T>> fill_value_(stride_size, fill_value);

    value_type_t<T> const * cur = input;

    // XXX(leon): *HOT* loop; is there a way to vectorize this stuff, and/or
    //            get rid of some branches?
    //
    //            One simple approach would be to pre-fill the destination array with
    //            our fill value, so that we can get rid of a branch below.
    //
    //            Is there a SIMD version possible here?

    for (std::size_t i = 0; i < size; ++i, cur += stride_size, ++mask, dst += stride_size)
    {
        if (*mask == true)
        {
            // We could probably get away with *just* setting *dst to the fill value, instead
            // of setting the whole range.
            std::copy(std::cbegin(fill_value_), std::cend(fill_value_), dst);
        }
        else
        {
            std::copy(cur, cur + stride_size, dst);
        }
    }

    return dst;
};

template <concepts::dtype T>
inline py::array fill_with_mask(
    py::array const & input, py::array const & mask, value_type_t<T> fill_value)
{
    array::ensure<T>(input);

    py::array::ShapeContainer shape{{input.size()}};
    py::array ret{input.dtype(), shape};

    assert(input.size() == mask.size());
    assert(input.size() == ret.size());

    fill_with_mask<T>(static_cast<value_type_t<T> const *>(input.data()),
        static_cast<bool const *>(mask.data()), static_cast<std::size_t>(input.size()),
        static_cast<std::size_t>(input.itemsize()), fill_value,
        static_cast<value_type_t<T> *>(ret.mutable_data()));
    return ret;
};

template <concepts::dtype T>
static inline py::array fill_with_mask(py::array const & input, py::array const & mask)
{
    return fill_with_mask(input, mask, T::null_value());
};

template <typename ValueType>
static void fill(py::array & xs, ValueType x) noexcept
{
    // For now, don't support multi-dimensional arrays (e.g. matrices) and
    // only plain vanilla arrays. Apart from some additional wrestling with
    // numpy / pybind APIs, it's possible to implement though.
    assert(xs.ndim() == 1);

    std::size_t n = xs.shape(0);

    if (n > 0) [[likely]]
    {
        ValueType * y = xs.mutable_unchecked<ValueType>().mutable_data();
        std::fill(y, y + n, x);
    }
}

// Create empty array, do not fill any values.
template <typename ValueType>
    requires(std::is_trivial_v<ValueType>)
static py::array initialize(py::array::ShapeContainer shape) noexcept
{
    return py::array(py::dtype::of<ValueType>(), shape);
}

// Create empty array, filled with `x`
template <typename ValueType>
    requires(std::is_trivial_v<ValueType>)
static py::array initialize(py::array::ShapeContainer shape, ValueType x) noexcept
{
    py::array xs = initialize<ValueType>(shape);
    fill(xs, x);
    return xs;
}

// Create empty array, do not fill any values.
template <typename ValueType>
    requires(std::is_trivial_v<ValueType>)
static py::array initialize(py::ssize_t size) noexcept
{
    return initialize<ValueType>({size});
}

// Create empty array, filled with `x`
template <typename ValueType>
    requires(std::is_trivial_v<ValueType>)
static py::array initialize(py::ssize_t size, ValueType x) noexcept
{
    return initialize<ValueType>(py::array::ShapeContainer{size}, x);
}

template <concepts::dtype T>
static py::array initialize(py::array::ShapeContainer shape, value_type_t<T> x) noexcept
{
    py::array xs = py::array(T::dtype(), shape);
    fill(xs, x);

    return xs;
}

template <concepts::dtype D>
static py::array initialize(py::ssize_t size, value_type_t<D> x) noexcept
{
    return initialize<D>(py::array::ShapeContainer{size}, x);
}

template <concepts::dtype T>
    requires(concepts::fixed_width_dtype<T>)
py::array of_list(py::list xs)
{
    using value_type = typename T::value_type;
    std::array<py::ssize_t, 1> shape{{static_cast<py::ssize_t>(xs.size())}};
    py::array xs_(T::dtype(), shape);
    value_type * xs__ = xs_.mutable_unchecked<value_type>().mutable_data();

    std::transform(std::begin(xs), std::end(xs), xs__, [](py::handle x) -> value_type {
        if (x.is_none())
        {
            return T::null_value();
        }
        else
        {
            return py::cast<value_type>(x);
        };
    });

    return ensure<T>(xs_);
};

template <concepts::dtype T>
    requires(concepts::fixed_width_dtype<T>)
std::pair<py::array, py::array> of_list_with_mask(py::list xs)
{
    std::array<py::ssize_t, 1> shape{{static_cast<py::ssize_t>(xs.size())}};

    py::array data = of_list<T>(xs);
    py::array mask = py::array{py::dtype::of<bool>(), shape};
    bool * mask_   = static_cast<bool *>(mask.mutable_data());

    auto is_none = [](py::handle x) -> bool { return x.is_none(); };

    std::transform(std::begin(xs), std::end(xs), mask_, is_none);

    return std::make_pair(data, mask);
};

} // namespace array

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
    assert(type != nullptr);

    // Allocate memory
    PyObject * res = type->tp_alloc(type, 1);
    // Call constructor.

    // TODO(leon): this _might_ not be strictly necessary, as there might
    // be a better way to allocate this object.
#if PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION < 8
    PyObject * tmp = PyObject_INIT_VAR(res, type, sizeof(PyDatetimeScalarObject));
#else
    PyVarObject * tmp = PyObject_INIT_VAR(res, type, sizeof(PyDatetimeScalarObject));
#endif

    return reinterpret_cast<PyDatetimeScalarObject *>(tmp);
}

inline bool PyDatetime64_Check(PyObject * o)
{
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

    return reinterpret_cast<PyObject *>(res);
}

} // namespace detail

class datetime64 : public py::object
{
public:
    PYBIND11_OBJECT_DEFAULT(datetime64, object, detail::PyDatetime64_Check)

    explicit datetime64(std::int64_t ts)
        : py::object(py::reinterpret_steal<py::object>(detail::to_datetime64(ts)))
    {}

    explicit datetime64(qdb_timespec_t const & ts);
};

} // namespace numpy
} // namespace qdb
