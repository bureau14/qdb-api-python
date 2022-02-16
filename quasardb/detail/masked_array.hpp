/*
 *
 * Official Python API
 *
 * Copyright (c) 2009-2022, quasardb SAS. All rights reserved.
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

#include "../numpy.hpp"

namespace py = pybind11;

namespace qdb
{

namespace detail
{

class masked_array
{
public:
  masked_array() = default;

  // Initialize a masked array with everything open
  masked_array(py::array arr)
    : masked_array(arr, arr)
  {}

  // Initialized from an array and a mask array. Mask
  // array should be with dtype bool. True indicates masked (invisible).
  masked_array(py::array arr, py::array mask)
    : arr_{arr}
    , mask_{mask}
  {}

  // Copy constructor
  masked_array(const masked_array & ma)
    : arr_{ma.arr_}
    , mask_{ma.mask_}
  {}

  py::object cast() const {
    static py::module numpy_ma = py::module::import("numpy.ma");
    static py::object init     = numpy_ma.attr("masked_array");
    return init(arr_, mask_);
  }

  // Initialize a mask array with all items masked. Follows `numpy.ma` interface.
  //
  // Intended to be used as follows:
  //
  //   py::array xs{"datetime64[ns]", 100};
  //   auto ma = masked_array{xs, masked_array::masked_all()};

  static py::array_t<bool> masked_all(py::array::ShapeContainer shape) {
    return qdb::numpy::array::initialize<bool>(shape, true);
  }

  // Initialize a mask array with all items masked. Follows `numpy.ma` interface.
  //
  // Intended to be used as follows:
  //
  //   py::array xs{"datetime64[ns]", 100};
  //   auto ma = masked_array{xs, masked_array::masked_none()};
  static py::array_t<bool> masked_none(py::array::ShapeContainer shape) {
    return qdb::numpy::array::initialize<bool>(shape, false);
  }

private:

  py::array arr_;
  py::array mask_;
};

template <typename Module>
static inline void register_masked_array(Module & m)
{
    namespace py = pybind11;

    py::class_<masked_array>{m, "MaskedArray"};
}


} // namespace detail

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
struct type_caster<qdb::detail::masked_array>
{
public:
    /**
     * Note that this macro magically sets a member variable called 'value'.
     */
    PYBIND11_TYPE_CASTER(qdb::detail::masked_array, _("qdb::detail::masked_array"));

    /**
     * We do not support Python->C++ (yet).
     */
    bool load(handle src, bool) const noexcept
    {
        // How to implement:
        //
        // `handle` would be a numpy.ma.MaskedArray here, and we would just need to read
        // the data and mask.
        //
        // We would then need to set the local `value` member variable, which is automatically
        // created by the PYBIND11_TYPE_CASTER macro.
        return false;
    }

    /**
     * C++->Python
     */
    static py::handle cast(qdb::detail::masked_array src,
                           return_value_policy /* policy */,
                           handle /* parent */)
    {
        py::object ret = src.cast();
        ret.inc_ref();
        return ret.ptr();
    }
};



} // namespace detail

} // namespace pybind11
