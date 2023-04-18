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
#include "numpy.hpp"
#include "traits.hpp"
#include <pybind11/numpy.h>
#include <pybind11/stl_bind.h>
#include <range/v3/algorithm/for_each.hpp>
#include <range/v3/view/chunk.hpp>
#include <range/v3/view/counted.hpp>

namespace qdb::detail
{

enum mask_probe_t
{
    mask_unknown   = 0,
    mask_all_true  = 1 << 0,
    mask_all_false = 1 << 1,
    mask_mixed     = mask_all_true | mask_all_false
};

/**
 * Efficiently probes one chunk, without returning early.
 */
template <typename Rng>
inline std::uint8_t probe_chunk(Rng const & xs) noexcept
{
    std::uint8_t state{static_cast<std::uint8_t>(mask_unknown)};

    // XXX(leon): Super hot code path, but it's auto-vectorized which makes it
    //            faster than any alternative (including reinterpreting them
    //            as 64-bit integers).
    ranges::for_each(xs, [&state](bool x) -> void {
        state |=
            (x ? static_cast<std::uint8_t>(mask_all_true) : static_cast<std::uint8_t>(mask_all_false));
    });

    return state;
}

template <typename Rng>
inline enum qdb::detail::mask_probe_t probe_mask(Rng const & xs) noexcept
{
    // We don't accept empty arrays
    assert(ranges::size(xs) > 0);

    // In order for auto-vectorization to work, we use an outer loop (this function)
    // which divides work into chunks of 256 booleans; these are then processed as
    // one work unit.
    //
    // The outer loop checks whether we already have a mixed mask, and shortcuts when
    // that's the case.
    //
    // This ensures that, if we're dealing with large, mixed masked, we scan only a
    // fraction of it.
    constexpr std::size_t chunk_size = 256; // not chosen scientifically
    std::uint8_t state               = mask_unknown;

    // Interpret the booleans as a range with `chunk_size` chunks of data.
    auto rng = xs | ranges::views::chunk(chunk_size);

    for (auto chunk : rng)
    {
        state |= probe_chunk(chunk);

        if (state == mask_mixed)
        {
            // Exit early if we have found mixed data.
            break;
        }
    }

    assert(0 < state && state <= 3);
    return static_cast<mask_probe_t>(state);
}

inline enum qdb::detail::mask_probe_t probe_mask(bool const * xs, std::size_t n) noexcept
{
    return probe_mask(ranges::views::counted(xs, n));
};

inline enum mask_probe_t probe_mask(py::array const & xs) noexcept
{
    assert(xs.dtype().kind() == 'b');
    bool const * xs_ = xs.unchecked<bool>().data();
    return probe_mask(xs_, xs.size());
};

template <bool v>
constexpr mask_probe_t probe_of_bool();

template <>
constexpr inline mask_probe_t probe_of_bool<true>()
{
    return mask_all_true;
};

template <>
constexpr inline mask_probe_t probe_of_bool<false>()
{
    return mask_all_false;
};

constexpr inline mask_probe_t probe_of_bool(bool b)
{
    return (b == true ? probe_of_bool<true>() : probe_of_bool<false>());
};

template <mask_probe_t v>
constexpr bool bool_of_probe();

template <>
constexpr inline bool bool_of_probe<mask_all_true>()
{
    return true;
};

template <>
constexpr inline bool bool_of_probe<mask_all_false>()
{
    return false;
};

constexpr inline bool bool_of_probe(mask_probe_t p)
{
    switch (p)
    {
    case mask_all_true:
        return true;
    case mask_all_false:
        return false;
    default:
        throw qdb::internal_local_exception(
            "Mask probe not convertible to boolean: " + std::to_string(p));
    };
};

}; // namespace qdb::detail

namespace qdb
{
namespace py = pybind11;

class mask
{
public:
    /**
     * Default constructor required because objects of this class are wrapped
     * inside masked_array, and py::cast needs default constructors to work.
     */
    inline mask() noexcept {};

    inline mask(mask const & o) noexcept
        : xs_{o.xs_}
        , probe_{o.probe_} {};

    inline mask(mask && o) noexcept
        : xs_{std::move(o.xs_)}
        , probe_{std::move(o.probe_)} {};

    inline mask(py::array const & xs, detail::mask_probe_t probe) noexcept
        : xs_{xs}
        , probe_{probe} {};

    inline mask & operator=(mask const & o) noexcept
    {
        xs_    = o.xs_;
        probe_ = o.probe_;
        return *this;
    };

    static inline mask of_array(py::array const & xs) noexcept
    {
        return mask{xs, detail::probe_mask(xs)};
    };

    inline bool load(py::array const & xs) noexcept
    {
        return load(xs, detail::probe_mask(xs));
    }

    inline bool load(py::array const & xs, detail::mask_probe_t probe) noexcept
    {
        if (xs.size() == 0) [[unlikely]]
        {
            return false;
        };

        xs_    = xs;
        probe_ = probe;
        return true;
    }
    /**
     * Initialize a mask of size `n` with all values set to `true` (everything
     * masked, i.e. everything hidden).
     */
    template <bool v>
    static inline mask of_all(py::ssize_t n) noexcept
    {
        assert(n > 0);
        return mask{numpy::array::initialize(n, v), detail::probe_of_bool(v)};
    };

    /**
     * Initialize a mask of size `n` with all values set to `true`.
     */
    template <detail::mask_probe_t p>
    static inline mask of_probe(py::ssize_t n) noexcept
    {
        assert(n > 0);
        return mask{numpy::array::initialize(n, detail::bool_of_probe<p>()), p};
    };

    /**
     * Returns amount of elemenbts in the mask.
     */
    py::ssize_t size() const noexcept
    {
        return xs_.size();
    };

    py::array const & array() const noexcept
    {
        return xs_;
    };

    bool const * data() const noexcept
    {
        return xs_.unchecked<bool>().data();
    };

    bool * mutable_data() noexcept
    {
        return xs_.mutable_unchecked<bool>().mutable_data();
    };

    detail::mask_probe_t probe() const noexcept
    {
        return probe_;
    };

private:
    py::array xs_;
    detail::mask_probe_t probe_;
};

/**
 * Masked array that can hold any type of data. Types are templated in the member
 * functions that require them, and no type checking is done upon initialization.
 */
class masked_array
{
public:
    using ShapeContainer = typename py::array::ShapeContainer;

public:
    // Default constructor 'deleted' but necessary for py::cast
    masked_array() = default;

    // Initialize a masked array with everything open
    explicit masked_array(py::array arr)
        : masked_array(arr, qdb::mask::of_probe<detail::mask_all_false>(arr.size()))
    {}

    // Initialized from an array and a mask array. Mask array should be with dtype bool.
    // Automatically 'probes' the mask to find all-true / all-false patterns.
    masked_array(py::array arr, py::array mask)
        : masked_array{arr, mask::of_array(mask)}
    {}

    // Initialized from an array and a mask array. Mask
    // array should be with dtype bool. True indicates masked (invisible).
    masked_array(py::array arr, qdb::mask mask)
        : logger_("quasardb.masked_array")
        , arr_{arr}
        , mask_{mask}
    {
        assert(arr.size() == mask.size());
    }

    // Copy constructor
    masked_array(const masked_array & ma)
        : arr_{ma.arr_}
        , mask_{ma.mask_}
    {}

    py::handle cast(py::return_value_policy /* policy */) const
    {
        py::module numpy_ma = py::module::import("numpy.ma");
        py::object init     = numpy_ma.attr("masked_array");

        return init(arr_, mask_.array()).inc_ref();
    }

    py::array data() const
    {
        return arr_;
    }

    qdb::mask const & mask() const
    {
        return mask_;
    }

    inline bool load(py::handle src)
    {
        if (masked_array::check(src)) [[likely]]
        {
            // This is an actual numpy.ma.array
            logger_.debug("loading masked array from numpy.ma.MaskedArray object");
            return load(src.attr("data"), src.attr("mask"));
        }
        else if (py::isinstance<py::array>(src))
        {
            logger_.debug(
                "initializing quasardb.masked_array from numpy.ndarray with size %d", arr_.size());
            py::array src_ = py::cast<py::array>(src);
            return load(src_, mask::of_all<false>(src_.size()));
        }

        return false;
    }

    inline bool load(py::array arr, py::array mask)
    {
        if (arr.size() != mask.size())
        {
            logger_.warn("array[%d] and mask[%d] not of identical size", arr.size(), mask.size());
        };
        assert(arr.size() == mask.size());

        arr_ = py::array::ensure(arr);
        if (mask_.load(mask) == false)
        {
            logger_.warn("unable to load mask");
            return false;
        };

        return true;
    }

    inline bool load(py::array arr, qdb::mask mask)
    {
        assert(arr.size() == mask.size());

        arr_  = py::array::ensure(arr);
        mask_ = mask;

        return true;
    }

    bool load(std::pair<py::array, py::array> const & src)
    {
        return load(std::get<0>(src), std::get<1>(src));
    }

    /**
     * Returns `true` if handle is of a masked array type.
     */
    static bool check(py::handle x)
    {
        // XXX(leon): perhaps there's a higher performance way to check the
        //            type, such as acquiring a reference to the type of
        //            `numpy.MaskedArray` and invoking py::isinstance().
        //
        //            but this path is not very performance critical.
        py::module numpy_ma = py::module::import("numpy.ma");
        py::object isMA     = numpy_ma.attr("isMaskedArray");

        return py::cast<bool>(isMA(x));
    }

    inline py::dtype dtype() const
    {
        return arr_.dtype();
    }

    inline std::size_t size() const noexcept
    {
        assert(arr_.size() == mask_.size());
        return arr_.size();
    }

    inline ShapeContainer shape() const noexcept
    {
        return ShapeContainer{{size()}};
    }

    /**
     * Return a regular numpy array with the masked values "filled" with
     * the provided value.
     *
     * The behavior mirrors the numpy.ma.filled function, and aligns fairly
     * well with how QuasarDB wants data to be shaped.
     *
     * This specific implementation is for dtypes with fixed length, which
     * allows us to use "fast" point-based copies.
     */
    template <concepts::dtype T>
    inline py::array filled(typename T::value_type const & fill_value) const
    {
        assert(arr_.size() == mask_.size());

        py::array ret;

        switch (mask_.probe())
        {
        case detail::mask_all_true:
            // Everything masked: which can just initialize a whole array with fill_value
            // and call it a day.
            return qdb::numpy::array::initialize<T>(arr_.size(), fill_value);
            break;

        case detail::mask_all_false:
            // Fast(est) path: nothing masked, which implies it's identical to arr_
            return arr_;

        case detail::mask_mixed:
            return qdb::numpy::array::fill_with_mask<T>(arr_, mask_.array(), fill_value);
        case detail::mask_unknown:
            // This  is an internal error: mask should always be probed (as it's probed
            // in the constructor).
            //
            // Only condition would be when masked_array is default-constructed and
            // not initialized.
            throw qdb::internal_local_exception{"Mask probe is unknown, masked array not initialized?"};

        default:
            throw qdb::internal_local_exception{
                "Mask probe is corrupted: not a known value: " + std::to_string(mask_.probe())};
        };
    }

    template <concepts::dtype T>
    inline py::array filled() const
    {
        return filled<T>(T::null_value());
    }

    static inline masked_array masked_all(py::array xs)
    {
        return masked_array{xs, mask::of_all<true>(xs.size())};
    };

    static inline masked_array masked_none(py::array xs)
    {
        return masked_array{xs, mask::of_all<false>(xs.size())};
    };

    // Initialize an array mask from a regular array and a "null" value.
    //
    // Intended to be used as follows:
    //
    //   py::array xs{"float64", 100};
    //   // .. fill data, some of them NaN .. //
    //   auto ma = masked_array{xs, masked_array::masked_null(xs)};

    template <concepts::dtype Dtype>
    static qdb::mask masked_null(py::array const & xs)
    {
        using value_type = typename Dtype::value_type;

        py::array_t<bool> ret{ShapeContainer{xs.size()}};
        bool * p_ret = static_cast<bool *>(ret.mutable_data());

        // The step_size is `1` for all fixed-width dtypes, but in case
        // of variable width dtypes, is, well, variable.
        py::ssize_t step_size = Dtype::stride_size(xs.itemsize());

        value_type const * begin = static_cast<value_type const *>(xs.data());
        value_type const * end   = begin + (xs.size() * step_size);

        // TODO(leon): [perf] we should be able to determine the mask_probe_t while
        //             we iterate over this array.
        for (value_type const * cur = begin; cur != end; cur += step_size, ++p_ret)
        {
            *p_ret = Dtype::is_null(*cur);
        };

        return mask::of_array(py::cast<py::array>(ret));
    }

protected:
    qdb::logger logger_;
    py::array arr_;
    qdb::mask mask_;
};

/**
 * "Typed" masked array, like py::array / py::array_t, except in our case our 'type'
 * is our dtype dispatch tag.
 *
 * IMPORTANT: we do *not* have a virtual destructor, this is fine because we do not
 *            have any member objects.
 */
template <concepts::dtype T>
class masked_array_t : public masked_array
{
public:
    bool load(py::handle src)
    {
        if (masked_array::load(src) == true) [[likely]]
        {
            return true;
        }

        if (py::isinstance<py::list>(src))
        {
            logger_.warn("Converting list to masked array: this is a very expensive operation. If you "
                         "are having performance issues, "
                         "consider using numpy.ndarray instead..");

            // Convert list to numpy array and try again.
            //
            // The reason we can do this, is that because we have the "additional" context
            // in masked_array_t of knowing what dtype we're looking for, we can also
            // reasonably cast a list to that type; otherwise you'll end up with a numpy
            // array of objects, which is very much meh.
            return masked_array::load(numpy::array::of_list_with_mask<T>(py::cast<py::list>(src)));
        }

        return false;
    }

    static py::array masked_null(py::array const & xs)
    {
        return masked_array::masked_null<T>(xs);
    };

    inline py::array filled(typename T::value_type const & fill_value) const
    {
        return masked_array::filled<T>(fill_value);
    };

    inline py::array filled() const
    {
        return masked_array::filled<T>(T::null_value());
    };
};

template <typename Module>
static inline void register_masked_array(Module & m)
{
    namespace py = pybind11;

    py::class_<masked_array>{m, "MaskedArray"};
}

} // namespace qdb

namespace pybind11::detail
{

/**
 * Implements custom type caster for our ts_value class, so that conversion
 * to and from native python types is completely transparent.
 */
template <>
struct type_caster<qdb::masked_array>
{
public:
    /**
     * Note that this macro magically sets a member variable called 'value'.
     */
    PYBIND11_TYPE_CASTER(qdb::masked_array, _("numpy.ma.MaskedArray"));

    /**
     * We do not support Python->C++ (yet).
     */
    bool load(py::handle src, bool)
    {
        return value.load(src);
    }

    /**
     * C++->Python
     */
    static py::handle cast(qdb::masked_array && src, return_value_policy policy, handle /* parent */)
    {
        return src.cast(policy);
    }
};

template <qdb::concepts::dtype T>
struct type_caster<qdb::masked_array_t<T>>
{
public:
    using type = qdb::masked_array_t<T>;
    PYBIND11_TYPE_CASTER(type, _("numpy.ma.MaskedArray<T>"));

    bool load(py::handle src, bool)
    {
        return value.load(src);
    };

    static py::handle cast(type && src, return_value_policy policy, handle /* parent */)
    {
        return src.cast(policy);
    }
};

} // namespace pybind11::detail
