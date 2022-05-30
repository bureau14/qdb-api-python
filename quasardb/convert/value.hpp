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

#include "../concepts.hpp"
#include "../error.hpp"
#include "../object_tracker.hpp"
#include "../traits.hpp"
#include <qdb/ts.h>
#include <pybind11/pybind11.h>
#include <range/v3/range/concepts.hpp>
#include <range/v3/view/counted.hpp>
#include <cstring>
#include <utf8.h>

namespace qdb::convert::detail
{

namespace py = pybind11;
typedef std::remove_cvref<decltype(qdb_string_t::data[0])>::type qdb_char_type;

////////////////////////////////////////////////////////////////////////////////
//
// VALUE CONVERTERS
//
///////////////////
//
// These converters operate on individual values, with various degrees of
// complexity.
//
////////////////////////////////////////////////////////////////////////////////

template <typename From, typename To>
struct value_converter;

template <typename From, typename To>
requires(std::is_same_v<From, To>) struct value_converter<From, To>
{
    inline To operator()(From const & x) const
    {
        static_assert(sizeof(To) >= sizeof(From));
        // Default implementation for "simple" conversions allowed by the compiler,
        // e.g. int32 to int64.
        return x;
    }
};

template <typename To>
struct value_converter<traits::int64_dtype, To> : public value_converter<std::int64_t, To>
{};

template <typename To>
struct value_converter<traits::int32_dtype, To> : public value_converter<std::int32_t, To>
{};

template <typename To>
struct value_converter<traits::int16_dtype, To> : public value_converter<std::int16_t, To>
{};

template <typename From>
struct value_converter<From, traits::int64_dtype> : public value_converter<From, std::int64_t>
{};

template <typename From>
struct value_converter<From, traits::int32_dtype> : public value_converter<From, std::int32_t>
{};

template <typename From>
struct value_converter<From, traits::int16_dtype> : public value_converter<From, std::int16_t>
{};

template <typename To>
struct value_converter<traits::float64_dtype, To> : public value_converter<double, To>
{};

template <typename To>
struct value_converter<traits::float32_dtype, To> : public value_converter<float, To>
{};

template <typename From>
struct value_converter<From, traits::float64_dtype> : public value_converter<From, double>
{};

template <typename From>
struct value_converter<From, traits::float32_dtype> : public value_converter<From, float>
{};

template <typename From>
struct value_converter<From, traits::pyobject_dtype> : public value_converter<From, py::object>
{};

template <>
struct value_converter<std::int64_t, qdb_timespec_t>
{
    inline constexpr qdb_timespec_t operator()(std::int64_t const & x) const
    {
        if (x < 0) [[unlikely]]
        {
            return qdb_timespec_t{qdb_min_time, qdb_min_time};
        }

        constexpr std::int64_t ns = 1'000'000'000ull;
        std::int64_t tv_nsec      = x % ns;
        std::int64_t tv_sec       = (x - tv_nsec) / ns;

        return qdb_timespec_t{tv_sec, tv_nsec};
    }
};

template <>
struct value_converter<qdb_timespec_t, std::int64_t>
{
    inline std::int64_t operator()(qdb_timespec_t const & x) const
    {
        // XXX(leon): potential overflow
        return x.tv_nsec + x.tv_sec * 1'000'000'000ull;
    }
};

template <>
struct value_converter<qdb_timespec_t, traits::datetime64_ns_dtype>
    : public value_converter<qdb_timespec_t, std::int64_t>
{};

template <>
struct value_converter<traits::datetime64_ns_dtype, qdb_timespec_t>
    : public value_converter<std::int64_t, qdb_timespec_t>
{};

template <>
struct value_converter<traits::bytestring_dtype, qdb_string_t>
{
    using char_t = std::string::value_type;

    template <concepts::input_range_t<char_t> R>
    requires(ranges::sized_range<R> && ranges::contiguous_range<R>) inline qdb_string_t operator()(
        R && x) const
    {
        std::size_t n     = (ranges::size(x) + 1) * sizeof(char_t);
        char_t const * x_ = ranges::data(x);
        char_t * tmp      = qdb::object_tracker::alloc<char_t>(n);

        std::memcpy((void *)(tmp), x_, ranges::size(x) + 1);
        return qdb_string_t{tmp, ranges::size(x)};
    }
};

template <>
struct value_converter<traits::unicode_dtype, qdb_string_t>
{
    typedef std::u32string::value_type in_char_type;
    typedef qdb_char_type out_char_type;

    template <concepts::input_range_t<in_char_type> R>
    requires(ranges::sized_range<R> && ranges::contiguous_range<R>) inline qdb_string_t operator()(
        R && x) const
    {
        std::size_t n_codepoints  = ranges::size(x);
        std::size_t max_bytes_out = n_codepoints * sizeof(in_char_type);

        out_char_type * data = qdb::object_tracker::alloc<out_char_type>(max_bytes_out);
        out_char_type * end  = utf8::utf32to8(ranges::begin(x), ranges::end(x), data);

        qdb_size_t n = static_cast<qdb_size_t>(std::distance(data, end));

        return qdb_string_t{data, n};
    }
};

template <>
struct value_converter<py::bytes, qdb_blob_t>
{
    using dtype = traits::object_dtype<py::bytes>;

    inline qdb_blob_t operator()(py::bytes const & x) const
    {
        assert(dtype::is_null(x) == false);
        assert(dtype::is_null(x) == x.is_none());

        qdb_blob_t ret{nullptr, 0};

        if (PYBIND11_BYTES_AS_STRING_AND_SIZE(
                x.ptr(), (char **)(&ret.content), (Py_ssize_t *)(&ret.content_length))) [[unlikely]]
        {
            throw qdb::incompatible_type_exception{"Unable to interpret object as bytes and size: "};
        }

        return ret;
    }
};

template <>
struct value_converter<traits::pyobject_dtype, qdb_blob_t>
    : public value_converter<py::bytes, qdb_blob_t>
{};

template <>
struct value_converter<qdb_blob_t, py::bytes>
{
    inline py::bytes operator()(qdb_blob_t const & x) const
    {
        // Again, if we're already at the point that we're sure we can cast it to py::bytes,
        // it implies it is guaranteed not to be null.
        assert(traits::is_null(x) == false);

        return py::bytes(static_cast<char const *>(x.content), x.content_length);
    }
};

template <>
struct value_converter<qdb_blob_t, py::object>
{
    using dtype = traits::pyobject_dtype;

    value_converter<qdb_blob_t, py::bytes> delegate_{};

    inline py::object operator()(qdb_blob_t const & x) const
    {
        if (traits::is_null(x))
        {
            return dtype::null_value();
        }

        return delegate_(x);
    }
};

template <>
struct value_converter<qdb_string_t, py::str>
{
    inline py::str operator()(qdb_string_t const & x) const
    {
        // Again, if we're already at the point that we're sure we can cast it to py::bytes,
        // it implies it is guaranteed not to be null.
        assert(traits::is_null(x) == false);

        return py::str(x.data, x.length);
    }
};

template <>
struct value_converter<qdb_string_t, py::object>
{
    using dtype = traits::pyobject_dtype;

    value_converter<qdb_string_t, py::str> delegate_{};

    inline py::object operator()(qdb_string_t const & x) const
    {
        if (traits::is_null(x))
        {
            return dtype::null_value();
        }

        return delegate_(x);
    }
};

using qdb_string_view = ranges::counted_view<qdb_char_type const *>;

template <>
struct value_converter<qdb_string_t, qdb_string_view>
{
    inline qdb_string_view operator()(qdb_string_t const & x) const
    {
        return qdb_string_view(x.data, static_cast<std::size_t>(x.length));
    }
};

template <>
struct value_converter<py::object, qdb_blob_t>
{
    using dtype = traits::object_dtype<py::object>;

    value_converter<py::bytes, qdb_blob_t> delegate_{};

    inline qdb_blob_t operator()(py::object const & x) const
    {
        if (dtype::is_null(x))
        {
            return traits::null_value<qdb_blob_t>();
        }

        return delegate_(x);
    }
};

template <>
struct value_converter<py::object, qdb_timespec_t>
{
    using dtype = traits::object_dtype<py::object>;

    value_converter<std::int64_t, qdb_timespec_t> delegate_{};

    inline qdb_timespec_t operator()(py::object const & x) const
    {
        if (dtype::is_null(x))
        {
            return traits::null_value<qdb_timespec_t>();
        }

        try
        {
            return delegate_(x.cast<std::int64_t>());
        }
        catch (py::cast_error const & /* e */)
        {
            throw qdb::invalid_datetime_exception{x};
        }
    }
};

template <>
struct value_converter<py::tuple, qdb_ts_range_t>
{
    using dtype = traits::object_dtype<py::object>;

    value_converter<py::object, qdb_timespec_t> delegate_{};

    inline qdb_ts_range_t operator()(py::tuple const & x) const
    {
        if (x.is_none()) [[unlikely]]
        {
            throw qdb::invalid_argument_exception{
                std::string{"Expected a Tuple of datetime, got None"}};
        }
        else if (x.size() != 2) [[unlikely]]
        {
            throw qdb::invalid_argument_exception{
                std::string{"A time range should be a Tuple with 2 datetimes, got "
                            + std::to_string(x.size()) + " items in tuple"}};
        }

        qdb_timespec_t begin = delegate_(x[0]);
        qdb_timespec_t end   = delegate_(x[1]);

        return qdb_ts_range_t{begin, end};
    }
};

template <>
struct value_converter<traits::bytestring_dtype, qdb_blob_t>
{
    value_converter<traits::bytestring_dtype, qdb_string_t> delegate_{};

    template <ranges::input_range R>
    inline qdb_blob_t operator()(R && x) const
    {
        qdb_string_t s = delegate_(std::forward<R &&>(x));

        return qdb_blob_t{static_cast<void const *>(s.data), s.length};
    }
};
}; // namespace qdb::convert::detail

namespace qdb::convert
{

////////////////////////////////////////////////////////////////////////////////
//
// PUBLIC API
//
///////////////////
//
// Functions below define the public API. Their intent to handle boilerplate and
// and easier-to-use interface than the lower-level converters defined above.
//
////////////////////////////////////////////////////////////////////////////////

// any -> any
template <typename From, typename To>
static inline constexpr To value(From const & x)
{
    detail::value_converter<From, To> c{};
    return c(x);
}

}; // namespace qdb::convert
