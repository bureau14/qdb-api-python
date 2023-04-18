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

#include "../concepts.hpp"
#include "../error.hpp"
#include "../object_tracker.hpp"
#include "../pytypes.hpp"
#include "../traits.hpp"
#include "unicode.hpp"
#include <qdb/ts.h>
#include <date/date.h> // We cannot use <chrono> until we upgrade to at least GCC11 (ARM).
#include <pybind11/pybind11.h>
#include <range/v3/algorithm/copy.hpp>
#include <range/v3/algorithm/for_each.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/view/counted.hpp>
#include <chrono>
#include <cstring>

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
    requires(std::is_same_v<From, To>)
struct value_converter<From, To>
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

////////////////////////////////////////////////////////////////////////////////
//
// qdb_timespec_t converters
//
///////////////////
//
// These converters focus on converting qdb_timespec_t to/from other types.
//
/////

using clock_t = std::chrono::system_clock;

// Explicitly specifying the durations here, rather than relying on type
// inference of the integer type, avoids pitfalls like Python using an
// `int` to represent seconds which isn't enough for chrono.
using nanoseconds_t  = std::chrono::duration<std::int64_t, std::nano>;
using microseconds_t = std::chrono::duration<std::int64_t, std::micro>;
using milliseconds_t = std::chrono::duration<std::int64_t, std::milli>;
using seconds_t      = std::chrono::duration<std::int64_t>;
using minutes_t      = std::chrono::duration<std::int32_t, std::ratio<60>>;
using hours_t        = std::chrono::duration<std::int32_t, std::ratio<3600>>;
using days_t         = std::chrono::duration<std::int32_t, std::ratio<86400>>;
using weeks_t        = std::chrono::duration<std::int32_t, std::ratio<604800>>;
using months_t       = std::chrono::duration<std::int32_t, std::ratio<2629746>>;
using years_t        = std::chrono::duration<std::int32_t, std::ratio<31556952>>;

/**
 * datetime.timedelta -> std::chrono::duration
 *
 * Useful for converting a datetime timezone offset to a chrono duration, among others.
 */
template <>
struct value_converter<qdb::pytimedelta, clock_t::duration>
{
    inline std::chrono::system_clock::duration operator()(pytimedelta const & x) const
    {
        assert(x.is_none() == false);

        static_assert(sizeof(decltype(x.days())) <= sizeof(days_t::rep));
        static_assert(sizeof(decltype(x.seconds())) <= sizeof(seconds_t::rep));
        static_assert(sizeof(decltype(x.microseconds())) <= sizeof(microseconds_t::rep));

        return days_t{x.days()} + seconds_t{x.seconds()} + microseconds_t{x.microseconds()};
    }
};

/**
 * datetime.datetime -> std::chrono::time_point
 *
 * Takes the input datetime, and converts it to a time point. ensures that the timezone
 * offset of datetime is taken into account, and output time_point is in UTC.
 */

template <>
struct value_converter<qdb::pydatetime, clock_t::time_point>
{
    value_converter<qdb::pytimedelta, clock_t::duration> offset_convert_{};

    inline clock_t::time_point operator()(qdb::pydatetime const & x) const

    {
        // Construct the date
        date::year_month_day ymd{
            date::year{x.year()}, date::month{(unsigned)x.month()}, date::day{(unsigned)x.day()}};

        // Calculate the number of days since epoch
        date::sys_days days_since_epoch{ymd};

        static_assert(sizeof(decltype(x.hour())) <= sizeof(hours_t::rep));
        static_assert(sizeof(decltype(x.second())) <= sizeof(seconds_t::rep));
        static_assert(sizeof(decltype(x.microsecond())) <= sizeof(microseconds_t::rep));

        // Calculate the time of day as a duration
        clock_t::duration time_of_day{hours_t{x.hour()} + minutes_t{x.minute()} + seconds_t{x.second()}
                                      + microseconds_t{x.microsecond()}};

        // Adjust for UTC
        clock_t::duration tz_offset = offset_convert_(x.utcoffset());

        // Compose the whole thing together
        return clock_t::time_point(days_since_epoch) + time_of_day - tz_offset;
    }
};

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

/**
 * chrono time_point -> qdb_timespec_t
 *
 * First converts timepoint to nanos since epoch, then delegates to another converter.
 */
template <>
struct value_converter<clock_t::time_point, qdb_timespec_t>
{
    value_converter<std::int64_t, qdb_timespec_t> delegate_{};

    inline constexpr qdb_timespec_t operator()(clock_t::time_point const & x) const
    {
        auto nanos = std::chrono::duration_cast<nanoseconds_t>(x.time_since_epoch());

        return delegate_(nanos.count());
    }
};

/**
 * clock_t::time_point -> qdb_time_t
 *
 * Returns the qdb_time_t representation of a time_point; qdb_time_t is assumed
 * to be using milliseconds.
 */
template <>
struct value_converter<clock_t::time_point, qdb_time_t>
{
    inline qdb_time_t operator()(clock_t::time_point const & x) const
    {
        auto time_since_epoch = x.time_since_epoch();

        return static_cast<qdb_time_t>(
            std::chrono::duration_cast<milliseconds_t>(time_since_epoch).count());
    }
};

/**
 * datetime.datetime -> qdb_time_t
 */
template <>
struct value_converter<qdb::pydatetime, qdb_time_t>
{
    value_converter<qdb::pydatetime, clock_t::time_point> dt_to_tp_{};
    value_converter<clock_t::time_point, qdb_time_t> tp_to_qt_{};

    inline qdb_time_t operator()(pydatetime const & x) const
    {
        if (x.is_none())
        {

            return qdb_time_t{0};
        }
        else
        {
            return tp_to_qt_(dt_to_tp_(x));
        }
    }
};

/**
 * qdb_timespec_t -> std::chrono::time_point
 */

template <>
struct value_converter<qdb_timespec_t, clock_t::time_point>
{
    inline clock_t::time_point operator()(qdb_timespec_t const & x) const
    {
        // We *could* feed chrono the nanoseconds_t directly, but:
        // - python is not able to represent nanoseconds;
        // - some architectures are unable to represent the system_clock with
        //   nanosecond precision; it requires some pretty big integers.
        //
        // As such, let's first truncate things to milliseconds
        milliseconds_t millis{x.tv_nsec / 1'000'000};
        seconds_t seconds{x.tv_sec};

        return clock_t::time_point(millis + seconds);
    }
};

/**
 * std::chrono::time_point -> datetime.datetime
 *
 * time point is assumed to be UTC.
 */
template <>
struct value_converter<clock_t::time_point, qdb::pydatetime>
{
    inline qdb::pydatetime operator()(clock_t::time_point const & tp) const
    {
        date::sys_days dp = date::floor<days_t>(tp);
        date::year_month_day ymd{dp};
        date::hh_mm_ss hms{date::floor<seconds_t>(tp - dp)};

        // We get the 'microseconds' part by simply calculating the total amount of seconds since
        // epoch, and then substracting that from the time point; whatever is left, is guaranteed
        // to be the fraction after the second.
        //
        // Similar appproach as here: https://stackoverflow.com/a/27137475

        auto since_epoch = tp.time_since_epoch();
        auto seconds     = std::chrono::duration_cast<seconds_t>(since_epoch);
        since_epoch -= seconds;

        // Round it to microseconds, because that's what pydatetime uses as max precision
        auto micros = std::chrono::duration_cast<microseconds_t>(since_epoch);

        return qdb::pydatetime::from_date_and_time(static_cast<int>(ymd.year()),
            static_cast<unsigned>(ymd.month()), static_cast<unsigned>(ymd.day()), static_cast<int>(hms.hours().count()),
            static_cast<int>(hms.minutes().count()), static_cast<int>(hms.seconds().count()), static_cast<int>(micros.count()));
    }
};

/**
 * qdb_timespec_t -> datetime.datetime
 *
 * composes two converters to convert a timespec into a datetime.datetime object in one
 * swoop:
 *
 * - first convert the qdb_timespec_t to a (utc) time point;
 * - use the utc time point to create a datetime object
 */
template <>
struct value_converter<qdb_timespec_t, qdb::pydatetime>
{
    value_converter<qdb_timespec_t, clock_t::time_point> ts_to_tp_{};
    value_converter<clock_t::time_point, qdb::pydatetime> tp_to_dt_{};

    inline qdb::pydatetime operator()(qdb_timespec_t const & x) const
    {
        return tp_to_dt_(ts_to_tp_(x));
    }
};

/**
 * datetime.datetime -> qdb_timespec_t
 *
 * composes two converters to convert a datetime.datetime into a timespec in one
 * swoop.
 */
template <>
struct value_converter<qdb::pydatetime, qdb_timespec_t>
{
    value_converter<qdb::pydatetime, clock_t::time_point> dt_to_tp_{};
    value_converter<clock_t::time_point, qdb_timespec_t> tp_to_ts_{};

    inline qdb_timespec_t operator()(qdb::pydatetime const & x) const
    {
        return tp_to_ts_(dt_to_tp_(x));
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

////////////////////////////////////////////////////////////////////////////////
//
// qdb_blob_t/qdb_string_t converters
//
///////////////////
//
// These converters focus on converting qdb_blob_t or qdb_string_t to/from
// other types. They *may* allocate free pointers on the heap, in which case
// those are tracked using the qdb::object_tracker
//
/////

template <>
struct value_converter<traits::bytestring_dtype, qdb_string_t>
{
    using char_t = std::string::value_type;

    template <concepts::input_range_t<char_t> R>
        requires(ranges::sized_range<R> && ranges::contiguous_range<R>)
    inline qdb_string_t operator()(R && x) const
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
        requires(ranges::sized_range<R> && ranges::contiguous_range<R>)
    inline qdb_string_t operator()(R && x) const
    {
        // std::cout << "+  input" << std::endl;
        // ranges::for_each(x, [](auto && x) { printf("%08X\n", x); });
        // std::cout << "- /input" << std::endl;

        // Calculate total size of output buffer; we *could* do it more
        // accurately by first scanning everything and then filling it,
        // but trades memory efficiency for performance.
        //
        // As such, we just allocate the maximum amount of theoretical bytes.
        std::size_t n_codepoints  = ranges::size(x);
        std::size_t max_bytes_out = n_codepoints * sizeof(in_char_type);

        // std::cout << "input size, n_codepoints  = " << n_codepoints << std::endl;
        // std::cout << "input size, max_bytes_out = " << max_bytes_out << std::endl;

        // Note: we allocate the buffer on our object_tracker heap!
        out_char_type * out = qdb::object_tracker::alloc<out_char_type>(max_bytes_out);

        // Get some range representation for this output buffer
        auto out_      = ranges::views::counted(out, max_bytes_out);
        auto out_begin = ranges::begin(out_);

        // Project our input data (in UTF32 / code points) to UTF8
        auto codepoints = unicode::utf32::decode_view(std::move(x));
        auto encoded    = unicode::utf8::encode_view(std::move(codepoints));

        // std::cout << "encoded size = " << ranges::size(encoded) << std::endl;

        // Copy everything and keep track of the end
        auto [in_end, out_end] = ranges::copy(encoded, out_begin);

        // We can use the position of the output iterator to calculate
        // the length of the generated string.
        qdb_size_t n = static_cast<qdb_size_t>(std::distance(out_begin, out_end));
        // std::cout << "n = " << n << std::endl;
        // std::cout << "ranges::size(encoded) = " << ranges::size(encoded) << std::endl;

        // std::cout << "+  output: " << std::endl;
        // ranges::for_each(encoded, [](auto && x) { printf("%02X\n", x); });
        // std::cout << "- /output " << std::endl;

        // Sanity check: we expect to have written exactly as many bytes as our range claims it is
        assert(n == ranges::size(encoded));

        // UTF32->UTF8 we always expect at least as many items
        // assert(n >= n_codepoints);

        return qdb_string_t{out, n};
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
struct value_converter<qdb_string_t, traits::unicode_dtype>
{
    value_converter<qdb_string_t, qdb_string_view> delegate_{};

    inline auto operator()(qdb_string_t const & x) const
    {
        return unicode::utf32::encode_view(unicode::utf8::decode_view(delegate_(x)));
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
} // namespace qdb::convert::detail

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

} // namespace qdb::convert
