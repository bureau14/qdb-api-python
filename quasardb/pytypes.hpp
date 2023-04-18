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

#include <qdb/client.h>
#include <pybind11/pybind11.h>
#include <datetime.h> // from python

namespace qdb
{
namespace py = pybind11;

class pytimedelta : public py::object
{
public:
    PYBIND11_OBJECT_DEFAULT(pytimedelta, object, PyDelta_Check);

    static pytimedelta from_dsu(py::ssize_t days, py::ssize_t seconds, py::ssize_t usec)
    {
        return py::reinterpret_steal<pytimedelta>(PyDelta_FromDSU(static_cast<int>(days), static_cast<int>(seconds), static_cast<int>(usec)));
    }

    inline int days() const noexcept
    {
        return PyDateTime_DELTA_GET_DAYS(ptr());
    }

    inline int seconds() const noexcept
    {
        return PyDateTime_DELTA_GET_SECONDS(ptr());
    }

    inline int microseconds() const noexcept
    {
        return PyDateTime_DELTA_GET_MICROSECONDS(ptr());
    }
};

class pytzinfo : public py::object
{
    PYBIND11_OBJECT_DEFAULT(pytzinfo, object, PyTZInfo_Check);

    pytimedelta utcoffset(py::object dt) const
    {
        py::object fn = attr("utcoffset");
        return py::reinterpret_borrow<pytimedelta>(fn(dt));
    }

    static pytzinfo utc() noexcept
    {
#if (PY_VERSION_HEX < 0x03070000)
#    pragma message("Python <= 3.6 detected, using slower introspection for UTC timezone lookup")
        py::module m   = py::module::import("datetime");
        py::object ret = m.attr("timezone").attr("utc");
        assert(ret.is_none() == false);
        return py::reinterpret_borrow<pytzinfo>(ret);
#else
        PyObject * ret = PyDateTime_TimeZone_UTC;
#endif
        return py::reinterpret_borrow<pytzinfo>(ret);
    }
};

/**
 * Wrapper for `datetime.datetime`.
 */
class pydatetime : public py::object
{
public:
    PYBIND11_OBJECT_DEFAULT(pydatetime, object, PyDateTime_Check);

    static pydatetime from_date_and_time(int year,
        int month,
        int day,
        int hour,
        int minute,
        int second,
        int microsecond,
        pytzinfo tz = pytzinfo::utc())
    {
        assert(-32767 <= year && year <= 32767);
        assert(1 <= month && month <= 12);
        assert(1 <= day && day <= 31);
        assert(0 <= hour && hour <= 23);
        assert(0 <= second && second <= 59);
        assert(0 <= microsecond && microsecond <= 1'000'000);

        // For funky kwarg syntax from pybind11 with _a literal
        using namespace pybind11::literals;

        return py::reinterpret_steal<pydatetime>(
            PyDateTime_FromDateAndTime(year, month, day, hour, minute, second, microsecond))

            .replace("tzinfo"_a = tz);
    }

    inline int year() const noexcept
    {
        return PyDateTime_GET_YEAR(ptr());
    }

    inline int month() const noexcept
    {
        return PyDateTime_GET_MONTH(ptr());
    }

    inline int day() const noexcept
    {
        return PyDateTime_GET_DAY(ptr());
    }

    inline int hour() const noexcept
    {
        return PyDateTime_DATE_GET_HOUR(ptr());
    }

    inline int minute() const noexcept
    {
        return PyDateTime_DATE_GET_MINUTE(ptr());
    }

    inline int second() const noexcept
    {
        return PyDateTime_DATE_GET_SECOND(ptr());
    }

    inline int microsecond() const noexcept
    {
        return PyDateTime_DATE_GET_MICROSECOND(ptr());
    }

    template <typename... KWargs>
    inline pydatetime replace(KWargs &&... args)
    {
        py::object fn = attr("replace");

        return fn(std::forward<KWargs...>(args...));
    }

    /**
     * proxy for `datetime.datetime.astimezone()` invoked without any arguments,
     * which effectively adds the local time zone to the datetime object.
     */
    inline pydatetime astimezone() const
    {
        py::object fn = attr("astimezone");
        return fn();
    }

    /**
     * convert this datetime to another timezone in such a way that the UTC
     * representation of both remains the same.
     */
    inline pydatetime astimezone(pytzinfo tz) const
    {
        py::object fn = attr("astimezone");
        return fn(tz);
    }

    inline pytzinfo tzinfo() const noexcept
    {

#if (PY_VERSION_HEX < 0x030A0000)
#    pragma message("Python <= 3.9 detected, using slower attribute lookup for tzinfo")
        PyObject * tz = PyObject_GetAttrString(ptr(), "tzinfo");
#else
        PyObject * tz  = PyDateTime_DATE_GET_TZINFO(ptr());
#endif
        pytzinfo tz_ = py::reinterpret_borrow<pytzinfo>(tz);

        if (tz_.is_none())
        {
            // either `datetime.now()` or `datetime.utcnow()`.
            //
            // since the use of `datetime .utcnow()` is actively discouraged in favor
            // of `datetime.now(tz=timezone.utc)`, we are going to assume that no timezone
            // means local time.
            //
            // the most elegant way to handle this is to amend this datetime
            // object with the local timezone and recurse.
            return astimezone().tzinfo();
        }

        return tz_;
    }

    inline pytimedelta utcoffset() const noexcept
    {
        return tzinfo().utcoffset(*this);
    }
};

} // namespace qdb
