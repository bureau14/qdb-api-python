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

#include <qdb/client.h>
#include <pybind11/pybind11.h>
#include <ctime>
#include <datetime.h> // from python
#include <iostream>
#include <time.h>

namespace qdb
{
namespace py = pybind11;

/**
 * Wrapper for `datetime.datetime`.
 */
class pydatetime : public py::object
{
public:
    PYBIND11_OBJECT_DEFAULT(pydatetime, object, PyDateTime_Check);

    inline int year() const noexcept
    {
        return PyDateTime_GET_YEAR(ptr());
    };

    inline int month() const noexcept
    {
        return PyDateTime_GET_MONTH(ptr());
    };

    inline int day() const noexcept
    {
        return PyDateTime_GET_DAY(ptr());
    };

    inline int hour() const noexcept
    {
        return PyDateTime_DATE_GET_HOUR(ptr());
    };

    inline int minute() const noexcept
    {
        return PyDateTime_DATE_GET_MINUTE(ptr());
    };

    inline int second() const noexcept
    {
        return PyDateTime_DATE_GET_SECOND(ptr());
    };

    inline int microsecond() const noexcept
    {
        return PyDateTime_DATE_GET_MICROSECOND(ptr());
    };
};

}; // namespace qdb
