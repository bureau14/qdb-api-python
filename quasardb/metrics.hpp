/*
 *
 * Official Python API
 *
 * Copyright (c) 2009-2024, quasardb SAS. All rights reserved.
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

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <cassert>
#include <chrono>
#include <map>
#include <string>

namespace qdb
{
namespace py              = pybind11;
using metrics_container_t = std::map<std::string, std::uint64_t>;

class metrics
{
private:
public:
    /**
     * Utility fixture that automatically records timings for a certain block of code.
     * This is intended to be used from native C++ code only.
     */
    class scoped_capture
    {
        using clock_t      = std::chrono::high_resolution_clock;
        using time_point_t = std::chrono::time_point<clock_t>;

    public:
        scoped_capture(std::string const & test_id) noexcept
            : test_id_{test_id}
            , start_{clock_t::now()} {};
        ~scoped_capture();

    private:
        std::string test_id_;
        time_point_t start_;
    };

    /**
     * Utility class that's exposed to Python which can be used to record metrics in
     * a scope. It makes it easy to track the difference between the beginning and end
     * of execution.
     */
    class measure
    {
    public:
        measure();
        ~measure(){};

    public:
        measure enter()
        {
            // No-op, all initialization is done in the constructor.
            return *this;
        }

        void exit(py::object /* type */, py::object /* value */, py::object /* traceback */)
        {}

        metrics_container_t get() const;

    private:
        metrics_container_t start_;
    };

public:
    metrics() noexcept {};

    ~metrics() noexcept {};

public:
    static void record(std::string const & test_id, std::uint64_t nsec);

    static metrics_container_t totals();
    static void clear();

private:
};

void register_metrics(py::module_ & m);

} // namespace qdb
