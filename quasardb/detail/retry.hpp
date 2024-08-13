/*
 *
 * Official Python API
 *
 * Copyright (c) 2009-2021, quasardb SAS. All rights reserved.
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

#include "../error.hpp"
#include <pybind11/pybind11.h>
#include <chrono>

namespace qdb::detail
{

struct retry_options
{
    // how many retries are left. 0 means no retries
    std::size_t retries_left;

    // delay for the next retry
    std::chrono::milliseconds delay;

    // factor by which delay is increased every retry
    std::size_t exponent;

    // random jitter added/removed to delay_. Jitter of 0.1 means that 10% is automatically
    // added or removed from delay_
    double jitter;

#ifdef QDB_TESTS_ENABLED
    // a canary when running tests, to trigger 'please retry' errors for this amount of times,
    // only enabled when building for tests.
    std::size_t test_canary_retry_count;

    /**
     * Retry options with test canary
     */
    retry_options(std::size_t retries       = 3,
        std::chrono::milliseconds delay     = std::chrono::milliseconds{3000},
        std::size_t exponent                = 2,
        double jitter                       = 0.1,
        std::size_t test_canary_retry_count = 0)
        : retries_left{retries}
        , delay{delay}
        , exponent{exponent}
        , jitter{jitter}
        , test_canary_retry_count{test_canary_retry_count}
    {}

#else

    /**
     * Retry options constructor
     */
    retry_options(std::size_t retries   = 3,
        std::chrono::milliseconds delay = std::chrono::milliseconds{3000},
        std::size_t exponent            = 2,
        double jitter                   = 0.1)
        : retries_left{retries}
        , delay{delay}
        , exponent{exponent}
        , jitter{jitter}
    {}

#endif

    static retry_options from_kwargs(py::kwargs args)
    {
        // TODO(leon): standardize theses default parameters in a constexpr somewhere, so that
        //             we don't have to maintain the same standard values in multiple places.
        std::size_t retries{3};
        std::chrono::milliseconds delay{std::chrono::milliseconds{3000}};
        std::size_t exponent{2};
        double jitter{0.1};

        if (args.contains("retries"))
        {
            retries = args["retries"].cast<std::size_t>();
        }

        if (args.contains("retry_delay"))
        {
            delay = args["retry_delay"].cast<std::chrono::milliseconds>();
        }

        if (args.contains("retry_exponent"))
        {
            exponent = args["retry_exponent"].cast<std::size_t>();
        }

        if (args.contains("retry_jitter"))
        {
            jitter = args["retry_jitterr"].cast<double>();
        }

#ifdef QDB_TESTS_ENABLED
        std::size_t test_canary_retry_count{0};

        if (args.contains("retry_test_canary_retry_count"))
        {
            test_canary_retry_count = args["retry_test_canary_retry_count"].cast<std::size_t>();
        }

        return {retries, delay, exponent, jitter, test_canary_retry_count};

#else
        return {retries, delay, exponent, jitter};
#endif
    }

    inline constexpr bool has_next() const
    {
        return retries_left > 0;
    }

    /**
     * Returns new object, with `retries_left_` and `delay_` adjusted accordingly.
     */
    retry_options next() const
    {
        if (has_next() == false) [[unlikely]]
        {
            throw qdb::out_of_bounds_exception{
                "RetryOptions.next() called but retries already exhausted."};
        }

        assert(has_next() == true);

        return retry_options{retries_left - 1, delay * exponent, exponent, jitter};
    }

    /**
     * Returns the next sleep duration, based on `delay` and the provided jitter.
     */

    std::chrono::milliseconds sleep_duration() const
    {
        // TODO(leon): include jitter_
        return delay;
    }
};

constexpr bool is_retryable(qdb_error_t e)
{
    switch (e)
    {
    case qdb_e_async_pipe_full:
    case qdb_e_try_again:
        return true;
        break;
    default:
        return false;
    };
}

static inline void register_retry_options(py::module_ & m)
{

    namespace py = pybind11;

    auto retry_options_c = py::class_<retry_options>{m, "RetryOptions"};

#ifdef QDB_TESTS_ENABLED                                                                           //
    retry_options_c                                                                                //
        .def(py::init<std::size_t, std::chrono::milliseconds, std::size_t, double, std::size_t>(), //
            py::arg("retries")                 = std::size_t{3},                                   //
            py::arg("delay")                   = std::chrono::milliseconds{3000},                  //
            py::arg("exponent")                = std::size_t{2},                                   //
            py::arg("jitter")                  = double{0.1},                                      //
            py::arg("test_canary_retry_count") = std::size_t{0}                                    //
        );
#else
    retry_options_c                                                                   //
        .def(py::init<std::size_t, std::chrono::milliseconds, std::size_t, double>(), //
            py::arg("retries")  = std::size_t{3},                                     //
            py::arg("delay")    = std::chrono::milliseconds{3000},                    //
            py::arg("exponent") = std::size_t{2},                                     //
            py::arg("jitter")   = double{0.1}                                         //
        );
#endif

    retry_options_c                                                                        //
        .def_readwrite("retries_left", &retry_options::retries_left)                       //
        .def_readwrite("delay", &retry_options::delay)                                     //
        .def_readwrite("exponent", &retry_options::exponent)                               //
        .def_readwrite("jitter", &retry_options::jitter)                                   //
                                                                                           //
#ifdef QDB_TESTS_ENABLED                                                                   //
        .def_readwrite("test_canary_retry_count", &retry_options::test_canary_retry_count) //
#endif                                                                                     //
                                                                                           //
        .def("has_next", &retry_options::has_next)                                         //
        .def("next", &retry_options::next)                                                 //

        ;
}

}; // namespace qdb::detail
