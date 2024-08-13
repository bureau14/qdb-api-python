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

#include <pybind11/pybind11.h>
#include <chrono>

namespace qdb::detail
{

struct retry_options
{
    // how many retries are left. 0 means no retries
    std::size_t retries_left_;

    // a canary when running tests, to trigger 'please retry' errors for this amount of times
#ifdef QDB_TESTS_ENABLED
#    warning "Tests are enabled, enabling retry canary"
    std::size_t test_canary_retry_count_;
#endif

    // delay for the next retry
    std::chrono::milliseconds delay_;

    // factor by which delay is increased every retry
    std::size_t exponent_;

    // random jitter added/removed to delay_. Jitter of 0.1 means that 10% is automatically
    // added or removed from delay_
    double jitter_;

    retry_options(std::size_t retries   = 3,
        std::chrono::milliseconds delay = std::chrono::milliseconds{3000},
        std::size_t exponent            = 2,
        double jitter                   = 0.1)
        : retries_left_{retries}
        , delay_{delay}
        , exponent_{exponent}
        , jitter_{jitter}
    {}

    static retry_options from_kwargs(py::kwargs args)
    {
        if (!args.contains("retries"))
        {
            return {};
        }

        if (!args.contains("retry_delay"))
        {
            return {args["retries"].cast<std::size_t>()};
        }

        // TODO(leon): support additional arguments such as exponent and jitter.
        //             for now we just use default values.
        return {
            args["retries"].cast<std::size_t>(), args["retry_delay"].cast<std::chrono::milliseconds>()};
    }

    inline constexpr bool has_next() const
    {
        return retries_left_ > 0;
    }

    /**
     * Returns new object, with `retries_left_` and `delay_` adjusted accordingly.
     */
    retry_options next() const
    {

        assert(has_next() == true);

        return retry_options{retries_left_ - 1, delay_ * exponent_, exponent_, jitter_};
    }

    /**
     * Returns the next sleep duration, based on `delay_` and the provided jitter.
     */

    std::chrono::milliseconds sleep_duration() const
    {
        // TODO(leon): include jitter_
        return delay_;
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

}; // namespace qdb::detail
