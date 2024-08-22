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
#include <chrono>

namespace qdb::detail
{

struct retry_options
{
    qdb::logger logger_;

    // how many retries are left. 0 means no retries
    std::size_t retries_left;

    // delay for the next retry
    std::chrono::milliseconds delay;

    // factor by which delay is increased every retry
    std::size_t exponent;

    // random jitter added/removed to delay_. Jitter of 0.1 means that 10% is automatically
    // added or removed from delay_
    double jitter;

    /**
     * Retry options constructor
     */
    retry_options(std::size_t retries   = 3,
        std::chrono::milliseconds delay = std::chrono::milliseconds{3000},
        std::size_t exponent            = 2,
        double jitter                   = 0.1)
        : logger_("quasardb.detail.retry")
        , retries_left{retries}
        , delay{delay}
        , exponent{exponent}
        , jitter{jitter}
    {}

    static inline retry_options from_kwargs(py::kwargs args)
    {
        if (args.contains("retries") == false)
        {
            return {};
        }

        auto retries = args["retries"];

        // We're going to assume that retries is an actual RetryOptions class, because
        // our Numpy and Pandas adapters always coerce it to that type. By making this
        // assumption, we can slightly optimize this code path.
        //
        // By providing an explicit RetryOptions object is also the only way to fine-tune
        // other parameters.

        try
        {
            return retries.cast<retry_options>();
        }
        catch (py::cast_error const & /*e*/)
        {
            // For convenience, we also give the user the choice to just provide `retries`
            // as an integer, in which case we'll assume this is the number of retries the
            // user wants to perform.
            //
            // In all likelihood, this is the only parameter the user will want to tune.
            return {retries.cast<std::size_t>()};
        }
    }

    inline constexpr bool has_next() const noexcept
    {
        return retries_left > 0;
    }

    /**
     * Returns new object, with `retries_left_` and `delay_` adjusted accordingly.
     */
    inline retry_options next() const
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
     * Returns true if we have retries left and the error is retryable.
     */
    inline constexpr bool should_retry(qdb_error_t e) const noexcept
    {
        // We check for `is_retryable` first, because in all likelihood e is qdb_e_ok
        return retry_options::is_retryable(e) && has_next();
    }

    static constexpr bool is_retryable(qdb_error_t e) noexcept
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
};

void register_retry_options(py::module_ & m);

}; // namespace qdb::detail
