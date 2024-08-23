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

#ifndef QDB_TESTS_ENABLED
#    error "This file is intended for tests purposes only"
#endif // QDB_TESTS_ENABLED

#include <qdb/error.h>
#include <pybind11/pybind11.h>

namespace qdb::detail
{
namespace py = pybind11;

// A mock when running tests, to trigger failures / errors for a cetain amount of times,
// only enabled when building for tests.
struct mock_failure_options
{

    std::size_t failures_left;

    // The type of error we raise
    qdb_error_t err;

    constexpr mock_failure_options(std::size_t failures = 0, qdb_error_t err = qdb_e_async_pipe_full)
        : failures_left{failures}
        , err{err}
    {}

    /**
     * Returns true if we have an additional failure to mock for tests
     */
    inline constexpr bool has_next() const
    {
        return failures_left > 0;
    }

    inline constexpr mock_failure_options next() const
    {
        // We can get away with an assertion here, because this code is not part of the production
        // release, so we don't have to "play nice" and throw exceptions.
        assert(has_next() == true);

        return {failures_left - 1, err};
    }

    inline constexpr qdb_error_t error() const
    {
        return err;
    }

    static inline mock_failure_options from_kwargs(py::kwargs args)
    {
        // Keeping this out of the main build prevents accidental mistakes
        if (args.contains("mock_failure_options") == false)
        {
            return {};
        }

        return args["mock_failure_options"].cast<mock_failure_options>();
    }
};

static inline void register_mock_failure_options(py::module_ & m);

}; // namespace qdb::detail
