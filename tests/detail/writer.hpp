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

#include "mock_failure.hpp"
#include <detail/writer.hpp>
#include <concepts.hpp>

namespace qdb::detail
{
namespace py = pybind11;

struct mock_failure_writer_push_strategy
{

    using Delegate = detail::default_writer_push_strategy;

    mock_failure_options options_;
    Delegate delegate_;

    mock_failure_writer_push_strategy() = delete;

    mock_failure_writer_push_strategy(mock_failure_options const & options, Delegate const & delegate)
        : options_{options}
        , delegate_{delegate} {};

    static mock_failure_writer_push_strategy from_kwargs(py::kwargs const & kwargs)
    {
        return {mock_failure_options::from_kwargs(kwargs), Delegate::from_kwargs(kwargs)};
    }

    inline qdb_error_t operator()(qdb_handle_t handle,
        qdb_exp_batch_options_t const * options,
        qdb_exp_batch_push_table_t const * tables,
        qdb_exp_batch_push_table_schema_t const ** table_schemas,
        qdb_size_t table_count)
    {
        if (options_.has_next() == true) [[unlikely]]
        {
            options_ = options_.next();
            return options_.error();
        }

        // No failures left, just delegate to the default
        return delegate_(handle, options, tables, table_schemas, table_count);
    }
};

}; // namespace qdb::detail
