/*
 *
 * Official Python API
 *
 * Copyright (c) 2009-2019, quasardb SAS
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

#include <qdb/error.h>
#include <utility>

namespace qdb
{

class exception
{
public:
    exception() noexcept
    {}

    explicit exception(qdb_error_t err) noexcept
        : _error{err}
    {}

    const char * what() const noexcept
    {
        return qdb_error(_error);
    }

private:
    qdb_error_t _error{qdb_e_ok};
};

namespace detail
{

struct no_op
{
    void operator()() const noexcept {}
};

} // namespace detail

// Allow a callable to be run just before the throw, thus making nice clean-up possible
// (such as calls to `qdb_release`)
// `pre_throw` defaults to a `no_op` functor that does nothing.
template <typename PreThrowFtor = detail::no_op>
void qdb_throw_if_error(qdb_error_t err, PreThrowFtor&& pre_throw = detail::no_op {})
{
    static_assert(noexcept(std::forward<PreThrowFtor&&>(pre_throw)()), "`pre_throw` argument must be noexcept");
    if ((qdb_e_ok != err) && (qdb_e_ok_created != err))
    {
        pre_throw();
        throw qdb::exception{err};
    }
}

} // namespace qdb
