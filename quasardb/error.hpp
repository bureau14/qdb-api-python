/*
 *
 * Official Python API
 *
 * Copyright (c) 2009-2020, quasardb SAS. All rights reserved.
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

#include <iostream>
#include "logger.hpp"
#include <qdb/error.h>
#include <qdb/client.h>
#include <utility>
#include <pybind11/pybind11.h>

namespace py = pybind11;

namespace qdb
{

class exception
{
public:
    exception() noexcept
    {}

    explicit exception(qdb_error_t err, std::string msg) noexcept
      : _error{err},
        _msg(msg)
    {}

    virtual const char * what() const noexcept
    {
      return _msg.c_str();
    }

private:
    qdb_error_t _error{qdb_e_ok};
    std::string _msg;
};

class input_buffer_too_small_exception : public exception
{
public:
    input_buffer_too_small_exception() noexcept
    : exception(qdb_e_network_inbuf_too_small,
                std::string("Input buffer too small: result set too large. Hint: consider increasing the buffer size using cluster.options().set_client_max_in_buf_size(..) prior to address this error."))
    {
    }
};

class incompatible_type_exception : public exception
{
public:
    incompatible_type_exception() noexcept
    : exception(qdb_e_incompatible_type, std::string("Incompatible type"))
    {
    }

};

class alias_already_exists_exception : public exception
{
public:
    alias_already_exists_exception() noexcept
    : exception(qdb_e_alias_already_exists, std::string("Alias already exists"))
    {
    }
};

class invalid_datetime_exception : public exception
{
public:
    invalid_datetime_exception() noexcept
    : exception(qdb_e_incompatible_type,
                std::string("Unable to interpret provided numpy datetime64. Hint: QuasarDB only works with nanosecond precision datetime64. You can correct this by explicitly casting your timestamps to the dtype datetime64[ns]"))
    {
    }

    invalid_datetime_exception(py::object o)
      : exception(qdb_e_incompatible_type,
                  std::string("Unable to interpret provided numpy datetime64: " + (std::string)(py::str(o)) + ". Hint: QuasarDB only works with nanosecond precision datetime64. You can correct this by explicitly casting your timestamps to the dtype datetime64[ns]"))

    {
    }

};

namespace detail
{

struct no_op
{
    void operator()() const noexcept
    {}
};

} // namespace detail

// Allow a callable to be run just before the throw, thus making nice clean-up possible
// (such as calls to `qdb_release`)
// `pre_throw` defaults to a `no_op` functor that does nothing.
template <typename PreThrowFtor = detail::no_op>
void qdb_throw_if_error(qdb_error_t err, PreThrowFtor && pre_throw = detail::no_op{})
{
    static_assert(noexcept(std::forward<PreThrowFtor &&>(pre_throw)()), "`pre_throw` argument must be noexcept");


    // HACKS(leon): we need to flush our log buffer a lot, ideally after every native qdb
    //              call. Guess which function is invoked exactly at those moments?
    qdb::native::flush();

    if ((qdb_e_ok != err) && (qdb_e_ok_created != err))
    {
      qdb_string_t msg_;
      qdb_error_t  err_;
      qdb_get_last_error(&err_, &msg_);

      if (err_ != err) {
        // Error context returned is not the same, which means this thread already made
        // another call to the QDB API, or the QDB API itself
        throw qdb::exception{err, qdb_error(err)};
      }
      assert(err_ == err);

      pre_throw();

      switch (err) {
      case qdb_e_alias_already_exists:
        throw qdb::alias_already_exists_exception{};

      case qdb_e_network_inbuf_too_small:
          throw qdb::input_buffer_too_small_exception{};

      case qdb_e_incompatible_type:
          throw qdb::incompatible_type_exception{};

      default:
          throw qdb::exception{err_, msg_.data};
      };
    }
}

template <typename Module>
static inline void register_errors(Module & m)
{
    py::register_exception<qdb::exception>(m, "Error");
    py::register_exception<qdb::input_buffer_too_small_exception>(m, "InputBufferTooSmallError");
    py::register_exception<qdb::alias_already_exists_exception>(m, "AliasAlreadyExistsError");
    py::register_exception<qdb::invalid_datetime_exception>(m, "InvalidDatetimeError");
    py::register_exception<qdb::incompatible_type_exception>(m, "IncompatibleTypeError");
}

} // namespace qdb
