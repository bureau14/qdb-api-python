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

#include "../handle.hpp"

namespace qdb
{

namespace detail
{


/**
 * Resource guard for QDB-native allocated resources. Automatically invokes
 * qdb_release() when the guard goes out of scope.
 */
template <typename ValueType>
class qdb_resource {
public:
  /**
   * Default constructor, initializes nullpointer.
   */
  qdb_resource(qdb::handle_ptr h) : qdb_resource(h, nullptr) {}

  /**
   * Constructor and immediately initialize with pointer.
   */
  qdb_resource(qdb::handle_ptr h, ValueType * p) : h_(h),
                                                   p_(p) {}


  qdb_resource(qdb_resource const &) = delete;
  qdb_resource &
  operator=(qdb_resource const &) = delete;


  ~qdb_resource() {
    if (p_ != nullptr) {
      qdb_release(*h_, p_);
    }

    p_ = nullptr;
  }

  constexpr operator ValueType*() const {
    return get();
  }

  operator ValueType*() {
    return get();
  }

  ValueType *
  get() {
    return p_;
  }

  constexpr ValueType const *
  get() const {
    return p_;
  }

  ValueType **
  operator&() {
    return &p_;
  }

  ValueType &
  operator*() {
    return *p_;
  }

  constexpr ValueType const &
  operator*() const {
    return *p_;
  }

private:
  qdb::handle_ptr h_;
  ValueType * p_;

};


} // namespace detail

} // namespace qdb
