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

#include "error.hpp"
#include <qdb/client.h>
#include <memory>
#include <string>

namespace qdb
{

class handle
{
public:
    handle() noexcept
        : handle_{nullptr}
    {}

    explicit handle(qdb_handle_t h) noexcept
        : handle_{h}
    {}

    ~handle()
    {
        close();
    }

    void connect(const std::string & uri);

    operator qdb_handle_t() const noexcept
    {
        return handle_;
    }

    void close();

    constexpr inline bool is_open() const
    {
        return handle_ != nullptr;
    }

    /**
     * Throws exception if the connection is not open. Should be invoked before any operation
     * is done on the handle, as the QuasarDB C API only checks for a canary presence in the
     * handle's memory arena. If a compiler is optimizing enough, the handle can be closed but
     * the canary still present in memory, so it's UB.
     *
     * As such, we should check on a higher level.
     */
    inline void check_open() const
    {
        if (is_open() == false) [[unlikely]]
        {
            throw qdb::invalid_handle_exception{};
        }
    }

private:
    qdb_handle_t handle_{nullptr};
};

using handle_ptr = std::shared_ptr<handle>;

static inline handle_ptr make_handle_ptr()
{
    return std::make_shared<qdb::handle>(qdb_open_tcp());
}

} // namespace qdb
