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

#include "handle.hpp"
#include <qdb/client.h>
#include <qdb/ts.h>
#include <pybind11/numpy.h>
#include <algorithm>
#include <chrono>
#include <string>
#include <vector>

namespace qdb
{
static inline std::vector<std::string> convert_strings_and_release(qdb::handle_ptr h, const char ** ss, size_t c)
{
    std::vector<std::string> res(c);

    std::transform(ss, ss + c, res.begin(), [](const char * s) { return std::string{s}; });

    qdb_release(*h, ss);

    return res;
}

static inline size_t max_length(const qdb_ts_blob_point * points, size_t count)
{
    if (!count) return 0;

    return std::max_element(points, points + count,
        [](const qdb_ts_blob_point & left, const qdb_ts_blob_point & right) { return left.content_length < right.content_length; })
        ->content_length;
}

static inline qdb_time_t to_localtime(qdb_time_t t)
{
    struct tm * local = localtime(&t);
    return mktime(local);
}

} // namespace qdb
