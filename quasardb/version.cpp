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
#include "version.hpp"
#include <regex>
#include <sstream>
#include <stdexcept>

static std::pair<int, int> get_version_pair(const char * version)
{
    std::regex re(u8"([0-9]+)\\.([0-9]+)\\..*");
    std::cmatch m;
    if (!std::regex_match(version, m, re))
    {
        std::ostringstream sstr;
        sstr << "Got an invalid QuasarDB C API version string (" << version << ").";
        throw std::invalid_argument(sstr.str());
    }
    const auto major = std::stoi(m[1].str());
    const auto minor = std::stoi(m[2].str());
    return std::make_pair(major, minor);
}

namespace qdb
{

const char * const qdb_c_api_version = QDB_PY_VERSION;

void check_qdb_c_api_version(const char * candidate)
{
    const auto ver_c   = get_version_pair(candidate);
    const auto ver_ref = get_version_pair(qdb_c_api_version);
    if (ver_c != ver_ref)
    {
        std::ostringstream sstr;
        sstr << "QuasarDB C API version mismatch. Expected " << ver_ref.first << '.' << ver_ref.second << " but got " << ver_c.first << '.' << ver_c.second
             << " instead.";
        throw std::runtime_error(sstr.str());
    }
}

} // namespace qdb
