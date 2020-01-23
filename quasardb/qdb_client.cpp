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
#include "cluster.hpp"
#include "node.hpp"
#include <pybind11/pybind11.h>

namespace py = pybind11;

PYBIND11_MODULE(quasardb, m)
{
    m.doc() = "QuasarDB Official Python API";
    m.def("version", &qdb_version, "Return version number");
    m.def("build", &qdb_build, "Return build number");
    m.attr("never_expires") = std::chrono::system_clock::time_point{};

    qdb::register_errors(m);
    qdb::register_cluster(m);
    qdb::register_node(m);
    qdb::register_options(m);
    qdb::register_perf(m);
    qdb::register_entry(m);
    qdb::register_blob(m);
    qdb::register_integer(m);
    qdb::register_direct_blob(m);
    qdb::register_direct_integer(m);
    qdb::register_tag(m);
    qdb::register_query(m);
    qdb::register_table(m);
    qdb::register_batch_inserter(m);
    qdb::register_table_reader(m);

    qdb::detail::register_ts_column(m);
    qdb::reader::register_ts_value(m);
    qdb::reader::register_ts_row(m);
}
