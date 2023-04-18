/*
 *
 * Official Python API
 *
 * Copyright (c) 2009-2023, quasardb SAS. All rights reserved.
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

#include "query.hpp"
#include <atomic>
#include <condition_variable>
#include <memory>

namespace py = pybind11;

namespace qdb
{

class query_continuous : public std::enable_shared_from_this<query_continuous>
{
public:
    query_continuous(qdb::handle_ptr h,
        qdb_query_continuous_mode_type_t mode,
        std::chrono::milliseconds pace,
        const std::string & query_string,
        const py::object & bools);
    query_continuous(const qdb::query_continuous & /*other*/) = delete;
    ~query_continuous();

private:
    void release_results();
    qdb_error_t copy_results(const qdb_query_result_t * res);
    static int continuous_callback(void * p, qdb_error_t err, const qdb_query_result_t * res);

private:
    dict_query_result_t unsafe_results();

public:
    // returns the results (blocking)
    dict_query_result_t results();
    // returns the results (non-blocking), empty if no results available yet
    // needed to be able to interface with other framework that can't wait for results to be available
    // in that case you would poll probe_results(), the cost is low because it doesn't result in a
    // remote call just acquiring the mutex and see if results have been updated
    dict_query_result_t probe_results();
    void stop();

private:
    qdb::handle_ptr _handle;
    qdb_query_cont_callback_t _callback;
    qdb_query_cont_handle_t _cont_handle;

    py::object _parse_bools;

    mutable std::condition_variable _results_cond;
    mutable std::mutex _results_mutex;
    std::atomic<size_t> _previous_watermark;
    std::atomic<size_t> _watermark;
    qdb_error_t _last_error;

    qdb_query_result_t * _results;
};

template <typename Module>
static inline void register_continuous(Module & m)
{
    namespace py = pybind11;

    py::class_<qdb::query_continuous, std::shared_ptr<qdb::query_continuous>>{m, "QueryContinuous"} //
        .def(py::init<qdb::handle_ptr, qdb_query_continuous_mode_type_t, std::chrono::milliseconds,
            const std::string &,
            const py::object &>())                                   //
        .def("results", &qdb::query_continuous::results)             //
        .def("probe_results", &qdb::query_continuous::probe_results) //
        .def("stop", &qdb::query_continuous::stop)                   //

        // required interface to use query_continuous as an iterator
        .def("__iter__", [](const std::shared_ptr<qdb::query_continuous> & cont) { return cont; }) //
        .def("__next__", &qdb::query_continuous::results);                                         //
}

} // namespace qdb
