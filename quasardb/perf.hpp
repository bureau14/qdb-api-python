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

#include "handle.hpp"
#include <qdb/perf.h>
#include <chrono>
#include <vector>

namespace qdb
{

std::string perf_label_name(qdb_perf_label_t label)
{
    switch (label)
    {
    case qdb_pl_undefined:
        return "undefined";
    case qdb_pl_accepted:
        return "accepted";
    case qdb_pl_received:
        return "received";
    case qdb_pl_secured:
        return "secured";
    case qdb_pl_deserialization_starts:
        return "deserialization_starts";
    case qdb_pl_deserialization_ends:
        return "deserialization_ends";
    case qdb_pl_entering_chord:
        return "entering_chord";
    case qdb_pl_processing_starts:
        return "processing_starts";
    case qdb_pl_dispatch:
        return "dispatch";
    case qdb_pl_serialization_starts:
        return "serialization_starts";
    case qdb_pl_serialization_ends:
        return "serialization_ends";
    case qdb_pl_processing_ends:
        return "processing_ends";
    case qdb_pl_replying:
        return "replying";
    case qdb_pl_replied:
        return "replied";
    case qdb_pl_entry_writing_starts:
        return "entry_writing_starts";
    case qdb_pl_entry_writing_ends:
        return "entry_writing_ends";
    case qdb_pl_content_reading_starts:
        return "content_reading_starts";
    case qdb_pl_content_reading_ends:
        return "content_reading_ends";
    case qdb_pl_content_writing_starts:
        return "content_writing_starts";
    case qdb_pl_content_writing_ends:
        return "content_writing_ends";
    case qdb_pl_directory_reading_starts:
        return "directory_reading_starts";
    case qdb_pl_directory_reading_ends:
        return "directory_reading_ends";
    case qdb_pl_directory_writing_starts:
        return "directory_writing_starts";
    case qdb_pl_directory_writing_ends:
        return "directory_writing_ends";
    case qdb_pl_entry_trimming_starts:
        return "entry_trimming_starts";
    case qdb_pl_entry_trimming_ends:
        return "entry_trimming_ends";
    case qdb_pl_ts_evaluating_starts:
        return "ts_evaluating_starts";
    case qdb_pl_ts_evaluating_ends:
        return "ts_evaluating_ends";
    case qdb_pl_ts_bucket_updating_starts:
        return "ts_bucket_updating_starts";
    case qdb_pl_ts_bucket_updating_ends:
        return "ts_bucket_updating_ends";
    case qdb_pl_affix_search_starts:
        return "affix_search_starts";
    case qdb_pl_affix_search_ends:
        return "affix_search_ends";
    case qdb_pl_unknown:
        return "unknown";
    }
    return "";
}

class perf
{
public:
    explicit perf(qdb::handle_ptr h)
        : _handle{h}
    {}

    // we use vectors of pairs instead of maps to keep the order as it is
    // provided to us
    using measurement = std::pair<std::string, std::chrono::nanoseconds>;
    using profile = std::pair<std::string, std::vector<measurement>>;
    
    std::vector<profile> get_profiles() const
    {
        std::vector<profile> profiles;
        
        qdb_perf_profile_t * qdb_profiles = nullptr;
        qdb_size_t count = 0;

        qdb::qdb_throw_if_error(qdb_perf_get_profiles(*_handle, &qdb_profiles, &count));
        
        profiles.reserve(count);
        std::transform(qdb_profiles, qdb_profiles + count, std::back_inserter(profiles), [](const qdb_perf_profile_t & prof) {
            std::vector<measurement> measurements;
            measurements.reserve(prof.count);
            std::transform(prof.measurements, prof.measurements + prof.count, std::back_inserter(measurements), [](const qdb_perf_measurement_t & mes) {
                return std::make_pair(std::move(perf_label_name(mes.label)), std::chrono::nanoseconds{mes.elapsed});
            });
            return std::make_pair(std::move(std::string{prof.name.data, prof.name.length}), std::move(measurements));
        });
        qdb_release(*_handle, qdb_profiles);

        return profiles;
    }

    void clear_all_profiles() const
    {
        qdb::qdb_throw_if_error(qdb_perf_clear_all_profiles(*_handle));
    }
    
    void enable_client_tracking() const
    {
        qdb::qdb_throw_if_error(qdb_perf_enable_client_tracking(*_handle));
    }
    
    void disable_client_tracking() const
    {
        qdb::qdb_throw_if_error(qdb_perf_disable_client_tracking(*_handle));
    }
    
private:
    qdb::handle_ptr _handle;
};


template <typename Module>
static inline void register_perf(Module & m)
{
    namespace py = pybind11;

    py::class_<qdb::perf>(m, "Perf")
        .def(py::init<qdb::handle_ptr>())                       //
        .def("get", &qdb::perf::get_profiles)                   //
        .def("clear", &qdb::perf::clear_all_profiles)           //
        .def("enable", &qdb::perf::enable_client_tracking)      //
        .def("disable", &qdb::perf::disable_client_tracking);   //
}

}
