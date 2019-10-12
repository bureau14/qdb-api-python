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

#include "error.hpp"
#include "handle.hpp"
#include "utils.hpp"
#include <qdb/tag.h>
#include <pybind11/chrono.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <chrono>

namespace qdb
{

using hostname = std::pair<std::string, unsigned short>;

class entry
{

public:
    struct metadata
    {
        metadata() noexcept
        {}

        // we need to adjust for timezone as quasardb is UTC and pybind11 will
        // assume local date time points
        metadata(const qdb_entry_metadata_t & md) noexcept
            : type{md.type}
            , size{md.size}
            , modification_time{std::chrono::seconds{md.modification_time.tv_sec}}
            , expiry_time{std::chrono::seconds{md.expiry_time.tv_sec}}
        {}

        qdb_entry_type_t type{qdb_entry_uninitialized};
        qdb_uint_t size{0};
        std::chrono::system_clock::time_point modification_time;
        std::chrono::system_clock::time_point expiry_time;
    };

public:
    entry(handle_ptr h, std::string a)
        : _handle{h}
        , _alias{a}
    {}

public:
    bool attach_tag(const std::string & tag)
    {
        const qdb_error_t err = qdb_attach_tag(*_handle, _alias.c_str(), tag.c_str());
        if (QDB_FAILURE(err)) throw qdb::exception{err};

        return err != qdb_e_tag_already_set;
    }

    void attach_tags(const std::vector<std::string> & tags)
    {
        std::vector<const char *> tag_pointers(tags.size());

        std::transform(tags.cbegin(), tags.cend(), tag_pointers.begin(), [](const std::string & s) { return s.c_str(); });

        qdb::qdb_throw_if_error(qdb_attach_tags(*_handle, _alias.c_str(), tag_pointers.data(), tag_pointers.size()));
    }

    bool detach_tag(const std::string & tag)
    {
        const qdb_error_t err = qdb_detach_tag(*_handle, _alias.c_str(), tag.c_str());
        if (QDB_FAILURE(err)) throw qdb::exception{err};

        return err != qdb_e_tag_not_set;
    }

    void detach_tags(const std::vector<std::string> & tags)
    {
        std::vector<const char *> tag_pointers(tags.size());

        std::transform(tags.cbegin(), tags.cend(), tag_pointers.begin(), [](const std::string & s) { return s.c_str(); });

        qdb::qdb_throw_if_error(qdb_detach_tags(*_handle, _alias.c_str(), tag_pointers.data(), tag_pointers.size()));
    }

    bool has_tag(const std::string & tag)
    {
        return qdb_has_tag(*_handle, _alias.c_str(), tag.c_str()) == qdb_e_ok;
    }

    std::vector<std::string> get_tags()
    {
        const char ** tags = nullptr;
        size_t tag_count   = 0;

        qdb::qdb_throw_if_error(qdb_get_tags(*_handle, _alias.c_str(), &tags, &tag_count));

        return convert_strings_and_release(_handle, tags, tag_count);
    }

public:
    void remove()
    {
        qdb::qdb_throw_if_error(qdb_remove(*_handle, _alias.c_str()));
    }

    qdb::hostname get_location() const
    {
        qdb_remote_node_t rn;

        qdb::qdb_throw_if_error(qdb_get_location(*_handle, _alias.c_str(), &rn));

        qdb::hostname res{rn.address, rn.port};

        qdb_release(*_handle, &rn);

        return res;
    }

    metadata get_metadata() const
    {
        qdb_entry_metadata_t md;

        qdb::qdb_throw_if_error(qdb_get_metadata(*_handle, _alias.c_str(), &md));

        return metadata{md};
    }

    qdb_entry_type_t get_entry_type() const
    {
        return get_metadata().type;
    }

    const std::string & get_name() const noexcept
    {
        return _alias;
    }

protected:
    handle_ptr _handle;
    std::string _alias;
};

class expirable_entry : public entry
{
public:
    expirable_entry(handle_ptr h, std::string a)
        : entry{h, a}
    {}

public:
    static qdb_time_t from_time_point(const std::chrono::system_clock::time_point & tp) noexcept
    {
        return tp == std::chrono::system_clock::time_point{}
                   ? qdb_time_t{0}
                   : qdb_time_t{1'000}
                         * static_cast<qdb_time_t>(std::chrono::duration_cast<std::chrono::seconds>(tp.time_since_epoch()).count());
    }

public:
    void expires_at(const std::chrono::system_clock::time_point & expiry_time)
    {
        qdb::qdb_throw_if_error(qdb_expires_at(*_handle, _alias.c_str(), from_time_point(expiry_time)));
    }

    void expires_from_now(std::chrono::milliseconds expiry_delta)
    {
        qdb::qdb_throw_if_error(qdb_expires_from_now(*_handle, _alias.c_str(), expiry_delta.count()));
    }

    std::chrono::system_clock::time_point get_expiry_time()
    {
        return get_metadata().expiry_time;
    }
};

template <typename Module>
static inline void register_entry(Module & m)
{
    namespace py = pybind11;

    py::class_<qdb::entry> e{m, "Entry"}; //

    py::enum_<qdb_entry_type_t>{e, "Type", py::arithmetic(), "Entry type"} //
        .value("Uninitialized", qdb_entry_uninitialized)                   //
        .value("Integer", qdb_entry_integer)                               //
        .value("HashSet", qdb_entry_hset)                                  //
        .value("Tag", qdb_entry_tag)                                       //
        .value("Deque", qdb_entry_deque)                                   //
        .value("Stream", qdb_entry_stream)                                 //
        .value("Timeseries", qdb_entry_ts);                                //
                                                                           //

    e.def(py::init<qdb::handle_ptr, std::string>())         //
        .def("attach_tag", &qdb::entry::attach_tag)         //
        .def("attach_tags", &qdb::entry::attach_tags)       //
        .def("detach_tag", &qdb::entry::detach_tag)         //
        .def("detach_tags", &qdb::entry::detach_tags)       //
        .def("has_tag", &qdb::entry::has_tag)               //
        .def("get_tags", &qdb::entry::get_tags)             //
        .def("remove", &qdb::entry::remove)                 //
        .def("get_location", &qdb::entry::get_location)     //
        .def("get_entry_type", &qdb::entry::get_entry_type) //
        .def("get_metadata", &qdb::entry::get_metadata);    //

    py::class_<qdb::entry::metadata>{e, "Metadata"}                                   //
        .def(py::init<>())                                                            //
        .def_readwrite("type", &qdb::entry::metadata::type)                           //
        .def_readwrite("size", &qdb::entry::metadata::size)                           //
        .def_readwrite("modification_time", &qdb::entry::metadata::modification_time) //
        .def_readwrite("expiry_time", &qdb::entry::metadata::expiry_time);            //

    py::class_<qdb::expirable_entry, qdb::entry>{m, "ExpirableEntry"}     //
        .def(py::init<qdb::handle_ptr, std::string>())                    //
        .def("expires_at", &qdb::expirable_entry::expires_at)             //
        .def("expires_from_now", &qdb::expirable_entry::expires_from_now) //
        .def("get_expiry_time", &qdb::expirable_entry::get_expiry_time);  //
}

} // namespace qdb
