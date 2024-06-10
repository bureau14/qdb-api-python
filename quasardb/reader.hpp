/*
 *
 * Official Python API
 *
 * Copyright (c) 2009-2021, quasardb SAS. All rights reserved.
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
#include "logger.hpp"
#include "object_tracker.hpp"
#include "table.hpp"
#include <qdb/ts.h>
#include <vector>

namespace py = pybind11;

namespace qdb
{

namespace detail
{

using int64_column     = std::vector<qdb_int_t>;
using double_column    = std::vector<double>;
using timestamp_column = std::vector<qdb_timespec_t>;
using blob_column      = std::vector<qdb_blob_t>;
using string_column    = std::vector<qdb_string_t>;

} // namespace detail

class reader
{
    using int64_column     = detail::int64_column;
    using double_column    = detail::double_column;
    using timestamp_column = detail::timestamp_column;
    using blob_column      = detail::blob_column;
    using string_column    = detail::string_column;

public:
    /**
     * Tables must always be a list of actual table objects. This ensures the lifetime
     * of any metadata inside the tables (such as its name) will always exceed that
     * of the reader, which simplifies things a lot.
     */
    reader(qdb::handle_ptr handle, std::vector<qdb::table> const & tables)
        : logger_("quasardb.reader")
        , handle_{handle}
        , reader_{}
        , tables_{tables}
    {}

    // prevent copy because of the table object, use a unique_ptr of the batch in cluster
    // to return the object.
    //
    // we prevent these copies because that is almost never what you want, and it gives us
    // more freedom in storing a lot of data inside this object.
    reader(const reader &) = delete;

    ~reader()
    {
        close();
    }

    /**
     * Opens the actual reader; this will initiatate a call to quasardb and initialize the local
     * reader handle. If table strings are provided instead of qdb::table objects, will automatically
     * look those up.
     *
     * May throw exception upon error.
     *
     * :NOTE(leon): We just return a reference to ourselves, but maybe we want the outer object to wrap
     *              a subclass and return that as well. Not 100% sure if that's the best way to go. This
     *              works right now and is the same approach that we take with e.g. qdb::cluster
     */
    reader const & enter();

    void exit(pybind11::object type, pybind11::object value, pybind11::object traceback)
    {
        return close();
    }

    /**
     * Clean up and close. Does not require all data to be actually read.
     */
    void close();

private:
    qdb::logger logger_;
    qdb::handle_ptr handle_;
    qdb_reader_handle_t reader_;

    std::vector<qdb::table> tables_;

    qdb::object_tracker::scoped_repository _object_tracker;
};

using reader_ptr = std::unique_ptr<reader>;

void register_reader(py::module_ & m);

} // namespace qdb
