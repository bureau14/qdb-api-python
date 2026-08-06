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

#include "error.hpp"
#include "handle.hpp"
#include "logger.hpp"
#include <qdb/query.h>
#include "detail/qdb_resource.hpp"
#include <cassert>
#include <memory>
#include <string>
#include <vector>

namespace py = pybind11;

namespace qdb
{

class query_reader;
using query_reader_ptr = std::unique_ptr<query_reader>;

namespace detail
{

class query_reader_iterator
{
public:
    // Default constructor, which represents the "end" of the range
    query_reader_iterator() noexcept
        : parent_{nullptr}
        , offset_{0}
    {}

    // Actual initialization; starts at the first row of the result.
    query_reader_iterator(query_reader const * parent) noexcept
        : parent_{parent}
        , offset_{0}
    {}

    bool operator!=(query_reader_iterator const & rhs) const noexcept
    {
        return !(*this == rhs);
    }

    bool operator==(query_reader_iterator const & rhs) const noexcept
    {
        // The end sentinel is (parent_ == nullptr, offset_ == 0); an in-range
        // iterator can never compare equal to it.
        if (parent_ == nullptr)
        {
            assert(offset_ == 0);
        }

        return (parent_ == rhs.parent_ //
                && offset_ == rhs.offset_);
    }

    query_reader_iterator & operator++();

    py::dict operator*() const;

private:
    qdb_size_t effective_batch_size() const noexcept;
    qdb_size_t this_batch_row_count() const noexcept;

private:
    /**
     * No per-batch prefetch (unlike the bulk reader): every batch is a row
     * subrange [offset_, offset_ + batch) of the C result held by parent_.
     * The iterator owns no resources.
     */
    query_reader const * parent_;
    qdb_size_t offset_;
};

}; // namespace detail

/**
 * Streaming counterpart of `numpy_query`: runs one `qdb_query` at `__enter__`,
 * keeps the C result alive for the stream's lifetime, and converts rows one
 * batch at a time. Peak memory is the C result plus one converted batch.
 *
 * The schema (column names and value-type tags) is probed once over the full
 * result at `__enter__`; dtypes cannot change between batches, even when a
 * batch is all-null for a column.
 *
 * Yielded batches are dicts of numpy-owned copies and stay valid after
 * `close()`. The C result is released in `close()`, or by the destructor.
 *
 * Duplicate column names (e.g. `SELECT sum(x), sum(x)`) collapse to one
 * dict key.
 */
class query_reader
{
public:
    using iterator = detail::query_reader_iterator;

public:
    query_reader(qdb::handle_ptr handle, std::string const & query, std::size_t batch_size)
        : logger_("quasardb.query_reader")
        , handle_{handle}
        , query_{query}
        , batch_size_{batch_size}
        , result_{*handle}
    {}

    // prevent copy and move: iterators point back at this object, and the
    // C result guard is single-owner.
    query_reader(const query_reader &) = delete;
    query_reader(query_reader &&)      = delete;

    ~query_reader()
    {
        close();
    }

    /**
     * Returns the configured batch size; 0 means a single batch.
     */
    constexpr inline std::size_t get_batch_size() const noexcept
    {
        return batch_size_;
    }

    /**
     * Executes the query and probes the schema. May throw exception upon error.
     */
    query_reader const & enter();

    void exit(pybind11::object type, pybind11::object value, pybind11::object traceback)
    {
        return close();
    }

    /**
     * Clean up and close. Does not require all data to be actually read.
     */
    void close();

    iterator begin() const
    {
        // A NULL result does not imply "not entered": DML statements leave it
        // NULL, so opened-ness is tracked separately.
        if (!entered_) [[unlikely]]
        {
            throw qdb::uninitialized_exception{
                "Query stream not yet opened: please encapsulate calls to the query stream in a "
                "`with` block"};
        }

        // Zero-row and DML (NULL) results yield zero batches, not one batch
        // of empty arrays.
        if (result_.get() == nullptr || result_->row_count == 0)
        {
            return end();
        }

        return iterator{this};
    }

    iterator end() const noexcept
    {
        return iterator{};
    }

private:
    // Iterators read result_ and the probed schema directly.
    friend class detail::query_reader_iterator;

    qdb::logger logger_;
    qdb::handle_ptr handle_;

    std::string query_;
    std::size_t batch_size_;

    detail::qdb_resource<qdb_query_result_t> result_;
    std::vector<std::string> column_names_;
    std::vector<qdb_query_result_value_type_t> column_types_;
    bool entered_{false};
};

static inline query_reader_ptr make_query_reader_ptr(handle_ptr handle, //
    std::string const & query,                                          //
    std::size_t batch_size                                              //
)
{
    return std::make_unique<query_reader>(handle, query, batch_size);
}

void register_query_reader(py::module_ & m);

} // namespace qdb
