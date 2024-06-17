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
#include <unordered_map>
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

/**
 * One "chunk" of data fetched from one table, represented as a py dict.
 */
class reader_data
{
public:
    reader_data() = delete;

    reader_data(reader_data const & rhs) = delete;

    reader_data(reader_data && rhs)
        : data_{std::move(rhs.data_)}
    {}

    reader_data(qdb_bulk_reader_table_data_t const & data)
    {
        _assign_data(data);
    };

    ~reader_data(){};

    py::str repr();

    /**
     * Provide access to __getitem__ as if we are a dict
     */
    py::object get(std::string const & column_name) const;

    /**
     * Returns total number of data points. I.e. if there are 8 columns with 100 rows each, plus a
     * timestamp index, it will report 900.
     */
    inline py::ssize_t size() const noexcept
    {
        py::ssize_t ret = {0};

        for (auto const & tuple : data_)
        {
            ret += tuple.second.size();
        }

        return ret;
    }

    /**
     * Returns true if no data is visible at all for this table.
     */
    inline bool empty() const noexcept
    {
        return size() == 0;
    }

private:
    /**
     * This function does the heavy lifting of parsing all data and converting it to numpy arrays.
     */
    void _assign_data(qdb_bulk_reader_table_data_t const & data);

private:
    /**
     * All data, indexed by column name. This also contains special columns such as $timestamp and
     * $table, that do not necessarily *have* to be masked arrays, but we just store them as such.
     */
    std::unordered_map<std::string, qdb::masked_array> data_;
};

class reader_iterator
{
public:
    // Default constructor, which represents the "end" of the range
    reader_iterator() noexcept
        : handle_{nullptr}
        , reader_{nullptr}
        , batch_size_{0}
        , table_count_{0}
        , ptr_{nullptr}
        , n_{0}
    {}

    // Actual initialization
    reader_iterator(handle_ptr handle,
        qdb_reader_handle_t reader,
        std::size_t batch_size,
        std::size_t table_count) noexcept
        : handle_{handle}
        , reader_{reader}
        , batch_size_{batch_size}
        , table_count_{table_count}

        , ptr_{nullptr}
        , n_{0}
    {
        // Always immediately try to fetch the first batch.
        this->operator++();
    }

    bool operator!=(reader_iterator const & rhs) const noexcept
    {
        return !(*this == rhs);
    }

    bool operator==(reader_iterator const & rhs) const noexcept
    {
        // This is just a sanity check: if our handle_ is null, it means basically
        // the entire object has to be null, and this will basically represent the
        // ".end()" iterator.

        if (handle_ == nullptr)
        {
            assert(reader_ == nullptr);
            assert(ptr_ == nullptr);
        }
        else
        {
            assert(reader_ != nullptr);
            assert(ptr_ != nullptr);
        }

        // Optimization: we *only* compare the pointers, we don't actually compare
        // the data itself. This saves a bazillion comparisons, and for the purpose
        // of iterators, we really only care whether the current iterator is at the
        // end.
        return (handle_ == rhs.handle_              //
                && reader_ == rhs.reader_           //
                && batch_size_ == rhs.batch_size_   //
                && table_count_ == rhs.table_count_ //
                && ptr_ == rhs.ptr_ && n_ == rhs.n_);
    }

    reader_iterator & operator++();

    reader_data operator*()
    {
        assert(ptr_ != nullptr);
        assert(n_ < table_count_);

        return reader_data{ptr_[n_]};
    }

private:
    qdb::handle_ptr handle_;
    qdb_reader_handle_t reader_;

    /**
     * The amount of rows to fetch in one operation. This can span multiple tables.
     */
    std::size_t batch_size_;

    /**
     * `table_count_` enables us to manage how much far we can iterate `ptr_`.
     */
    std::size_t table_count_;
    qdb_bulk_reader_table_data_t * ptr_;
    std::size_t n_;
};

}; // namespace detail

class reader
{
public:
    using iterator = detail::reader_iterator;

public:
    /**
     * Tables must always be a list of actual table objects. This ensures the lifetime
     * of any metadata inside the tables (such as its name) will always exceed that
     * of the reader, which simplifies things a lot.
     */
    reader(qdb::handle_ptr handle, std::vector<qdb::table> const & tables, std::size_t batch_size)
        : logger_("quasardb.reader")
        , handle_{handle}
        , reader_{}
        , tables_{tables}
        , batch_size_{batch_size}
    {}

    // prevent copy because of the table object, use a unique_ptr of the batch in cluster
    // to return the object.
    //
    // we prevent these copies because that is almost never what you want, and it gives us
    // more freedom in storing a lot of data inside this object.
    reader(const reader &) = delete;
    reader(reader &&)      = delete;

    ~reader()
    {
        close();
    }

    /**
     * Convenience function for accessing the configured batch size. Returns 0 when everything should
     * be read in a single batch.
     */
    constexpr inline std::size_t get_batch_size() const noexcept
    {
        return batch_size_;
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

    iterator begin() const noexcept
    {
        return iterator{handle_, reader_, batch_size_, tables_.size()};
    }

    iterator end() const noexcept
    {
        return iterator{};
    }

private:
    qdb::logger logger_;
    qdb::handle_ptr handle_;
    qdb_reader_handle_t reader_;

    std::vector<qdb::table> tables_;
    std::size_t batch_size_;

    qdb::object_tracker::scoped_repository object_tracker_;
};

using reader_ptr = std::unique_ptr<reader>;

void register_reader(py::module_ & m);

} // namespace qdb
