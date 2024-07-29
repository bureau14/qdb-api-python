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

class reader_data
{
public:
    /**
     * Utility function which converts table data into a vanilla dict. Currently this works well, as
     * there isn't any additional data/state we need to keep track of --
     */
    static py::dict convert(qdb_bulk_reader_table_data_t const & data);
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
    reader_iterator(
        handle_ptr handle, qdb_reader_handle_t reader, std::size_t batch_size, std::size_t table_count)
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

    py::dict operator*()
    {
        assert(ptr_ != nullptr);
        assert(n_ < table_count_);

        return reader_data::convert(ptr_[n_]);
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
    reader(                                            //
        qdb::handle_ptr handle,                        //
        std::vector<std::string> const & table_names,  //
        std::vector<std::string> const & column_names, //
        std::size_t batch_size,                        //
        std::vector<py::tuple> const & ranges)         //
        : logger_("quasardb.reader")
        , handle_{handle}
        , reader_{nullptr}
        , table_names_{table_names}
        , column_names_{column_names}
        , batch_size_{batch_size}
        , ranges_{ranges}
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

    iterator begin() const
    {
        if (reader_ == nullptr) [[unlikely]]
        {
            throw qdb::uninitialized_exception{
                "Reader not yet opened: please encapsulate calls to the reader in a `with` block, or "
                "explicitly `open` and `close` the resource"};
        }
        return iterator{handle_, reader_, batch_size_, table_names_.size()};
    }

    iterator end() const noexcept
    {
        return iterator{};
    }

private:
    qdb::logger logger_;
    qdb::handle_ptr handle_;
    qdb_reader_handle_t reader_;

    std::vector<std::string> table_names_;
    std::vector<std::string> column_names_;
    std::size_t batch_size_;
    std::vector<py::tuple> ranges_;
};

using reader_ptr = std::unique_ptr<reader>;

void register_reader(py::module_ & m);

} // namespace qdb
