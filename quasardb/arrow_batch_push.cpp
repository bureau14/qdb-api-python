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

#include "arrow_batch_push.hpp"
#include <convert/value.hpp>
#include <detail/writer.hpp>
#include <algorithm>

namespace qdb
{

namespace
{

void _set_deduplication_mode(
    detail::deduplication_mode_t mode, bool columns, qdb_exp_batch_push_arrow_t & out)
{
    // Set deduplication mode only when `columns` is true, in which we will deduplicate based on
    // *all* columns.
    out.deduplication_mode =
        (columns == true ? detail::to_qdb(mode) : qdb_exp_batch_deduplication_mode_disabled);
}

void _set_deduplication_mode(detail::deduplication_mode_t mode,
    std::vector<std::string> const & columns,
    qdb_exp_batch_push_arrow_t & out)
{
    // A specific set of columns to deduplicate has been provided, in which case
    // we'll need to do a small transformation of the column names.
    auto where_duplicate = std::make_unique<char const *[]>(columns.size());

    std::transform(std::cbegin(columns), std::cend(columns), where_duplicate.get(),
        [](std::string const & column) -> char const * { return column.c_str(); });

    out.deduplication_mode    = detail::to_qdb(mode);
    out.where_duplicate       = where_duplicate.release(); //???
    out.where_duplicate_count = columns.size();
}

class arrow_stream_holder
{
public:
    explicit arrow_stream_holder(pybind11::object reader)
        : _reader{std::move(reader)}
    {
        _reader.attr("_export_to_c")(pybind11::int_(reinterpret_cast<uintptr_t>(&_stream)));

        if (_stream.get_schema)
        {
            const int result = _stream.get_schema(&_stream, &_schema);
            if (result != 0)
            {
                throw std::runtime_error("Arrow: get_schema() failed");
            }
        }
        else
        {
            throw std::runtime_error("Arrow: get_schema() is null");
        }
    }

    ~arrow_stream_holder()
    {
        reset();
    }

    arrow_stream_holder(const arrow_stream_holder &)              = delete;
    arrow_stream_holder & operator=(const arrow_stream_holder &)  = delete;
    arrow_stream_holder(const arrow_stream_holder &&)             = delete;
    arrow_stream_holder & operator=(const arrow_stream_holder &&) = delete;

    ArrowArrayStream * stream() noexcept
    {
        return &_stream;
    }
    const ArrowArrayStream * stream() const noexcept
    {
        return &_stream;
    }

    ArrowSchema * schema() noexcept
    {
        return &_schema;
    }
    const ArrowSchema * schema() const noexcept
    {
        return &_schema;
    }

private:
    void reset()
    {
        if (_stream.release)
        {
            _stream.release(&_stream);
            invalidate_stream();
        }
        if (_schema.release)
        {
            _schema.release(&_schema);
            invalidate_schema();
        }
    }

    void invalidate_stream() noexcept
    {
        _stream.release      = nullptr;
        _stream.get_next     = nullptr;
        _stream.get_schema   = nullptr;
        _stream.private_data = nullptr;
    }

    void invalidate_schema() noexcept
    {
        _schema.release      = nullptr;
        _schema.private_data = nullptr;
        _schema.format       = nullptr;
        _schema.name         = nullptr;
        _schema.metadata     = nullptr;
        _schema.flags        = 0;
        _schema.n_children   = 0;
        _schema.children     = nullptr;
        _schema.dictionary   = nullptr;
    }

    pybind11::object _reader;
    ArrowArrayStream _stream{};
    ArrowSchema _schema{};
};

struct arrow_batch
{
    arrow_stream_holder stream;
    std::vector<std::string> duplicate_names;
    std::vector<qdb_string_t> duplicate_columns;

    explicit arrow_batch(pybind11::object reader)
        : stream{std::move(reader)}
    {}

    qdb_exp_batch_push_arrow_t build(const std::string & table_name,
        const detail::deduplicate_options & dedup,
        qdb_ts_range_t * ranges)
    {
        qdb_exp_batch_push_arrow_t batch{};

        batch.name                  = table_name.data();
        batch.data.stream           = *stream.stream();
        batch.data.schema           = *stream.schema();
        batch.truncate_ranges       = ranges;
        batch.truncate_range_count  = (ranges == nullptr ? 0u : 1u);
        batch.where_duplicate       = nullptr;
        batch.where_duplicate_count = 0u;

        std::visit([&mode = dedup.mode_, &batch](
                       auto const & columns) { _set_deduplication_mode(mode, columns, batch); },
            dedup.columns_);

        return batch;
    }
};

} // namespace

void exp_batch_push_arrow_with_options(handle_ptr handle,
    const std::string & table_name,
    const pybind11::object & reader,
    pybind11::kwargs args)
{
    auto dedup = detail::deduplicate_options::from_kwargs(args);

    qdb_ts_range_t range{};
    qdb_ts_range_t * range_ptr = nullptr;

    if (args.contains("range"))
    {
        range = convert::value<pybind11::tuple, qdb_ts_range_t>(
            pybind11::cast<pybind11::tuple>(args["range"]));
        range_ptr = &range;
    }

    arrow_batch batch{reader};
    auto c_batch                    = batch.build(table_name, dedup, range_ptr);
    qdb_exp_batch_options_t options = detail::batch_options::from_kwargs(args);

    qdb::qdb_throw_if_error(
        *handle, qdb_exp_batch_push_arrow_with_options(*handle, &options, &c_batch, nullptr, 1u));
}

} // namespace qdb