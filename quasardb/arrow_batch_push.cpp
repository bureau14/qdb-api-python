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
#include <metrics.hpp>

namespace qdb
{

namespace
{

class arrow_stream_holder
{
public:
    explicit arrow_stream_holder(pybind11::object reader)
        : _reader{std::move(reader)}
    {
        _reader.attr("_export_to_c")(pybind11::int_(reinterpret_cast<uintptr_t>(&_stream)));
    }

    ~arrow_stream_holder()
    {
        reset();
    }

    void detach() noexcept
    {
        invalidate_stream();
    }

    arrow_stream_holder(const arrow_stream_holder &)             = delete;
    arrow_stream_holder & operator=(const arrow_stream_holder &) = delete;
    arrow_stream_holder(arrow_stream_holder &&)                  = delete;
    arrow_stream_holder & operator=(arrow_stream_holder &&)      = delete;

    ArrowArrayStream & stream() noexcept
    {
        return _stream;
    }

private:
    void reset() noexcept
    {
        if (_stream.release)
        {
            _stream.release(&_stream);
            invalidate_stream();
        }
    }

    void invalidate_stream() noexcept
    {
        _stream.release      = nullptr;
        _stream.get_next     = nullptr;
        _stream.get_schema   = nullptr;
        _stream.private_data = nullptr;
    }

    pybind11::object _reader;
    ArrowArrayStream _stream{};
};

struct arrow_batch
{
    arrow_stream_holder stream;
    std::vector<std::string> duplicate_names;
    std::vector<char const *> duplicate_ptrs;

    explicit arrow_batch(pybind11::object reader)
        : stream{std::move(reader)}
    {}

    static inline void set_deduplication_mode(
        detail::deduplication_mode_t mode, bool columns, qdb_exp_batch_push_arrow_t & out)
    {
        // Set deduplication mode only when `columns` is true, in which we will deduplicate based on
        // *all* columns.
        out.deduplication_mode =
            (columns == true ? detail::to_qdb(mode) : qdb_exp_batch_deduplication_mode_disabled);
    }

    inline void set_deduplication_mode(detail::deduplication_mode_t mode,
        std::vector<std::string> const & columns,
        qdb_exp_batch_push_arrow_t & out)
    {
        duplicate_names = columns; // save names to keep them alive

        duplicate_ptrs.resize(duplicate_names.size());
        std::transform(duplicate_names.begin(), duplicate_names.end(), duplicate_ptrs.begin(),
            [](std::string const & s) { return s.c_str(); });

        out.deduplication_mode    = detail::to_qdb(mode);
        out.where_duplicate       = duplicate_ptrs.data();
        out.where_duplicate_count = static_cast<qdb_size_t>(duplicate_ptrs.size());
    }

    qdb_exp_batch_push_arrow_t build(const std::string & table_name,
        const detail::deduplicate_options & dedup,
        qdb_ts_range_t * ranges,
        qdb_size_t ranges_count)
    {
        qdb_exp_batch_push_arrow_t batch{};

        batch.name                  = table_name.c_str();
        batch.stream                = stream.stream();
        batch.truncate_ranges       = ranges;
        batch.truncate_range_count  = (ranges == nullptr ? 0u : ranges_count);
        batch.where_duplicate       = nullptr;
        batch.where_duplicate_count = 0u;

        std::visit([&mode = dedup.mode_, &batch, this](
                       auto const & columns) { set_deduplication_mode(mode, columns, batch); },
            dedup.columns_);

        stream.detach();
        return batch;
    }
};

} // namespace

void exp_batch_push_arrow_with_options(handle_ptr handle,
    const std::string & table_name,
    const pybind11::object & reader,
    pybind11::kwargs args)
{
    auto dedup                      = detail::deduplicate_options::from_kwargs(args);
    qdb_exp_batch_options_t options = detail::batch_options::from_kwargs(args);

    std::vector<qdb_ts_range_t> truncate_ranges;
    qdb_ts_range_t * range_ptr = nullptr;

    if (options.mode == qdb_exp_batch_push_truncate)
        [[unlikely]] // Unlikely because truncate isn't used much
    {
        if (args.contains(detail::batch_truncate_ranges::kw_range))
        {
            truncate_ranges = detail::batch_truncate_ranges::from_kwargs(args);
            range_ptr       = truncate_ranges.data();
        }
        else
        {
            throw qdb::invalid_argument_exception{"No truncate range provided."};
        }
    }

    arrow_batch batch{reader};
    auto c_batch = batch.build(table_name, dedup, range_ptr, truncate_ranges.size());

    qdb::logger logger("quasardb.batch_push_arrow");
    logger.debug("Pushing Arrow stream in %s using %s push mode", table_name,
        detail::batch_push_mode::to_string(options.mode));
    qdb_error_t err{qdb_e_ok};
    {
        // Make sure to measure the time it takes to do the actual push.
        // This is in its own scoped block so that we only actually measure
        // the push time, not e.g. retry time.
        qdb::metrics::scoped_capture capture{"qdb_batch_push_arrow"};

        err = qdb_exp_batch_push_arrow_with_options(*handle, &options, &c_batch, nullptr, 1u);
    }

    auto retry_options = detail::retry_options::from_kwargs(args);
    if (retry_options.should_retry(err))
        [[unlikely]] // Unlikely, because err is most likely to be qdb_e_ok
    {
        if (err == qdb_e_async_pipe_full) [[likely]]
        {
            logger.info("Async pipelines are currently full");
        }
        else
        {
            logger.warn("A temporary error occurred");
        }

        std::chrono::milliseconds delay = retry_options.delay;
        logger.info("Sleeping for %d milliseconds", delay.count());

        std::this_thread::sleep_for(delay);

        // Now try again -- easier way to go about this is to enter recursion. Note how
        // we permutate the retry_options, which automatically adjusts the amount of retries
        // left and the next sleep duration.
        logger.warn("Retrying push operation, retries left: %d", retry_options.retries_left);
        err = qdb_exp_batch_push_arrow_with_options(*handle, &options, &c_batch, nullptr, 1u);
    }

    qdb::qdb_throw_if_error(*handle, err);
}

} // namespace qdb