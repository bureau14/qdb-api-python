/*
 *
 * Official Python API
 *
 * Copyright (c) 2009-2024, quasardb SAS. All rights reserved.
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

#include "concepts.hpp"
#include "error.hpp"
#include "logger.hpp"
#include "metrics.hpp"
#include "object_tracker.hpp"
#include "detail/writer.hpp"
#include <vector>

namespace qdb
{
class writer
{

    using int64_column     = detail::int64_column;
    using double_column    = detail::double_column;
    using timestamp_column = detail::timestamp_column;
    using blob_column      = detail::blob_column;
    using string_column    = detail::string_column;
    using any_column       = detail::any_column;

public:
public:
    writer(qdb::handle_ptr h)
        : _logger("quasardb.writer")
        , _handle{h}
    {}

    // prevent copy because of the table object, use a unique_ptr of the batch in cluster
    // to return the object
    writer(const writer &) = delete;

    ~writer()
    {}

    const std::vector<qdb_exp_batch_push_column_t> & prepare_columns();

    template <                                            //
        qdb::concepts::writer_push_strategy PushStrategy, //
        qdb::concepts::sleep_strategy SleepStrategy>      //
    void push(detail::writer_data const & data, py::kwargs kwargs)
    {
        qdb::object_tracker::scoped_capture capture{_object_tracker};

        // We always want to have a push mode at this point
        kwargs = detail::batch_push_mode::ensure(kwargs);

        _push_impl<PushStrategy, SleepStrategy>( //
            detail::staged_tables::index(data),  //
            kwargs                               //
        );                                       //
    }

    template <                                            //
        qdb::concepts::writer_push_strategy PushStrategy, //
        qdb::concepts::sleep_strategy SleepStrategy>      //
    void push_async(detail::writer_data const & data, py::kwargs kwargs)
    {
        _logger.warn("writer.push_async() is deprecated, please invoke writer.push() directly and "
                     "provide the push mode as a kwarg");

        return push<PushStrategy, SleepStrategy>(
            data, detail::batch_push_mode::set(kwargs, qdb_exp_batch_push_async));
    }

    template <                                            //
        qdb::concepts::writer_push_strategy PushStrategy, //
        qdb::concepts::sleep_strategy SleepStrategy>      //
    void push_fast(detail::writer_data const & data, py::kwargs kwargs)
    {
        _logger.warn("writer.push_fast() is deprecated, please invoke writer.push() directly and "
                     "provide the push mode as a kwarg");

        return push<PushStrategy, SleepStrategy>(
            data, detail::batch_push_mode::set(kwargs, qdb_exp_batch_push_fast));
    }

    template <                                            //
        qdb::concepts::writer_push_strategy PushStrategy, //
        qdb::concepts::sleep_strategy SleepStrategy>      //
    void push_truncate(detail::writer_data const & data, py::kwargs kwargs)
    {
        _logger.warn("writer.push_fast() is deprecated, please invoke writer.push() directly and "
                     "provide the push mode as a kwarg");

        return push<PushStrategy, SleepStrategy>(
            data, detail::batch_push_mode::set(kwargs, qdb_exp_batch_push_truncate));

        // qdb::object_tracker::scoped_capture capture{_object_tracker};
        // auto idx = detail::staged_tables::index(data);

        // // As we are actively removing data, let's add an additional check to ensure the user
        // // doesn't accidentally truncate his whole database without inserting anything.
        // if (data.empty()) [[unlikely]]
        // {
        //     throw qdb::invalid_argument_exception{
        //         "Writer is empty: you did not provide any rows to push."};
        // }

        // qdb_ts_range_t tr;

        // if (kwargs.contains("range"))
        // {
        //     tr = convert::value<py::tuple, qdb_ts_range_t>(py::cast<py::tuple>(kwargs["range"]));
        // }
        // else
        // {
        //     // TODO(leon): support multiple tables for push truncate
        //     if (idx.size() != 1) [[unlikely]]
        //     {
        //         throw qdb::invalid_argument_exception{"Writer push truncate only supports a single "
        //                                               "table unless an explicit range is provided:
        //                                               you " "provided more than one table without" "
        //                                               an explicit range."};
        //     }

        //     detail::staged_table const & staged_table = idx.first();
        //     tr                                        = staged_table.time_range();
        // }

        // const qdb_exp_batch_options_t options = {
        //     .mode       = qdb_exp_batch_push_truncate,                  //
        //     .push_flags = detail::batch_push_flags::from_kwargs(kwargs) //
        // };

        // _push_impl<PushStrategy, SleepStrategy>( //
        //     std::move(idx),                      //
        //     options,                             //
        //     kwargs,                              //
        //     &tr                                  //
        // );                                       //
    }

private:
    template <                                       //
        concepts::writer_push_strategy PushStrategy, //
        concepts::sleep_strategy SleepStrategy>      //
    void _push_impl(                                 //
        detail::staged_tables && idx,                //
        py::kwargs kwargs)                           //
    {
        _handle->check_open();

        // Ensure some default variables that are set
        kwargs = detail::batch_push_flags::ensure(kwargs);

        std::vector<qdb_ts_range_t> truncate_ranges{};

        if (detail::batch_push_mode::from_kwargs(kwargs) == qdb_exp_batch_push_truncate)
            [[unlikely]] // Unlikely because truncate isn't used much
        {
            kwargs          = detail::batch_truncate_ranges::ensure(kwargs, idx);
            truncate_ranges = detail::batch_truncate_ranges::from_kwargs(kwargs);
        }

        qdb_exp_batch_options_t options = detail::batch_options::from_kwargs(kwargs);

        if (idx.empty()) [[unlikely]]
        {
            throw qdb::invalid_argument_exception{"No data written to batch writer."};
        }

        auto deduplicate_options = detail::deduplicate_options::from_kwargs(kwargs);

        std::vector<qdb_exp_batch_push_table_t> batch;
        batch.assign(idx.size(), qdb_exp_batch_push_table_t());

        qdb_ts_range_t * truncate_ranges_{nullptr};
        if (truncate_ranges.empty() == false) [[unlikely]]
        {
            truncate_ranges_ = truncate_ranges.data();
        }

        int cur = 0;

        for (auto pos = idx.begin(); pos != idx.end(); ++pos)
        {
            std::string const & table_name      = pos->first;
            detail::staged_table & staged_table = pos->second;
            auto & batch_table                  = batch.at(cur++);

            staged_table.prepare_batch( //
                options.mode,           //
                deduplicate_options,    //
                truncate_ranges_,       //
                batch_table);

            if (batch_table.data.column_count == 0) [[unlikely]]
            {
                throw qdb::invalid_argument_exception{
                    "Writer is empty: you did not provide any columns to push."};
            }

            _logger.debug("Pushing %d rows with %d columns in %s", batch_table.data.row_count,
                batch_table.data.column_count, table_name);
        }

        _do_push<PushStrategy, SleepStrategy>(         //
            options,                                   //
            batch,                                     //
            PushStrategy::from_kwargs(kwargs),         //
            detail::retry_options::from_kwargs(kwargs) //
        );                                             //
    }

    template <                                       //
        concepts::writer_push_strategy PushStrategy, //
        concepts::sleep_strategy SleepStrategy>      //
    void _do_push(qdb_exp_batch_options_t const & options,
        std::vector<qdb_exp_batch_push_table_t> const & batch,
        PushStrategy push_strategy,
        detail::retry_options const & retry_options)
    {
        qdb_error_t err{qdb_e_ok};

        {
            // Make sure to measure the time it takes to do the actual push.
            // This is in its own scoped block so that we only actually measure
            // the push time, not e.g. retry time.
            qdb::metrics::scoped_capture capture{"qdb_batch_push"};

            err = push_strategy( //
                *_handle,        //
                &options,        //
                batch.data(),    //
                nullptr,         //
                batch.size());   //
        }

        if (retry_options.should_retry(err))
            [[unlikely]] // Unlikely, because err is most likely to be qdb_e_ok
        {
            if (err == qdb_e_async_pipe_full) [[likely]]
            {
                _logger.info("Async pipelines are currently full");
            }
            else
            {
                _logger.warn("A temporary error occurred");
            }

            std::chrono::milliseconds delay = retry_options.delay;
            _logger.info("Sleeping for %d milliseconds", delay.count());

            SleepStrategy::sleep(delay);

            // Now try again -- easier way to go about this is to enter recursion. Note how
            // we permutate the retry_options, which automatically adjusts the amount of retries
            // left and the next sleep duration.
            _logger.warn("Retrying push operation, retries left: %d", retry_options.retries_left);
            return _do_push<PushStrategy, SleepStrategy>(
                options, batch, push_strategy, retry_options.next());
        }

        qdb::qdb_throw_if_error(*_handle, err);
    }

private:
    qdb::logger _logger;
    qdb::handle_ptr _handle;

    qdb::object_tracker::scoped_repository _object_tracker;

public:
    // the 'legacy' API needs some state attached to the pinned writer; monkey patching
    // the pinned writer purely in python for this is possible, but annoying to do right;
    // it's much easier to just have it live here (for now), until the batch writer is
    // fully deprecated.
    py::dict legacy_state_;
};

// don't use shared_ptr, let Python do the reference counting, otherwise you will have an undefined
// behavior
using writer_ptr = std::unique_ptr<writer>;

template <                                            //
    qdb::concepts::writer_push_strategy PushStrategy, //
    qdb::concepts::sleep_strategy SleepStrategy>      //
static void register_writer(py::module_ & m)
{
    using PS = PushStrategy;
    using SS = SleepStrategy;

    namespace py = pybind11;

    // Writer data
    auto writer_data_c = py::class_<qdb::detail::writer_data>{m, "WriterData"};
    writer_data_c.def(py::init())
        .def("append", &qdb::detail::writer_data::append, py::arg("table"), py::arg("index"),
            py::arg("column_data"), "Append new data")
        .def("empty", &qdb::detail::writer_data::empty, "Returns true if underlying data is empty");

    // Different push modes, makes it easy to convert to<>from native types, as we accept the push
    // mode as a kwarg.
    py::enum_<qdb_exp_batch_push_mode_t>{m, "WriterPushMode", py::arithmetic(), "Push Mode"} //
        .value("Transactional", qdb_exp_batch_push_transactional)                            //
        .value("Fast", qdb_exp_batch_push_fast)                                              //
        .value("Truncate", qdb_exp_batch_push_truncate)                                      //
        .value("Async", qdb_exp_batch_push_fast);                                            //

    // And the actual pinned writer
    auto writer_c = py::class_<qdb::writer>{m, "Writer"};

    // basic interface
    writer_c.def(py::init<qdb::handle_ptr>()); //

    writer_c.def_readwrite("_legacy_state", &qdb::writer::legacy_state_);

    // push functions
    writer_c
        .def("push", &qdb::writer::push<PS, SS>, "Regular batch push") //
        .def("push_async", &qdb::writer::push_async<PS, SS>,
            "Asynchronous batch push that buffers data inside the QuasarDB daemon") //
        .def("push_fast", &qdb::writer::push_fast<PS, SS>,
            "Fast, in-place batch push that is efficient when doing lots of small, incremental "
            "pushes.") //
        .def("push_truncate", &qdb::writer::push_truncate<PS, SS>,
            "Before inserting data, truncates any existing data. This is useful when you want your "
            "insertions to be idempotent, e.g. in "
            "case of a retry.");
}

} // namespace qdb
