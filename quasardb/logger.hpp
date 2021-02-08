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

#include <qdb/log.h>
#include <pybind11/pybind11.h>
#include <atomic>
#include <mutex>

namespace qdb
{
namespace py = pybind11;

/**
 * Utility class that wraps around Python's logging module. Should *not*
 * be used from Python directly, and as such is not registered as a Python
 * class.
 *
 * Note that every log invocations calls a python function, causes a bunch
 * of reflections, and is generally as slow as you expect it to be. This cannot
 * be optimized.
 *
 * As such, try to keep this logging out of the critical performance path.
 */
class logger
{
public:
    /**
     * Default constructor, invoking any .debug() etc on a default initialized
     * instantiation of this class leads to undefined behavior, but it's needed
     * for certain situations.
     */
    logger(){};

    /**
     * Simple logger instance, all complexity is handled in the logging handlers.
     */
    logger(const std::string & module_name) :
        _module_name(module_name) {}

public:
    /**
     * Wraps logger.debug
     */
    template <typename... Args>
    void debug(Args &&... args) const
    {
        _log("debug",
             std::forward<Args>(args)...);
    }

    /**
     * Wraps logger.info
     */
    template <typename... Args>
    void info(Args &&... args) const
    {
        _log("info",
             std::forward<Args>(args)...);
    }

    /**
     * Wraps logger.warn
     */
    template <typename... Args>
    void warn(Args &&... args) const
    {
        _log("warning",
             std::forward<Args>(args)...);
    }

    /**
     * Wraps logger.error
     */
    template <typename... Args>
    void error(Args &&... args) const
    {
        _log("error",
             std::forward<Args>(args)...);
    }

    /**
     * Wraps logger.critical
     */
    template <typename... Args>
    void critical(Args &&... args) const
    {
        _log("critical",
             std::forward<Args>(args)...);
    }

private:
    template <typename... Args>
    void _log(Args &&... args) const
    {
        _do_log(_module_name,
                std::forward<Args>(args)...);
    }

    template <typename... Args>
    static void _do_log(std::string const & module_name,
                        char const * level,
                        const std::string & msg, Args &&... args)
    {
        /**
         * Calls Python imports, functions, etc, reflection kicks in, relatively slow.
         *
         * BUGFIX(leon) 2021-02-08
         *
         * We were observing crashes upon process exit, which were related to persistent
         * references to this logger being kept in logger.cpp. We got a double-free, due
         * to an incorrect reference count being observed.
         *
         * We now do all the reflection / lookups inside this function, which causes
         * a performance degradation, but makes reasoning over object ownership /
         * lifecycle much easier, thus no more double-free reference-counted issues.
         *
         * A future improvement would be to figure out how to properly deal with
         * Python's reference counting + persistent object ownership, possibly by
         * installing a cleanup handler.
         */
        py::module logging    = py::module::import("logging");
        py::object get_logger = logging.attr("getLogger");
        py::object logger     = get_logger(module_name);
        py::object logfn      = logger.attr(level);

        char const * errors = NULL;
        PyObject * buf = PyUnicode_DecodeLatin1(msg.data(), msg.size(), errors);
        assert(buf != NULL);
        assert(errors == NULL);

        logfn(py::str(buf), std::forward<Args>(args)...);
    }

private:
    std::string _module_name;
};

/**
 * QuasarDB's logging handler works with callbacks, and unfortunately is fairly
 * error prone to 'just keep exactly one callback': if there are no active
 * sessions left, the callback is removed.
 *
 * As such, the only way to bridge this logging API with QuasarDB's is to:
 *
 *  - frequently (i.e. every new connection) remove any existing callbacks and
 *    set our new callback.
 *
 *  - when a callback is invoked, keep a local container with buffered logs.
 *
 *  - from all other functions, basically after every native qdb call, flush all
 *    buffered logs.
 *
 *  - unfortunately, QuasarDB also buffers the logs before triggering callbacks,
 *    so it is unlikely that relevant logs are already present here right after an
 *    error. :'(
 *
 */

namespace native
{

typedef struct
{
    int year;
    int mon;
    int day;
    int hour;
    int min;
    int sec;
} message_time_t;

typedef struct
{
    qdb_log_level_t level;
    message_time_t timestamp;

    int pid;
    int tid;
    std::string message;

} message_t;

void swap_callback();

void flush();

void _callback(                  //
    qdb_log_level_t log_level,   // qdb log level
    const unsigned long * date,  // [years, months, day, hours, minute, seconds] (valid only in the context of the callback)
    unsigned long pid,           // process id
    unsigned long tid,           // thread id
    const char * message_buffer, // message buffer (valid only in the context of the callback)
    size_t message_size);        // message buffer size

/**
 * Implementation function of flush(), which assumes that locks have been
 * acquired and buffer actually contains logs.
 */
void _do_flush();
} // namespace native

} // namespace qdb
