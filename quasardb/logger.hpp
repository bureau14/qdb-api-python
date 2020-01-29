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

#include <pybind11/pybind11.h>

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
   * Acquire a logger instance for a module. This constructor is a relatively
   * expensive call, try to minimize invocations by re-using a logger accross
   * invocations.
   */
  logger(const std::string & module_name)
  {

    // TODO(leon): is there a way we can (safely) cache these calls? they should
    //             remain the same for every python VM instance... is it safe to cache?
    //
    // NOTE(leon): looking at the pybind11 code, it appears they never cache anything.. so
    //             that might hint that it's complex to get right
    py::module logging = py::module::import("logging");
    py::object get_logger = logging.attr("getLogger");

    _logger   = get_logger(module_name);

    /**
     * We cache all function pointer references for the various log levels, to speed
     * up the calls of the actually invocations below.
     */
    _debug    = _logger.attr("debug");
    _info     = _logger.attr("info");
    _warning  = _logger.attr("warning");
    _error    = _logger.attr("error");
    _critical = _logger.attr("critical");
  }


public:

  /**
   * Wraps logger.debug
   */
  template <typename... Args>
  void debug(Args&&... args) {
    _log(_debug, std::forward<Args>(args)...);
  }

  /**
   * Wraps logger.info
   */
  template <typename... Args>
  void info(Args&&... args) {
    _log(_info, std::forward<Args>(args)...);
  }

  /**
   * Wraps logger.warn
   */
  template <typename... Args>
  void warn(Args&&... args) {
    _log(_warning, std::forward<Args>(args)...);
  }

  /**
   * Wraps logger.error
   */
  template <typename... Args>
  void error(Args&&... args) {
    _log(_error, std::forward<Args>(args)...);
  }

  /**
   * Wraps logger.critical
   */
  template <typename... Args>
  void critical(Args&&... args) {
    _log(_critical, std::forward<Args>(args)...);
  }

private:
  template <typename... Args>
  static void _log(py::object level, const std::string & msg, Args&&... args) {
    /**
     * Calls Python function, reflection kicks in, relatively slow.
     */
    level(msg, std::forward<Args>(args)...);
  }

private:

  py::object _logger;
  py::object _debug;
  py::object _info;
  py::object _warning;
  py::object _error;
  py::object _critical;
};


} // namespace qdb
