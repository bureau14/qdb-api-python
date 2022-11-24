# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# - Find ARROW (arrow/api.h, libarrow.a, libarrow.so)
# This module defines
#  ARROW_INCLUDE_DIR, directory containing headers
#  ARROW_LIBS, directory containing arrow libraries
#  ARROW_STATIC_LIB, path to libarrow.a
#  ARROW_SHARED_LIB, path to libarrow's shared library
#  ARROW_FOUND, whether arrow has been found

include(FindPkgConfig)

unset(ARROW_FOUND)

if (NOT "$ENV{ARROW_HOME}" STREQUAL "")
  set(ARROW_HOME "$ENV{ARROW_HOME}")
elseif(NOT "$ENV{CONDA_PREFIX}" STREQUAL "")
  set(ARROW_HOME "$ENV{CONDA_PREFIX}")
endif()

if (NOT ARROW_HOME)
  pkg_check_modules(ARROW arrow)
  if (ARROW_FOUND)
    pkg_get_variable(ARROW_ABI_VERSION arrow abi_version)
    message(STATUS "Arrow ABI version: ${ARROW_ABI_VERSION}")
    pkg_get_variable(ARROW_SO_VERSION arrow so_version)
    message(STATUS "Arrow SO version: ${ARROW_SO_VERSION}")
    set(ARROW_INCLUDE_DIR ${ARROW_INCLUDE_DIRS})
    set(ARROW_LIBS ${ARROW_LIBRARY_DIRS})
    set(ARROW_SEARCH_LIB_PATH ${ARROW_LIBRARY_DIRS})
  elseif(DEFINED ENV{VIRTUAL_ENV})
    find_path(ARROW_INCLUDE_DIR arrow/api.h HINTS
      $ENV{VIRTUAL_ENV}/Lib/site-packages/pyarrow/include)
    get_filename_component(ARROW_SEARCH_LIB_PATH ${ARROW_INCLUDE_DIR} DIRECTORY)
  else()
    if (MSVC)
      find_path(ARROW_INCLUDE_DIR arrow/api.h HINTS
        $ENV{PYTHON}/lib/site-packages/pyarrow/include)
    else()
      find_path(ARROW_INCLUDE_DIR arrow/api.h HINTS
        /usr/local/lib/*/dist-packages/pyarrow/include)
    endif()
    get_filename_component(ARROW_SEARCH_LIB_PATH ${ARROW_INCLUDE_DIR} DIRECTORY)
    set(ARROW_SEARCH_HEADER_PATHS ${ARROW_INCLUDE_DIR})
    message(STATUS "Found candidate Arrow location: ${ARROW_SEARCH_LIB_PATH}")
  endif()
else()
  set(ARROW_SEARCH_HEADER_PATHS
    ${ARROW_HOME}/include
    )

  set(ARROW_SEARCH_LIB_PATH
    ${ARROW_HOME}/lib
    )

  find_path(ARROW_INCLUDE_DIR arrow/array.h PATHS
    ${ARROW_SEARCH_HEADER_PATHS}
    # make sure we don't accidentally pick up a different version
    NO_DEFAULT_PATH
    )
endif()

if (MSVC)
  SET(CMAKE_FIND_LIBRARY_SUFFIXES ".lib" ".dll")
endif()

find_library(ARROW_LIB_PATH NAMES arrow
  PATHS
  ${ARROW_SEARCH_LIB_PATH}
  NO_DEFAULT_PATH)
message(STATUS "Found ${ARROW_LIB_PATH} in ${ARROW_SEARCH_LIB_PATH}")
get_filename_component(ARROW_LIBS ${ARROW_LIB_PATH} DIRECTORY)

find_library(ARROW_PYTHON_LIB_PATH NAMES arrow_python
  PATHS
  ${ARROW_SEARCH_LIB_PATH}
  NO_DEFAULT_PATH)

if (ARROW_INCLUDE_DIR AND ARROW_LIBS)
  set(ARROW_FOUND TRUE)

  set(ARROW_LIB_NAME arrow)
  set(ARROW_PYTHON_LIB_NAME arrow_python)

  if (MSVC)
    set(ARROW_STATIC_LIB ${ARROW_LIBS}/${ARROW_LIB_NAME}${ARROW_MSVC_STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX})
    set(ARROW_PYTHON_STATIC_LIB ${ARROW_LIBS}/${ARROW_PYTHON_LIB_NAME}${ARROW_MSVC_STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX})
    set(ARROW_SHARED_LIB ${ARROW_LIBS}/${ARROW_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
    set(ARROW_PYTHON_SHARED_LIB ${ARROW_LIBS}/${ARROW_PYTHON_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
    set(ARROW_SHARED_IMP_LIB ${ARROW_LIBS}/${ARROW_LIB_NAME}.lib)
    set(ARROW_PYTHON_SHARED_IMP_LIB ${ARROW_LIBS}/${ARROW_PYTHON_LIB_NAME}.lib)
  else()
    set(ARROW_STATIC_LIB ${ARROW_PYTHON_LIB_PATH}/libarrow.a)
    set(ARROW_PYTHON_STATIC_LIB ${ARROW_PYTHON_LIB_PATH}/libarrow_python.a)

    set(ARROW_SHARED_LIB ${ARROW_LIBS}/libarrow${CMAKE_SHARED_LIBRARY_SUFFIX})
    set(ARROW_PYTHON_SHARED_LIB ${ARROW_LIBS}/libarrow_python${CMAKE_SHARED_LIBRARY_SUFFIX})
  endif()
endif()

if (ARROW_FOUND)
  if (NOT Arrow_FIND_QUIETLY)
    message(STATUS "Found the Arrow core library: ${ARROW_LIB_PATH}")
    message(STATUS "Found the Arrow Python library: ${ARROW_PYTHON_LIB_PATH}")
  endif ()
else ()
  if (NOT Arrow_FIND_QUIETLY)
    set(ARROW_ERR_MSG "Could not find the Arrow library. Looked for headers")
    set(ARROW_ERR_MSG "${ARROW_ERR_MSG} in ${ARROW_SEARCH_HEADER_PATHS}, and for libs")
    set(ARROW_ERR_MSG "${ARROW_ERR_MSG} in ${ARROW_SEARCH_LIB_PATH}")
    if (Arrow_FIND_REQUIRED)
      message(FATAL_ERROR "${ARROW_ERR_MSG}")
    else (Arrow_FIND_REQUIRED)
      message(STATUS "${ARROW_ERR_MSG}")
    endif (Arrow_FIND_REQUIRED)
  endif ()
  set(ARROW_FOUND FALSE)
endif ()

mark_as_advanced(
  ARROW_INCLUDE_DIR
  ARROW_STATIC_LIB
  ARROW_SHARED_LIB
  ARROW_PYTHON_STATIC_LIB
  ARROW_PYTHON_SHARED_LIB
  ARROW_JEMALLOC_STATIC_LIB
  ARROW_JEMALLOC_SHARED_LIB
)
