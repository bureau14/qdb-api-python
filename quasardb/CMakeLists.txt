cmake_minimum_required(VERSION 3.9.4)
project(quasardb)
set(CMAKE_EXPORT_COMPILE_COMMANDS TRUE)
set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
set(CMAKE_OSX_DEPLOYMENT_TARGET "10.14")
set(CMAKE_VERBOSE_MAKEFILE FALSE)

include(CheckIPOSupported)

option(QDB_LINK_STATIC_LIB "Link qdb_api_static instead of dynamic qdb_api." OFF)
option(QDB_TESTS_ENABLED "Build quasardb test suite?" OFF)

set(PACKAGE_NAME quasardb)

if("${CMAKE_SYSTEM_PROCESSOR}" MATCHES "arm"
        OR "${CMAKE_SYSTEM_PROCESSOR}" STREQUAL "aarch64"
        OR "${CMAKE_SYSTEM_PROCESSOR}" MATCHES "cortex"
        OR CMAKE_GENERATOR MATCHES "ARM"
        OR CMAKE_GENERATOR_PLATFORM MATCHES "ARM")
    set(QDB_CPU_IS_ARM ON)
elseif("${CMAKE_SYSTEM_PROCESSOR}" STREQUAL "x86"
        OR "${CMAKE_SYSTEM_PROCESSOR}" STREQUAL "x86_64"
        OR "${CMAKE_SYSTEM_PROCESSOR}" STREQUAL "i686"
        OR "${CMAKE_SYSTEM_PROCESSOR}" STREQUAL "AMD64"
        OR "${CMAKE_SYSTEM_PROCESSOR}" STREQUAL "amd64")
    set(QDB_CPU_IS_X86 ON)
else()
    message(FATAL_ERROR "Unknown CMAKE_SYSTEM_PROCESSOR = ${CMAKE_SYSTEM_PROCESSOR}")
endif()

###
#
# Discover QuasarDB api location, set QDB_API_DIR accordingly.
#
###

set(QDB_API_NAME qdb_api)

message(STATUS "[qdb-api-python] Using qdb api name ${QDB_API_NAME}")

if(DEFINED ENV{QDB_API_PATH})
  message(STATUS "[qdb-api-python] Getting qdb API from environment variable QDB_API_PATH: $ENV{QDB_API_PATH}")
  set(QDB_API_DIR $ENV{QDB_API_PATH})
else()
  set(QDB_API_DIR "${CMAKE_SOURCE_DIR}/../qdb")
endif()

find_library(
  QDB_API_LIB
  NAMES ${QDB_API_NAME}
  PATHS ${QDB_API_DIR}/lib
  NO_DEFAULT_PATH)

if(QDB_API_LIB)
  if(NOT IS_DIRECTORY "${QDB_API_DIR}/include")
    message(FATAL_ERROR "[qdb-api-python] Please unzip qdb c-api into ${QDB_API_DIR}")
  endif()
  include_directories(SYSTEM ${QDB_API_DIR}/include)
else()
  find_library(QDB_API_LIB NAMES ${QDB_API_NAME})
endif()

message(STATUS "[qdb-api-python] Resolved QDB API library location: ${QDB_API_LIB}")

if(NOT QDB_API_LIB)
  message(
    FATAL_ERROR
      "\n\
                      ************************************************************************** \n\
                      \n\
                      Unable to locate QuasarDB library: please install the QuasarDB API library. \n\
                      \n\
                      For more information, please consult the manual: \n\
                        https://doc.quasardb.net/master/api/c.html. \n\
                      \n\
                      **************************************************************************"
  )
endif()





###
#
# High-level compiler/OS-independent cmake options
#
###

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)


###
#
# Probe compiler/OS
#
###

if ("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
  message(STATUS "[qdb-api-python] Detected: clang compiler")
  set(CLANG TRUE)
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  message(STATUS "[qdb-api-python] Detected: gcc")
  set(GNU TRUE)
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
  message(STATUS "[qdb-api-python] Detected: msvc")
  set(MSVC TRUE)
  set(WINDOWS TRUE)

  if(CMAKE_SIZEOF_VOID_P EQUAL 8)
    message(STATUS "[qdb-api-python] Detected: win64")
    set(WIN64 TRUE)
  elseif(CMAKE_SIZEOF_VOID_P EQUAL 4)
    message(STATUS "[qdb-api-python] Detected: win32")
    set(WIN32 TRUE)
  endif()
endif()

if (CMAKE_HOST_APPLE)
  message(STATUS "[qdb-api-python] Detected: apple host")
  set(APPLE TRUE)
elseif(CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
  message(STATUS "[qdb-api-python] Detected: freebsd")
  set(FREEEBSD TRUE)
elseif(CMAKE_SYSTEM_NAME MATCHES "Linux")
  message(STATUS "[qdb-api-python] Detected: linux")
  set(LINUX TRUE)
endif()


###
#
# Compile options based on compiler/OS
#
###

if(CLANG OR GNU)
  if(CLANG AND NOT APPLE)
    # Clang + Ninja eats colors
    add_compile_options(
      -fdiagnostics-color=always
      -ftemplate-backtrace-limit=0
      -fmacro-backtrace-limit=0)
  endif()

  if (LINUX)
    add_compile_options(
      -Wno-register
      -Wall
      -fconcepts-diagnostics-depth=3

      $<$<CONFIG:Debug>:-Og>
      $<$<CONFIG:Release>:-O3>
      )

    # Can we perhaps make different builds, one for core2 and another
    # for haswell?

    if(QDB_CPU_IS_X86)
      add_compile_options(-march=core2)
    endif()
  endif()

  if (CLANG)
    add_compile_options(
      -fcolor-diagnostics # force color output on ninja
      -fmacro-backtrace-limit=0
      $<$<CONFIG:Release>:-O3>
      $<$<OR:$<CONFIG:Debug>,$<CONFIG:RelWithDebInfo>>:-g>
      $<$<CONFIG:Release>:-ftree-vectorize>)
  elseif (GNU)
    add_compile_options(
      -fdata-sections
      -ffunction-sections
      -fno-stack-protector

      -fnon-call-exceptions
      -ftrack-macro-expansion=0
      $<$<CONFIG:Release>:-O3>
      $<$<CONFIG:Debug>:-Og>
      $<$<CONFIG:Debug>:-fno-eliminate-unused-debug-types>
      $<$<OR:$<CONFIG:Debug>,$<CONFIG:RelWithDebInfo>>:-ggdb>)
  endif()


elseif (MSVC)
  message(STATUS "[qdb-api-python] Detected: msvc")
  add_definitions(/DSTATIC_LINKED)
  add_definitions(/D_CRT_SECURE_NO_WARNINGS=1)

  add_compile_options(
    /volatile:iso
    /Gy
    /Zc:wchar_t
    /wd5033
    /MT
    /EHa
    /GR
    /GF

    $<$<OR:$<CONFIG:Debug>,$<CONFIG:RelWithDebInfo>>:/Zi> # Produces a program database (PDB) that contains type information and symbolic debugging information for use with the debugger

    # Debug: Turns off all optimizations in the program and speeds compilation.
    # Non-debug: Selects full optimization.
    $<IF:$<CONFIG:Debug>,/Od,/Ox>
    $<$<CONFIG:Debug>:/RTC1>       # Enable the run-time error checks feature, in conjunction with the runtime_checks pragma.

    $<$<NOT:$<CONFIG:Debug>>:/Oi>  # Replaces some function calls with intrinsic or otherwise special forms of the function that help your application run faster.
    $<$<NOT:$<CONFIG:Debug>>:/Ot>  # Maximizes the speed of EXEs and DLLs by instructing the compiler to favor speed over size.
    $<$<NOT:$<CONFIG:Debug>>:/Oy>  # Suppresses creation of frame pointers on the call stack.
    $<$<NOT:$<CONFIG:Debug>>:/GS-> # Suppresses Buffer Security Check


  )
  if(QDB_CPU_IS_X86)
    add_compile_options(
      # Enable AVX as we target Core2 and later
      /arch:AVX
    )
  endif()
endif()

###
#
# Link options based on compiler/OS
#
###


if (CLANG)
  if(QDB_LINKER)
    if(QDB_LINKER STREQUAL "lld")
      if(APPLE)
        message(STATUS "[qdb-api-python] Detected: clang + lld + apple")
        add_link_options(--ld-path=ld64.lld)
      elseif(WIN32)
        message(STATUS "[qdb-api-python] Detected: clang + lld + win32")
        add_link_options(--ld-path=lld-link)
      else()
        message(STATUS "[qdb-api-python] Detected: clang + lld (other)")
        add_link_options(--ld-path=ld.lld)
      endif()
    else()
      message(STATUS "[qdb-api-python] Detected: clang + qdb_linker (other)")
      add_link_options(--ld-path=${QDB_LINKER})
    endif()
  endif()

  # clang + anything not apple
  if(NOT APPLE)
    message(STATUS "[qdb-api-python] Detected: clang + not apple")
    add_link_options(
      -Qunused-arguments
      -Wl,--gc-sections)
  endif()
endif()

###
#
# Third-party libraries
#
###

message(STATUS "[qdb-api-python] Including thirdparty libraries with binary directory ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}")
include_directories(SYSTEM ../thirdparty/date/include)
include_directories(SYSTEM ../thirdparty/range-v3/include)

add_subdirectory(../thirdparty/ ${CMAKE_LIBRARY_OUTPUT_DIRECTORY})

include_directories(SYSTEM qdb/include)
include_directories(${CMAKE_SOURCE_DIR})
include_directories(../quasardb/)


#
# step 2: copy the libraries
#
# Based on the OS, we copy the relevant files into our output directory. This
# matches the `extdir` as defined in setup.py's CMakeBuild.build_extension().

# We need to detect the _actual_ location of the qdb api lib file, lest we
# accidentally copy the symlink instead of the file.
get_filename_component(QDB_API_LIB_REAL ${QDB_API_LIB} REALPATH)
get_filename_component(QDB_API_LIB_REAL_NAME ${QDB_API_LIB_REAL} NAME)

if(NOT QDB_LINK_STATIC_LIB)
  set(FILES_TO_COPY)

  if(WIN32)
    list(APPEND FILES_TO_COPY "${QDB_API_DIR}/bin/${QDB_API_NAME}.dll")
  elseif(APPLE)
    file(
      GLOB SO_LIBS
      LIST_DIRECTORIES false
      "${QDB_API_DIR}/lib/libc[x+][x+]*.dylib")

    list(APPEND FILES_TO_COPY "${QDB_API_LIB_REAL}" ${SO_LIBS})
  elseif("${CMAKE_SYSTEM_NAME}" STREQUAL "FreeBSD")
    file(
      GLOB SO_LIBS
      LIST_DIRECTORIES false
      "${QDB_API_DIR}/lib/libc[x+][x+]*.so*")

    list(APPEND FILES_TO_COPY "${QDB_API_LIB_REAL}" ${SO_LIBS})
  else()
    list(APPEND FILES_TO_COPY "${QDB_API_LIB_REAL}")
    # Is there a case where the C API library is not called libqdb_api.so?
    # file(RENAME "${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/${QDB_API_LIB_REAL_NAME}"
    # "${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/libqdb_api.so")
  endif()

  message(STATUS "[qdb-api-python] Files to copy: ${FILES_TO_COPY}")
  file(COPY ${FILES_TO_COPY} DESTINATION "${CMAKE_LIBRARY_OUTPUT_DIRECTORY}")

  # for Apple we need to change the id otherwise we won't be able to load the
  # extension
  if(APPLE)
    execute_process(COMMAND install_name_tool -id "@loader_path/lib${QDB_API_NAME}.dylib"
                            ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/lib${QDB_API_NAME}.dylib)
  endif()
endif()

set(QDB_FILES
  batch_inserter.hpp
  blob.hpp
  cluster.hpp
  concepts.hpp
  continuous.cpp
  continuous.hpp
  convert.hpp
  double.hpp
  error.hpp
  entry.hpp
  handle.hpp
  logger.hpp
  logger.cpp
  masked_array.hpp
  module.hpp
  module.cpp
  node.hpp
  numpy.hpp
  numpy.cpp
  object_tracker.hpp
  options.hpp
  pinned_writer.cpp
  pinned_writer.hpp
  query.hpp
  query.cpp
  string.hpp
  table.cpp
  table.hpp
  table_reader.hpp
  tag.hpp
  timestamp.hpp
  traits.hpp
  utils.cpp
  utils.hpp
  detail/qdb_resource.hpp
  detail/ts_column.hpp
  reader/ts_row.hpp
  reader/ts_value.hpp
  utils/blob_deque.hpp
  utils/ostream.hpp
  utils/permutation.hpp
  utils/stable_sort.hpp
)

set(QDB_TEST_FILES "")

if(QDB_TESTS_ENABLED)
  message(WARNING "\
------------------------------------------------------------------ \
[qdb-api-python] Tests enabled -- do not enable for release builds \
------------------------------------------------------------------ \ ")
  set(QDB_TEST_FILES

    ../tests/conftest.cpp
    ../tests/conftest.hpp
    ../tests/test_convert.cpp)
endif()

# step 3: build
pybind11_add_module(
  quasardb

  ${QDB_FILES}
  ${QDB_TEST_FILES})

target_compile_definitions(quasardb PUBLIC QDB_PY_VERSION="${QDB_PY_VERSION}")
if(QDB_LINK_STATIC_LIB)
  add_definitions(-DQDB_API_STATIC_LINK=1)

  set(LIB_DIR "${QDB_API_DIR}/lib_static")
  if(WIN32)
    set(LIB_PREFIX "")
    set(LIB_SUFFIX "$<$<CONFIG:Debug>:d>.lib")
  else()
    set(LIB_PREFIX "lib")
    set(LIB_SUFFIX "$<$<CONFIG:Debug>:d>.a")
  endif()

  target_link_libraries(quasardb
    PUBLIC
      "${LIB_DIR}/${LIB_PREFIX}qdb_api_static${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}qdb_query_client${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}qdb_client${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}qdb_aggregation${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}qdb_chord${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}qdb_network${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}qdb_protocol${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}qdb_perf${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}qdb_persistence${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}qdb_application${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}rocksdb${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}zlibstatic${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}qdb_query${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}geohash${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}qdb_timeseries${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}qdb_query_dsl${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}arrow${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}qdb_metadata${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}qdb_config${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}boost_program_options${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}qdb_serialization${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}qdb_compression${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}qdb_serialization${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}qdb_compression${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}qdb_auth${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}qdb_crypto${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}qdb_json${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}qdb_id${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}skein${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}qdb_network_resolver${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}libsodium${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}qdb_io${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}qdb_log${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}lz4${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}qdb_sys${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}qdb_time${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}brigand${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}qdb_util${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}boost_filesystem${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}fmt${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}robin_hood${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}asio${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}xxhash${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}qdb_memory${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}tbbmalloc${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}tbb${LIB_SUFFIX}"
      "${LIB_DIR}/${LIB_PREFIX}qdb_version${LIB_SUFFIX}"
  )

  if(WIN32)
    target_link_libraries(quasardb
      PUBLIC
        "${LIB_DIR}/${LIB_PREFIX}zstd_static${LIB_SUFFIX}"
    )
  else()
    target_link_libraries(quasardb
      PUBLIC
        "${LIB_DIR}/${LIB_PREFIX}boost_system${LIB_SUFFIX}"
        "${LIB_DIR}/${LIB_PREFIX}zstd${LIB_SUFFIX}"
    )
  endif()
else()
  target_link_libraries(quasardb PUBLIC ${QDB_API_LIB})
endif()

if(CMAKE_COMPILER_IS_GNUCXX)
  target_link_options(
    quasardb
    PUBLIC
    -static-libgcc
    -static-libstdc++
  )
endif()

if(APPLE)
  set_target_properties(quasardb PROPERTIES INSTALL_RPATH "@loader_path")
else()
  set_target_properties(quasardb PROPERTIES INSTALL_RPATH "$ORIGIN/")
endif()

if (CLANG OR GNU)
  if (NOT APPLE)
    check_ipo_supported(RESULT LTO_SUPPORTED OUTPUT error)
    if (LTO_SUPPORTED)
      message(STATUS "[qdb-api-python] Enabling LTO")
      set_target_properties(quasardb PROPERTIES INTERPROCEDURAL_OPTIMIZATION TRUE)
    else()
      message(STATUS "[qdb-api-python] Disabling LTO")
      set_target_properties(quasardb PROPERTIES INTERPROCEDURAL_OPTIMIZATION FALSE)
    endif()
  endif()
endif()
