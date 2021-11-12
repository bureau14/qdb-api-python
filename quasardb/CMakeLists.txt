cmake_minimum_required(VERSION 3.5.1)
project(quasardb)
set(CMAKE_EXPORT_COMPILE_COMMANDS TRUE)
set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)

option(QDB_LINK_STATIC_LIB "Link qdb_api_static instead of dynamic qdb_api." OFF)

set(PACKAGE_NAME quasardb)

# add_compile_options(
#   # $<$<CONFIG:Debug>:-DPy_DEBUG=1> # No effect?
#   $<$<CONFIG:Debug>:-D_DEBUG=1> # This will make link to pythonXY_d.lib instead of pythonXY.lib.
# )

# step 1: discover libraries
if(DEFINED ENV{QDB_API_PATH})
  message(STATUS "Getting qdb API from environment variable QDB_API_PATH: $ENV{QDB_API_PATH}")
  set(QDB_API_DIR $ENV{QDB_API_PATH})
else()
  set(QDB_API_DIR "${CMAKE_SOURCE_DIR}/../qdb")
endif()

find_library(
  QDB_API_LIB
  NAMES qdb_api
  PATHS ${QDB_API_DIR}/lib
  NO_DEFAULT_PATH)

if(QDB_API_LIB)
  if(NOT IS_DIRECTORY "${QDB_API_DIR}/include")
    message(FATAL_ERROR "Please unzip qdb c-api into ${QDB_API_DIR}")
  endif()
  include_directories(SYSTEM ${QDB_API_DIR}/include)
else()
  find_library(QDB_API_LIB NAMES qdb_api)
endif()

message(STATUS "Resolved QDB API library location: ${QDB_API_LIB}")

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

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  set(CLANG TRUE)
endif()

set(CMAKE_CXX_STANDARD 14)
if(CLANG OR CMAKE_COMPILER_IS_GNUCXX)
  add_compile_options(-Wno-register)
else()
  add_definitions(/DSTATIC_LINKED)
  add_definitions(/D_CRT_SECURE_NO_WARNINGS=1)
  add_compile_options(/wd5033) # register is no longer a supported storage class
  add_compile_options(/EHa)
  add_compile_options(/MT)
endif()

# Target core2 by default
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang" OR CMAKE_CXX_COMPILER_ID MATCHES "GNU")
  add_compile_options(
      -march=core2
  )
endif()

add_subdirectory(pybind11)

include_directories(SYSTEM qdb/include)

include_directories(${CMAKE_SOURCE_DIR})

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
    list(APPEND FILES_TO_COPY "${QDB_API_DIR}/bin/qdb_api.dll")
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

  message(STATUS "Files to copy: ${FILES_TO_COPY}")
  file(COPY ${FILES_TO_COPY} DESTINATION "${CMAKE_LIBRARY_OUTPUT_DIRECTORY}")

  # for Apple we need to change the id otherwise we won't be able to load the
  # extension
  if(APPLE)
    execute_process(COMMAND install_name_tool -id "@loader_path/libqdb_api.dylib"
                            ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/libqdb_api.dylib)
  endif()
endif()

# step 3: build
pybind11_add_module(
  quasardb
  cluster.hpp
  continuous.cpp
  continuous.hpp
  error.hpp
  entry.hpp
  blob.hpp
  handle.hpp
  options.hpp
  qdb_client.cpp
  query.hpp
  query.cpp
  table.hpp
  table_reader.hpp
  batch_inserter.hpp
  ts_convert.hpp
  tag.hpp
  utils.hpp
  version.hpp
  version.cpp
  logger.hpp
  logger.cpp
  detail/ts_column.hpp
  reader/ts_row.hpp
  reader/ts_value.hpp
  utils/blob_deque.hpp
  utils/ostream.hpp
  utils/permutation.hpp
  utils/stable_sort.hpp
)

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
