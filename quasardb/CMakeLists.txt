cmake_minimum_required(VERSION 3.5.1)
project(quasardb)
set(CMAKE_EXPORT_COMPILE_COMMANDS TRUE)
set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)

set(PACKAGE_NAME quasardb)

# add_compile_options(
#   # $<$<CONFIG:Debug>:-DPy_DEBUG=1> # No effect?
#   $<$<CONFIG:Debug>:-D_DEBUG=1> # This will make link to pythonXY_d.lib instead of pythonXY.lib.
# )

# step 1: discover libraries
set(QDB_API_DIR "${CMAKE_SOURCE_DIR}/../qdb")
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

# step 3: build
pybind11_add_module(
  quasardb
  cluster.hpp
  error.hpp
  entry.hpp
  blob.hpp
  handle.hpp
  memcpy_wrap.cpp
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
  reader/ts_value.hpp)

target_compile_definitions(quasardb PUBLIC QDB_PY_VERSION="${QDB_PY_VERSION}")
target_link_libraries(quasardb PUBLIC ${QDB_API_LIB})

if(APPLE)
  set_target_properties(quasardb PROPERTIES INSTALL_RPATH "@loader_path")
else()
  set_target_properties(quasardb PROPERTIES INSTALL_RPATH "$ORIGIN/")
endif()
