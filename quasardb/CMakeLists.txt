cmake_minimum_required(VERSION 2.8.12)
project(quasardb)
set(CMAKE_EXPORT_COMPILE_COMMANDS TRUE)

set(PACKAGE_NAME quasardb)

# step 1: discover libraries
set(QDB_API_DIR "${CMAKE_SOURCE_DIR}/../qdb")
find_library(QDB_API_LIB NAMES qdb_api PATHS ${QDB_API_DIR}/lib NO_DEFAULT_PATH)

if (QDB_API_LIB)
    if(NOT IS_DIRECTORY "${QDB_API_DIR}/include")
        message(FATAL_ERROR "Please unzip qdb c-api into ${QDB_API_DIR}")
    endif()
    include_directories(SYSTEM
        ${QDB_API_DIR}/include
    )
else()
    find_library(QDB_API_LIB NAMES qdb_api)
  endif()

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    set(CLANG TRUE)
endif()

if(CLANG OR CMAKE_COMPILER_IS_GNUCXX)
    set(PYBIND11_CPP_STANDARD -std=c++14)
    add_compile_options(-Wno-register)
else()
    add_definitions(/DSTATIC_LINKED)
    add_definitions(/D_CRT_SECURE_NO_WARNINGS=1)
    add_compile_options(/wd5033) # register is no longer a supported storage class
    add_compile_options(/EHa)
    add_compile_options(/MT)
    set(PYBIND11_CPP_STANDARD /std:c++14)
endif()

add_subdirectory(pybind11)

find_library(QDB_API_LIB NAMES qdb_api PATHS qdb/lib NO_DEFAULT_PATH)

include_directories(SYSTEM
    qdb/include
)

# step 2: copy the libraries

if (WIN32)
    file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/../qdb/lib/qdb_api.lib" DESTINATION "${CMAKE_BINARY_DIR}/quasardb/qdb/lib")
    file(COPY "${QDB_API_DIR}/bin/qdb_api.dll" DESTINATION "${PACKAGE_NAME}")
elseif(APPLE)
    file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/../qdb/lib/libqdb_api.dylib" DESTINATION "${CMAKE_BINARY_DIR}/quasardb/qdb/lib")
    file(COPY "${QDB_API_DIR}/lib/libqdb_api.dylib"  DESTINATION "${PACKAGE_NAME}")
    file(COPY "${QDB_API_DIR}/lib/libc++.1.dylib"    DESTINATION "${PACKAGE_NAME}")
    file(COPY "${QDB_API_DIR}/lib/libc++abi.1.dylib" DESTINATION "${PACKAGE_NAME}")
elseif("${CMAKE_SYSTEM_NAME}" STREQUAL "FreeBSD")
    file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/qdb/lib/libqdb_api.so" DESTINATION "${CMAKE_BINARY_DIR}/quasardb/qdb/lib")
    file(COPY "${QDB_API_DIR}/lib/libqdb_api.so" DESTINATION "${PACKAGE_NAME}")
    file(COPY "${QDB_API_DIR}/lib/libc++.so" DESTINATION "${PACKAGE_NAME}")
    file(COPY "${QDB_API_DIR}/lib/libc++abi.so" DESTINATION "${PACKAGE_NAME}")
else()
    file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/../qdb/lib/libqdb_api.so" DESTINATION "${CMAKE_BINARY_DIR}/quasardb/qdb/lib")
    file(COPY "${QDB_API_DIR}/lib/libqdb_api.so" DESTINATION "${PACKAGE_NAME}")
endif()

# for Apple we need to change the id otherwise we won't be able to load the extension
if (APPLE)
    execute_process(COMMAND install_name_tool -id "@loader_path/libqdb_api.dylib" ${CMAKE_CURRENT_BINARY_DIR}/${PACKAGE_NAME}/libqdb_api.dylib)
endif()

# step 3: build
pybind11_add_module(quasardb
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
    ts.hpp
    ts_batch.hpp
    ts_convert.hpp
    tag.hpp
    utils.hpp
    version.hpp
    version.cpp
    )

target_compile_definitions(quasardb PUBLIC QDB_PY_VERSION="${QDB_PY_VERSION}")
target_link_libraries(quasardb PUBLIC ${QDB_API_LIB})

set_target_properties(quasardb PROPERTIES BUILD_WITH_INSTALL_RPATH 1)

if (APPLE)
    set_target_properties(quasardb PROPERTIES INSTALL_RPATH "@loader_path")
else()
    set_target_properties(quasardb PROPERTIES INSTALL_RPATH "$ORIGIN/")
endif()
