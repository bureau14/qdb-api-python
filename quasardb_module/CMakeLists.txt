cmake_minimum_required(VERSION 2.8.12)
project(quasardb-module)

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
    )

target_link_libraries(quasardb PUBLIC ${QDB_API_LIB})

set_target_properties(quasardb PROPERTIES BUILD_WITH_INSTALL_RPATH 1)

if (APPLE)
    set_target_properties(quasardb PROPERTIES INSTALL_RPATH "@loader_path")
else()
    set_target_properties(quasardb PROPERTIES INSTALL_RPATH "$ORIGIN/")
endif()
