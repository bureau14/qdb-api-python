# We have a two stages build process
# This CMakeLists.txt will create the structure that will enable setuptools to build the extension
# the quasardb_module directory contains the actual source code of the module and is built by setuptools
# this enables us to create a source distribution of the quasardb API

cmake_minimum_required(VERSION 2.8.12)
set(CMAKE_EXPORT_COMPILE_COMMANDS TRUE)
project(qdb-api-python)
enable_testing()

set(QDB_BIN_DIR "${CMAKE_CURRENT_SOURCE_DIR}/bin")

if(DEFINED ENV{PYTHON_EXECUTABLE})
    if("$ENV{PYTHON_EXECUTABLE}" STREQUAL "python2")
        find_package(PythonInterp 2.7 REQUIRED)
    elseif("$ENV{PYTHON_EXECUTABLE}" STREQUAL "python3")
        find_package(PythonInterp 3.6 REQUIRED)
    else()
        set(PYTHON_EXECUTABLE "$ENV{PYTHON_EXECUTABLE}")
    endif()
else()
    find_package(PythonInterp REQUIRED)
endif()

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    set(CLANG TRUE)
endif()

set(QDB_API_DIR "${CMAKE_SOURCE_DIR}/qdb")
if(NOT IS_DIRECTORY "${QDB_API_DIR}/include")
    message(FATAL_ERROR "Please unzip qdb c-api into ${QDB_API_DIR}")
endif()

include_directories(SYSTEM
    ${QDB_API_DIR}/include
)

find_library(QDB_API_LIB NAMES qdb_api PATHS ${QDB_API_DIR}/lib NO_DEFAULT_PATH)

file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/quasardb_module" DESTINATION "${CMAKE_BINARY_DIR}")
file(COPY "${QDB_API_DIR}/include" DESTINATION "${CMAKE_BINARY_DIR}/quasardb_module/qdb")

# add_subdirectory(src)

set(QDB_PY_VERSION "3.1.0")

set(QDB_DAEMON "${QDB_API_DIR}/bin/qdbd")

# get the full path of qdb_api.dll
if(WIN32)
    set(QDB_PYTHON_LIBRARY_GLOB "qdb_api.*")
else()
    set(QDB_PYTHON_LIBRARY_GLOB "lib*")
endif()

string(TOLOWER ${CMAKE_SYSTEM_NAME} PY_PACKAGE_SOURCE_SUFFIX)

set(PACKAGE_NAME quasardb)

set(SHARED_LIBRARY_EXTENSIONS "")
if(WIN32)
    set(QDB_PYD_EXT "*.pyd")
    set(SHARED_LIBRARY_EXTENSIONS "*.pyd *.dll")
elseif(APPLE)
    set(QDB_PYD_EXT "*.so")
    set(SHARED_LIBRARY_EXTENSIONS "*.so *.dylib")
else()
    set(QDB_PYD_EXT "*.so")
    set(SHARED_LIBRARY_EXTENSIONS "*.so")
endif()

# generate setup script by replacing CMake variables
configure_file(packaging/setup.in.py setup.py @ONLY)
configure_file(packaging/setup.in.cfg setup.cfg @ONLY)

configure_file(README.md README.txt COPYONLY)
configure_file(packaging/__init__.py ${PACKAGE_NAME}/__init__.py COPYONLY)
configure_file(packaging/ez_setup.py ez_setup.py COPYONLY)

configure_file(packaging/MANIFEST.in MANIFEST.in @ONLY)

# copy the libraries
if (WIN32)
    file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/qdb/lib/qdb_api.lib" DESTINATION "${CMAKE_BINARY_DIR}/quasardb_module/qdb/lib")
    file(COPY "${QDB_API_DIR}/bin/qdb_api.dll" DESTINATION "${PACKAGE_NAME}")
elseif(APPLE)
    file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/qdb/lib/libqdb_api.dylib" DESTINATION "${CMAKE_BINARY_DIR}/quasardb_module/qdb/lib")
    file(COPY "${QDB_API_DIR}/lib/libqdb_api.dylib"  DESTINATION "${PACKAGE_NAME}")
    file(COPY "${QDB_API_DIR}/lib/libc++.1.dylib"    DESTINATION "${PACKAGE_NAME}")
    file(COPY "${QDB_API_DIR}/lib/libc++abi.1.dylib" DESTINATION "${PACKAGE_NAME}")
elseif("${CMAKE_SYSTEM_NAME}" STREQUAL "FreeBSD")
    file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/qdb/lib/libqdb_api.so" DESTINATION "${CMAKE_BINARY_DIR}/quasardb_module/qdb/lib")
    file(COPY "${QDB_API_DIR}/lib/libqdb_api.so" DESTINATION "${PACKAGE_NAME}")
    file(COPY "${QDB_API_DIR}/lib/libc++.so" DESTINATION "${PACKAGE_NAME}")
    file(COPY "${QDB_API_DIR}/lib/libc++abi.so" DESTINATION "${PACKAGE_NAME}")
else()
    file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/qdb/lib/libqdb_api.so" DESTINATION "${CMAKE_BINARY_DIR}/quasardb_module/qdb/lib")
    file(COPY "${QDB_API_DIR}/lib/libqdb_api.so" DESTINATION "${PACKAGE_NAME}")
endif()

# for Apple we need to change the id otherwise we won't be able to load the extension
if (APPLE)
    execute_process(COMMAND install_name_tool -id "@loader_path/libqdb_api.dylib" ${CMAKE_CURRENT_BINARY_DIR}/${PACKAGE_NAME}/libqdb_api.dylib)
endif()

# Step 2: Build
add_custom_target(qdb_python_api_egg ALL
    COMMAND ${PYTHON_EXECUTABLE} setup.py bdist_egg sdist
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

add_custom_target(qdb_python_api_wheel ALL
    COMMAND ${PYTHON_EXECUTABLE} setup.py bdist_wheel
    DEPENDS qdb_python_api_egg
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

if(WIN32)
    add_custom_target(qdb_python_api_installer ALL
        COMMAND ${PYTHON_EXECUTABLE} setup.py bdist_wininst
        DEPENDS qdb_python_api_wheel
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    )
endif()

# Step 3: Test
add_test(NAME qdb_python_test
    COMMAND ${PYTHON_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/testcases/tests.py
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/testcases)
