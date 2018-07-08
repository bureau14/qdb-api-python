cmake_minimum_required(VERSION 2.8.12)
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

if(MSVC)
    set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${QDB_BIN_DIR}")
    set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${QDB_BIN_DIR}")
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${QDB_BIN_DIR}")
else()
    set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${QDB_BIN_DIR}/${CMAKE_BUILD_TYPE}")
    set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${QDB_BIN_DIR}/${CMAKE_BUILD_TYPE}")
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${QDB_BIN_DIR}/${CMAKE_BUILD_TYPE}")
endif()

if(CLANG OR CMAKE_COMPILER_IS_GNUCXX)
    set(PYBIND11_CPP_STANDARD -std=c++1z)
else()
    add_definitions(/DSTATIC_LINKED)
    add_definitions(/D_CRT_SECURE_NO_WARNINGS=1)
    add_compile_options(/wd5033) # register is no longer a supported storage class
    add_compile_options(/EHa)
    add_compile_options(/MT)
    set(PYBIND11_CPP_STANDARD /std:c++17)
endif()

add_subdirectory(pybind11)

set(QDB_PYD "${QDB_BIN_DIR}/Release/quasardb.pyd")
set(QDB_API_DIR "${CMAKE_SOURCE_DIR}/qdb")
if(NOT IS_DIRECTORY "${QDB_API_DIR}/include")
    message(WARNING "Please unzip qdb c-api into ${QDB_API_DIR}")
endif()

include_directories(SYSTEM
    ${QDB_API_DIR}/include
)

find_library(QDB_API_LIB NAMES qdb_api PATHS ${QDB_API_DIR}/lib NO_DEFAULT_PATH)

add_subdirectory(src)



set(QDB_PY_VERSION "3.0.0.dev0")

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
    set(SHARED_LIBRARY_EXTENSIONS "*.lib *.dll")
elseif(APPLE)
    set(SHARED_LIBRARY_EXTENSIONS "*.dylib")
else()
    set(SHARED_LIBRARY_EXTENSIONS "*.so")
endif()

# generate setup script by replacing CMake variables
configure_file(packaging/setup.in.py setup.py @ONLY)
configure_file(packaging/setup.in.cfg setup.cfg @ONLY)

configure_file(README.md README.txt COPYONLY)
configure_file(packaging/__init__.py ${PACKAGE_NAME}/__init__.py COPYONLY)
configure_file(packaging/ez_setup.py ez_setup.py COPYONLY)

configure_file(packaging/MANIFEST.in MANIFEST.in @ONLY)

# copy include dir
# file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/qdb/include/qdb" DESTINATION "include")

# copy the libraries
if (WIN32)
  #  file(COPY "${QDB_API_DIR}/lib/qdb_api.lib" DESTINATION "${PACKAGE_NAME}")
    file(COPY "${QDB_API_DIR}/bin/qdb_api.dll" DESTINATION "${PACKAGE_NAME}")
    file(COPY "${QDB_API_DIR}/bin/qdb_api.dll" DESTINATION "${QDB_BIN_DIR}/Debug")
    file(COPY "${QDB_API_DIR}/bin/qdb_api.dll" DESTINATION "${QDB_BIN_DIR}/Release")
elseif(APPLE)
    file(COPY "${QDB_API_DIR}/lib/libqdb_api.dylib" DESTINATION "${QDB_BIN_DIR}/debug")
    file(COPY "${QDB_API_DIR}/lib/libqdb_api.dylib" DESTINATION "${QDB_BIN_DIR}/release")
    file(COPY "${QDB_API_DIR}/lib/libqdb_api.dylib"  DESTINATION "${PACKAGE_NAME}")
    file(COPY "${QDB_API_DIR}/lib/libc++.1.dylib"    DESTINATION "${PACKAGE_NAME}")
    file(COPY "${QDB_API_DIR}/lib/libc++abi.1.dylib" DESTINATION "${PACKAGE_NAME}")
elseif("${CMAKE_SYSTEM_NAME}" STREQUAL "FreeBSD")
    file(COPY "${QDB_API_DIR}/lib/libqdb_api.so" DESTINATION "${QDB_BIN_DIR}/debug")
    file(COPY "${QDB_API_DIR}/lib/libqdb_api.so" DESTINATION "${QDB_BIN_DIR}/release")
    file(COPY "${QDB_API_DIR}/lib/libqdb_api.so" DESTINATION "${PACKAGE_NAME}")
    file(COPY "${QDB_API_DIR}/lib/libc++.so" DESTINATION "${PACKAGE_NAME}")
    file(COPY "${QDB_API_DIR}/lib/libc++abi.so" DESTINATION "${PACKAGE_NAME}")
else()
    file(COPY "${QDB_API_DIR}/lib/libqdb_api.so" DESTINATION "${QDB_BIN_DIR}/debug")
    file(COPY "${QDB_API_DIR}/lib/libqdb_api.so" DESTINATION "${QDB_BIN_DIR}/release")
    file(COPY "${QDB_API_DIR}/lib/libqdb_api.so" DESTINATION "${PACKAGE_NAME}")
endif()

# for Apple we need to change the id otherwise we won't be able to load the extension
if (APPLE)
    execute_process(COMMAND install_name_tool -id "@loader_path/libqdb_api.dylib" ${CMAKE_CURRENT_BINARY_DIR}/${PACKAGE_NAME}/libqdb_api.dylib)
endif()

# Step 2: Build

add_custom_target(qdb_python_api_egg ALL
    COMMAND ${PYTHON_EXECUTABLE} setup.py bdist_egg
    DEPENDS quasardb
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

add_custom_target(qdb_python_api_wheel ALL
    COMMAND ${PYTHON_EXECUTABLE} setup.py bdist_wheel
    DEPENDS quasardb qdb_python_api_egg
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

if(WIN32)
    add_custom_target(qdb_python_api_installer ALL
        COMMAND ${PYTHON_EXECUTABLE} setup.py bdist_wininst
        DEPENDS quasardb qdb_python_api_wheel
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    )
endif()

# add_custom_command(TARGET qdb_python_api_egg
#     PRE_BUILD
#     COMMAND pip install pytz tzlocal
#     WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
# )

# Step 3: Test
add_test(NAME qdb_python_test
    COMMAND ${PYTHON_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/testcases/tests.py
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/testcases)
