cmake_minimum_required(VERSION 2.8.12)
project(qdb-api-python)
enable_testing()

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

find_package(SWIG 3.0 REQUIRED)

if(POLICY CMP0026)
    # unable to find a workaround: doc says we should use $<TARGET_FILE> generator expression but
    # - set() doesn't support generator expression
    # - configure_file() doesn't support generator expression
    # - file(GENERATE ) support generator expression bu not variables
    cmake_policy(SET CMP0026 OLD)
endif()

set(SWIG_COMMON_FILES
    qdb_enum.i
    qdb_struct.i
    qdb_python_adapters.i
    qdb_python_common.i
)

set(QDB_PY_VERSION "2.3.0")
set(QDB_PY_WRAPPER "${CMAKE_CURRENT_BINARY_DIR}/qdb_python_wrapper.cxx")

set(QDB_API_DIR "${CMAKE_SOURCE_DIR}/qdb")
if(NOT IS_DIRECTORY "${QDB_API_DIR}/include")
    message(WARNING "Please unzip qdb c-api into ${QDB_API_DIR}")
endif()

set(QDB_DAEMON "${QDB_API_DIR}/bin/qdbd")

# get the full path of qdb_api.dll
if(WIN32)
    set(SWIG_PLATFORM_FILE qdb_python_windows.i)
    set(QDB_PYTHON_LIBRARY_GLOB "qdb_api.*")
else()
    set(SWIG_PLATFORM_FILE qdb_python_unix.i)
    set(QDB_PYTHON_LIBRARY_GLOB "lib*")
endif()

string(TOLOWER ${CMAKE_SYSTEM_NAME} PY_PACKAGE_SOURCE_SUFFIX)

# generate setup script by replacing CMake variables
configure_file(setup.in.py setup.py @ONLY)
configure_file(setup.in.cfg setup.cfg @ONLY)

set(PACKAGE_NAME quasardb)

configure_file(README.md README.txt COPYONLY)
configure_file(__init__.py ${PACKAGE_NAME}/__init__.py COPYONLY)
configure_file(qdb_convert.py ${PACKAGE_NAME}/qdb_convert.py COPYONLY)
configure_file(qdb_enum.py ${PACKAGE_NAME}/qdb_enum.py COPYONLY)
configure_file(ez_setup.py ez_setup.py COPYONLY)

set(SHARED_LIBRARY_EXTENSIONS "")
if(WIN32)
    set(SHARED_LIBRARY_EXTENSIONS "*.lib *.dll")
elseif(APPLE)
    set(SHARED_LIBRARY_EXTENSIONS "*.dylib")
else()
    set(SHARED_LIBRARY_EXTENSIONS "*.so")
endif()
configure_file(MANIFEST.in MANIFEST.in @ONLY)

file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/src)

# copy include dir
file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/qdb/include/qdb" DESTINATION "include")

# copy the libraries
if (WIN32)
    file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/qdb/lib/qdb_api.lib" DESTINATION "${PACKAGE_NAME}")
    file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/qdb/bin/qdb_api.dll" DESTINATION "${PACKAGE_NAME}")
elseif (APPLE)
    file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/qdb/lib/libqdb_api.dylib" DESTINATION "${PACKAGE_NAME}")
else()
    file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/qdb/lib/libqdb_api.so" DESTINATION "${PACKAGE_NAME}")
endif()

file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/memcpy_wrap.cxx DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/src/)

# for Apple we need to change the id otherwise we won't be able to load the extension
if (APPLE)
    execute_process(COMMAND install_name_tool -id "@loader_path/libqdb_api.dylib" ${CMAKE_CURRENT_BINARY_DIR}/${PACKAGE_NAME}/libqdb_api.dylib)
endif()

# Step 1: SWIG
add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/src/qdb_python_wrapper.cxx ${CMAKE_CURRENT_BINARY_DIR}/${PACKAGE_NAME}/impl.py
    COMMAND ${SWIG_EXECUTABLE} -c++ -python
        -o ${CMAKE_CURRENT_BINARY_DIR}/src/qdb_python_wrapper.cxx
        -outdir ${CMAKE_CURRENT_BINARY_DIR}/${PACKAGE_NAME}
        -O
        -module impl
        -interface _qdb
        ${SWIG_PLATFORM_FILE}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    DEPENDS ${SWIG_COMMON_FILES} ${SWIG_PLATFORM_FILE})

# Step 2: Build
set(BUILD_DEPENDENCIES
    ${CMAKE_CURRENT_BINARY_DIR}/src/memcpy_wrap.cxx
    ${CMAKE_CURRENT_BINARY_DIR}/src/qdb_python_wrapper.cxx
    ${CMAKE_CURRENT_BINARY_DIR}/${PACKAGE_NAME}/impl.py
)

add_custom_target(qdb_python_api_egg ALL
    COMMAND ${PYTHON_EXECUTABLE} setup.py bdist_egg sdist
    DEPENDS ${BUILD_DEPENDENCIES}
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

add_custom_target(qdb_python_api_wheel ALL
    COMMAND ${PYTHON_EXECUTABLE} setup.py bdist_wheel
    DEPENDS ${BUILD_DEPENDENCIES} qdb_python_api_egg
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

if(WIN32)
    add_custom_target(qdb_python_api_installer ALL
        COMMAND ${PYTHON_EXECUTABLE} setup.py bdist_wininst
        DEPENDS ${BUILD_DEPENDENCIES} qdb_python_api_wheel
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
endif()

add_custom_command(TARGET qdb_python_api_egg
    PRE_BUILD
    COMMAND pip install pytz tzlocal
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

# Step 3: Test
add_test(NAME qdb_python_test
    COMMAND ${PYTHON_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/testcases/tests.py
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/testcases)
