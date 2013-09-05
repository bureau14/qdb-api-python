set(ENV{CC} clang)
set(ENV{CXX} clang++)
execute_process(COMMAND python2.7 setup.py build WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
