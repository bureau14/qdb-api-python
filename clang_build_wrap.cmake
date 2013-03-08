set(ENV{CC} clang)
execute_process(COMMAND python setup.py build WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
