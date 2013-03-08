set(ENV{CC} gcc46)
execute_process(COMMAND python setup.py build WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
