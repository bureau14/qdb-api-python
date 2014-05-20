set(ENV{CC} g++48)
set(ENV{CXX} g++48)
execute_process(COMMAND python2.7 setup.py build WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
