set(ENV{CC} g++46)
set(ENV{CXX} g++46)
execute_process(COMMAND python2.7 setup.py build WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
