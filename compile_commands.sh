#!/usr/bin/env bash

rm -rf build || true

mkdir build \
    && cd build \
    && cmake -G Ninja -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++  -DCMAKE_BUILD_TYPE=Debug -DCMAKE_LIBRARY_OUTPUT_DIRECTORY=output/ ../quasardb/ \
    && bear --output ../compile_commands.json -- cmake --build . --config Debug -j \
    && cd .. \
    && rm -rf build
