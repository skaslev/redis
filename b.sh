#!/usr/bin/bash
cd deps/mimalloc
cmake -B build -S . -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
mkdir -p lib
cp build/libmimalloc.a lib/libmimalloc.a
