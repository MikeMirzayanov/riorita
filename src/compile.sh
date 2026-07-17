#!/usr/bin/env bash
set -euo pipefail

CXX=${CXX:-g++}

# This is intentionally a compact-only, host-native production build. The
# resulting binary is optimized for the CPU that runs this script and is not
# intended to be copied to older or different machines.
COMMON_FLAGS=(
  -std=c++14
  -Wall
  -Wextra
  -Wconversion
  -pipe
  -pthread
  -ffunction-sections
  -fdata-sections
)

OPT_FLAGS=(
  -march=native
  -mtune=native
  -Ofast
  -flto=auto
  -fuse-linker-plugin
  -fno-fat-lto-objects
  -fomit-frame-pointer
  -fno-plt
  -fno-semantic-interposition
  -funroll-loops
  -DNDEBUG
  -DBOOST_BIND_GLOBAL_PLACEHOLDERS
  -DBOOST_DISABLE_ASSERTS
)

SOURCES=(riorita.cpp protocol.cpp compact.cpp storage.cpp cache.cpp)
LIBS=(-lboost_system -lboost_thread -lboost_filesystem -lboost_program_options -lsnappy)
LINK_FLAGS=(-Wl,-O1,--as-needed,--gc-sections)

"$CXX" "${COMMON_FLAGS[@]}" "${OPT_FLAGS[@]}" \
  -o riorita "${SOURCES[@]}" "${LINK_FLAGS[@]}" "${LIBS[@]}"
