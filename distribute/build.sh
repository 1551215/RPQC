#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${BUILD_DIR:-$ROOT/build}"
OUTPUT_DIR="${OUTPUT_DIR:-$BUILD_DIR/bin}"

CC="${CC:-gcc}"
CXX="${CXX:-g++}"
COMMON_OPT="${COMMON_OPT:--O3}"
ARCH_FLAGS="${ARCH_FLAGS:--mtune=generic}"

mkdir -p "$BUILD_DIR" "$OUTPUT_DIR"

"$CC" $COMMON_OPT $ARCH_FLAGS \
    -I"$ROOT/vendor/roaring" \
    -c "$ROOT/vendor/roaring/roaring.c" \
    -o "$BUILD_DIR/roaring.o"

CXXFLAGS=(
    -std=c++17
    "$COMMON_OPT"
    "$ARCH_FLAGS"
    -pthread
    -I"$ROOT/app"
    -I"$ROOT"
    -I"$ROOT/vendor"
)

"$CXX" "${CXXFLAGS[@]}" \
    "$ROOT/app/simple_worker_incremental.cpp" \
    "$BUILD_DIR/roaring.o" -lz \
    -o "$OUTPUT_DIR/worker"

"$CXX" "${CXXFLAGS[@]}" \
    "$ROOT/app/simple_coordinator_incremental.cpp" \
    "$BUILD_DIR/roaring.o" -lz \
    -o "$OUTPUT_DIR/coordinator"

"$CXX" -std=c++17 "$COMMON_OPT" \
    "$ROOT/tools/build_partitions_from_v2p.cpp" \
    -o "$OUTPUT_DIR/build_partitions_from_v2p"

printf 'worker=%s\n' "$OUTPUT_DIR/worker"
printf 'coordinator=%s\n' "$OUTPUT_DIR/coordinator"
printf 'partitioner=%s\n' "$OUTPUT_DIR/build_partitions_from_v2p"
