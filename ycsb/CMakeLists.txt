
PROJECT(ycsb)
#SET(CMAKE_VERBOSE_MAKEFILE on)
cmake_minimum_required(VERSION 3.10)
SET(CMAKE_CXX_FLAGS  " -Wl,--rpath=./lib/ -m64 -ldl -lstdc++ -Wall -O0 -pthread -std=c++17 -fPIC -fopenmp")

# SET(SRC_LIST rocksdb/rocksdb_test2.cc rocksdb/rocksdb_client2.cc core/core_workload.cc)
SET(SRC_LIST rocksdb_client/rocksdb_test2.cc rocksdb_client/rocksdb_client2.cc core/core_workload.cc)
INCLUDE_DIRECTORIES(core)

INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/../include)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR})

LINK_DIRECTORIES(${PROJECT_SOURCE_DIR}/../cmake_build/)
LINK_DIRECTORIES("/tmp/fbcode_builder_getdeps-ZhomeZxzwjZRocksDB-FHZthird-partyZfollyZbuildZfbcode_builder/installed/folly")

set(ROCKSDB_LIB_DIR ${PROJECT_SOURCE_DIR}/../cmake_build/lib)
set(FOLLY_LIB_DIR "/tmp/fbcode_builder_getdeps-ZhomeZxzwjZRocksDB-FHZthird-partyZfollyZbuildZfbcode_builder/installed/folly/lib/")
set(GLOG_LIB_DIR /tmp/fbcode_builder_getdeps-ZhomeZxzwjZRocksDB-FHZthird-partyZfollyZbuildZfbcode_builder/installed/glog-wgiU9YrbuCkTKVNwfQnhKxPrFG3pBP6j63nWrlcbRkw/lib/)
find_library(ROCKSDB_LIBRARY NAMES rocksdb PATHS ${ROCKSDB_LIB_DIR})
find_library(FOLLY_LIBRARY NAMES folly PATHS ${FOLLY_LIB_DIR})
find_library(GLOG_LIBRARY NAMES glog PATHS ${GLOG_LIB_DIR})
message(${FOLLY_LIBRARY})
ADD_EXECUTABLE(rocksdb2 ${SRC_LIST})
set(FOLLY_LIB /tmp/fbcode_builder_getdeps-ZhomeZxzwjZRocksDB-FHZthird-partyZfollyZbuildZfbcode_builder/installed/folly/lib/libfolly.a)
set(GLOG_LIB /tmp/fbcode_builder_getdeps-ZhomeZxzwjZRocksDB-FHZthird-partyZfollyZbuildZfbcode_builder/installed/glog-wgiU9YrbuCkTKVNwfQnhKxPrFG3pBP6j63nWrlcbRkw/lib/libglog.so)
set(GFLAGS_LIB /tmp/fbcode_builder_getdeps-ZhomeZxzwjZRocksDB-FHZthird-partyZfollyZbuildZfbcode_builder/installed/gflags-glq6iLPEpABB1wBb_mzuYWSA-xjnN7v2K8lztb8FUhY/lib/libgflags.a)
set(URING_LIB /usr/lib/liburing.a)
set(FMT_LIB /tmp/fbcode_builder_getdeps-ZhomeZxzwjZRocksDB-FHZthird-partyZfollyZbuildZfbcode_builder/installed/fmt-1z6N0UHTwTftonqpa-duO7F3cYTiGjoVHEKRtiUBGFc/lib/libfmt.a)
TARGET_LINK_LIBRARIES(rocksdb2 ${ROCKSDB_LIBRARY} ${FOLLY_LIB} ${GLOG_LIB} ${GFLAGS_LIB} ${URING_LIB} ${FMT_LIB} dl stdc++)


