
PROJECT(ycsb)
#SET(CMAKE_VERBOSE_MAKEFILE on)
cmake_minimum_required(VERSION 3.24)
SET(CMAKE_CXX_FLAGS  " -Wl,--rpath=./lib/ -m64 -Wall -O0 -pthread -std=c++17 -fPIC -fopenmp")

# SET(SRC_LIST rocksdb/rocksdb_test2.cc rocksdb/rocksdb_client2.cc core/core_workload.cc)
SET(SRC_LIST rocksdb_client/rocksdb_test2.cc rocksdb_client/rocksdb_client2.cc core/core_workload.cc)

# find_library(LIBCLHT clht PATHS /home/spdk/zy/CLHT/)
# message("${LIBCLHT}")
# find_library(LIBSSMEM ssmem PATHS /home/spdk/zy/CLHT/external/ssmem)

INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/../include)
INCLUDE_DIRECTORIES($ENV{DPDK_INCLUDE})
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR})
##include_directories(/home/spdk/spdk)

# LINK_DIRECTORIES(${PROJECT_SOURCE_DIR}/../build/lib64)
LINK_DIRECTORIES(${PROJECT_SOURCE_DIR}/../build/)
LINK_DIRECTORIES(${$ENV{DPDK_LIB}})

set(ROCKSDB_LIB_DIR ${PROJECT_SOURCE_DIR}/../build)
find_library(ROCKSDB_LIBRARY NAMES rocksdb PATHS ${ROCKSDB_LIB_DIR})

INCLUDE_DIRECTORIES(core)
ADD_EXECUTABLE(rocksdb2 ${SRC_LIST})

#TARGET_LINK_LIBRARIES(rocksdb2 rocksdb ${LIBCLHT} ${LIBSSMEM} ${JUNCTION_ALL_LIBRARIES})
TARGET_LINK_LIBRARIES(rocksdb2 ${ROCKSDB_LIBRARY})


