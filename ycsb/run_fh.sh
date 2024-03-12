#!/bin/bash

# workload configuration file
workloads=("workloadc.spec")
# database data directory
data_dir="/home/spdk/p4510/FH_Rocksdb_Data"
# data_dir="/home/xzwj1699/rocksdb/ycsb/rocksdb_data"
# database log directory
log_dir="/home/spdk/p4510/FH_Rocksdb_Data"
# log_dir="/home/xzwj1699/rocksdb/ycsb/rocksdb_data"
# database name
dbnames=("rocksdb")
# cache size
cache_size=("30")
# whether load data from existing 
cp="0"
# segment number, how many partition for a database
seg=("16")
# cache type
# cache_type=("LRU" "LRU_FH")
cache_type=("LRU_FH")
# memorys
memorys=("1")
# loading data or not
isload="0"
# thread number
threads=("64")
# warm up requets number in million
warm="30"
# request number in million
request_num="20"
# request size
req_size="4096"

rocksdb_base="/home/spdk/p4510/rocksdb"
# rocksdb_base="/home/xzwj1699/rocksdb/ycsb/rocksdb_data"
for db in "${dbnames[@]}";
do
    for workload in "${workloads[@]}";
    do
        for t in "${threads[@]}";
        do
            for m in "${memorys[@]}";
            do
                for size in "${cache_size[@]}";
                do
                    for seg in "${seg[@]}";
                    do
                        for cache in "${cache_type[@]}";
                        do
                            outdata="$data_dir""/data"
                            outlog="$log_dir""/log"
                            echo "workloads/""$workload" "$t" "$outdata" "$outlog" \
                            "$m" "$isload" "$db" "$size" "$warm" "$request_num" "$cp" "$req_size" "$seg" "$cache"
                            ./build/rocksdb2 "workloads/""$workload" "$t" "$outdata" "$outlog" \
                            "$m" "$isload" "$db" "$size" "$warm" "$request_num" "$cp" "$req_size" "$seg" "$cache" | tee "$cache"".log"
                        done
                    done
                done
            done
        done
    done
done