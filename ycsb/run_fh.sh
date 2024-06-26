#!/bin/bash

# workload configuration file
# workloads=("workloada.spec" "workloadb.spec" "workloadc.spec" "workloadd.spec" "workloade.spec" "workloadf.spec")
# workloads=("workloade.spec" "workloadf.spec")
workloads=("workloadb.spec")
# workloads=("workloada.spec" "workloadb.spec" "workloadd.spec" "workloade.spec" "workloadf.spec")
# database data directory
# data_dir="/home/spdk/p4510/zhangjuncheng.zjc/RocksDB_FH_Data"
data_dir="/home/cqy/qemu-iso/zhangjuncheng.zjc/RocksDB_FH_Data"
# data_dir="/home/xzwj1699/rocksdb/ycsb/rocksdb_data"
# database log directory
# log_dir="/home/spdk/p4510/zhangjuncheng.zjc/RocksDB_FH_Data"
# log_dir="/home/cqy/qemu-iso/zhangjuncheng.zjc/RocksDB_FH_Data"
log_dir="nologging"
# log_dir="/home/xzwj1699/rocksdb/ycsb/rocksdb_data"
# database name
dbnames=("rocksdb")
# cache size
cache_size=("30")
# whether load data from existing 
cp="0"
# segment number, how many partition for a database
shard_bits=("4" "3" "2" "1" "0")
# shard_bits=("0")
# cache type
cache_type=("LRU_FH" "LRU")
# cache_type=("LRU_FH")
# memorys in MB
memorys=("1024")
# memorys=("64" "128" "256" "512" "1024")
# loading data or not
isload="0"
# thread number
# threads=("64" "128" "160")
threads=("160")
# warm up request number in million
warm="30"
# request number in million
request_num="60"
# request size
req_size="4096"

# rocksdb_base="/home/spdk/RocksDB_FH_Data_bak"
rocksdb_base="/home/cqy/qemu-iso/zhangjuncheng.zjc/RocksDB_FH_Data_bak"
# rocksdb_base="/home/xzwj1699/rocksdb/ycsb/rocksdb_data"

ulimit -n 100000

for db in "${dbnames[@]}";
do
    for workload in "${workloads[@]}";
    do
        if [ "$workload" != "workloadc.spec" ];then
            cp="1"
        fi
        for t in "${threads[@]}";
        do
            for m in "${memorys[@]}";
            do
                for size in "${cache_size[@]}";
                do
                    for shard_bit in "${shard_bits[@]}";
                    do
                        for cache in "${cache_type[@]}";
                        do
                            outdata="$data_dir""/data"
                            outlog="$log_dir""/log"
                            if [ "$log_dir" == "nologging" ]; then
                                outlog="$log_dir"
                            fi
                            echo "../ycsb/workloads/""$workload" "$t" "$outdata" "$outlog" \
                            "$m" "$isload" "$db" "$size" "$warm" "$request_num" "$cp" "$req_size" "$shard_bit" "$cache"
                            ./../cmake_build/rocksdb_test2 "../ycsb/workloads/""$workload" "$t" "$outdata" "$outlog" \
                            "$m" "$isload" "$db" "$size" "$warm" "$request_num" "$cp" "$req_size" "$shard_bit" "$cache" | tee "$cache""-""$workload""-""$m""-""$shard_bit"".log"
                            if [ "$cp" == "1" ];then
                                rm -rf "$data_dir"
                                cp -r "$rocksdb_base" "$data_dir"
                            fi
                        done
                    done
                done
            done
        done
    done
done
