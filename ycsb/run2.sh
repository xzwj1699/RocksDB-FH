#!/bin/bash

#workloads=("microbench.spec")
#workloads=("microbench.spec" "workloada.spec" "workloada.uniform.spec" "workloadb.spec" "workloade.spec" "workloadf.spec")
#workloads=("microbench.spec" "workloada.spec" "workloada.uniform.spec" "workloadb.spec" "workloade.spec" "workloadf.spec")
#workloads=("workloadb2tb.spec")
#workloads=("workloada1tb.spec" "workloadb1tb.spec")
#workloads=("workloadb512gb.spec" "workloada1tb.spec")
#workloads=("microbench2tb.spec" "workloada2tb.spec" "workloadb2tb.spec")
#workloads=("microbench32b.spec" "microbench128b.spec" "microbench512b.spec")
#workloads=("workloada512gb.spec" "workloada1tb.spec" "workloada2tb.spec")
#workloads=("workloada512gb.uniform.spec" "workloadb512gb.spec" "workloadb512gb.uniform.spec")
#workloads=("microbench.spec" "workloada.spec" "workloada.uniform.spec" "workloadb.spec" "workloadb.uniform.spec")
#workloads=("microbench.uniform.spec")
#workloads=("insert.spec")
#workloads=("write0_512gb.spec" "write30_512gb.spec" "write70_512gb.spec" "write100_512gb.spec")
#("workloadb512gb.spec" "workloade512gb.spec" "workloadf512gb.spec")
#workloads=("microbench512gb.spec" "workloada512gb.spec" "workloadb512gb.spec")
#workloads=("workloadc.uniform.spec")
#workloads=("workloadc512gb.spec")
workloads=("workloadd.spec")

#data_dir=("raid5")
data_dir=("ropt0")

#log_dir=("nologging" "ramdisk" "nvme1" "optane")
log_dir=("nologging")
#log_dir=("nvme")
#log_dir=("nvme" "optane")

#memorys=("1")
memorys=("1")

#dbnames=("rocksdb" "logsdb_pl" "logsdb_ll0" "logsdb_l0" "rocksdb_split")
#dbnames=("logsdb_l0")
dbnames=("rocksdb")

cache_size=("30")

#cache_type = ("hhvm" "deferred" "topfs")
cp=("1")

req_size=("4096")
#seg=("1" "2" "4" "8" "16" "40" "80")
seg=("64")

ulimit -n 100000

# rocksdb_base="/home/spdk/sata/rocksdb_bak"
# rocksdb_base="/home/spdk/nvme/rocksdb_100gb_bak/"
rocksdb_base="/mnt/ropt0/rocksdb_100gb_bak/"
#rocksdb_base="/home/spdk/rocksdb_100gb_bak/"
#rocksdb_base_512gb="/home/spdk/sata/rocksdb_bak_512gb"
# rocksdb_base_512gb="/home/spdk/nvme/rocksdb_512gb_bak"
# rocksdb_base_2tb="/home/spdk/nvme/rocksdb_bak_2tb"
# rocksdb_base_4kb="/home/spdk/sata/rocksdb_4kb_bak/"

for db in "${dbnames[@]}";do
  if [ "$db" == "rocksdb" -o "$db" == "rocksdb_split" ];then
    base="$rocksdb_base"
    threads=("64")
    #threads=("20" "40" "80" "100" "120")
  elif [ "$db" == "logsdb_pl" ];then
    #threads=("4" "8" "12")
    threads=("6")
    base="$rocksdb_base"
  elif [ "$db" == "logsdb_ll0" ];then
    threads=("6")
    base="$rocksdb_base"
  elif [ "$db" == "logsdb_l0" ];then
    #threads=("8" "16" "32" "64" "79")
    threads=("64")
    #base="$lo_base"
    base="$rocksdb_base"
  fi
  echo "$db"
  for workload in "${workloads[@]}";do

    result=$(echo $workload | grep "512gb")
    if [[ "$result" != "" ]]
    then
      base="$rocksdb_base_512gb"
    else
      result=$(echo $workload | grep "1tb")
      if [[ "$result" != "" ]]
      then
        base="/home/spdk/sata/rocksdb_bak_1tb"
      else
        result=$(echo $workload | grep "2tb")
        if [[ "$result" != "" ]]
        then
          base="$rocksdb_base_2tb"
        else
          result=$(echo $workload | grep "200gb")
          if [[ "$result" != "" ]]
          then
            base="/home/spdk/nvme/rocksdb_bak_200gb"
          else
            result=$(echo $workload | grep "4kb")
            if [[ "$result" != "" ]]
            then
              base="$rocksdb_base_4kb"
            fi
          fi       
        fi
      fi
    fi

    alpha=$(echo $workload | grep "workloadc")
    if [[ "$alpha" != "" ]]
    then
      warm="20"
      n="200"
    else
      alpha=$(echo $workload | grep "workloadb")
      if [[ "$alpha" != "" ]]
      then
        warm="20"
        n="200"
      else
        alpha=$(echo $workload | grep "workloadd")
        if [[ "$alpha" != "" ]]
        then
          warm="30"
          n="200"
        else
          alpha=$(echo $workload | grep "workloade")
          if [[ "$alpha" != "" ]]
          then
            warm="6"
            n="20"
          else
            warm="20"
            n="200"
          fi
        fi
      fi
    fi

    echo "$workload"
    echo "-e"
    for data in "${data_dir[@]}";do
      for log in "${log_dir[@]}";do
        for size in "${cache_size[@]}";do
          for m in "${memorys[@]}";do
            for t in "${threads[@]}";do
              for flag in "${cp[@]}";do
                for req in "${req_size[@]}";do
                  for seg in "${seg[@]}";do
                    if [ "$log" == "nologging" ];then
                    outlog="nologging"
                    else
                    outlog="/mnt/""$log""/log"
                    fi
                    outdata="/mnt/""$data""/data"

                    echo "$data" "-" "$log" "-" "$t" "thd -" "$m" "GB memtable -" "$size" "GB cache" "-" "$warm" "M -" "$n" "M"

                    isload="0"
                    if [ "$db" == "logsdb_ll0" -o "$db" == "rocksdb_split" ]; then
                    levels=("4")
                    else
                    levels=("-2")
                    fi

                    for i in "${levels[@]}"; do
                    echo "-e"
                    echo "start: ""`date '+%Y-%m-%d %H:%M:%S'`"

                    #rm "$outdata"/*
                    if [ "$outlog" != "nologging" ];then
                    rm "$outlog"/*
                    fi
                    if [ "$isload" == "0" ];then
                        echo "Loading database from ""$base" to "$outdata"
                        #cp "$base"/* "$outdata"/
                        echo "Loading finish"
                    fi

                    output="$db""_""$workload""_""$t""thd.txt"
                    echo "workloads/""$workload" "$t" "$outdata" "$outlog" \
                    "$i" "$m" "$isload" "$db" "$size" "$warm" "$n" "$flag" "$req" "$seg"
                    
                    #valgrind --tool=memcheck --leak-check=full \
                    ./build/rocksdb2 "workloads/""$workload" "$t" "$outdata" \
                      "$outlog" "$i" "$m" "$isload" "$db" "$size" "$warm" "$n"\
                      "$flag" "$req" "$seg"
                    # gdb -ex=r --args ./build/rocksdb2 "workloads/""$workload" "$t" "$outdata" \
                    #   "$outlog" "$i" "$m" "$isload" "$db" "$size" "$warm" "$n"\
                    #   "$flag" "$req" "$seg"

                    rm "$outdata"/*
                    cp "$base"/* "$outdata"/

                    sleep 1
                    echo "-e"
                    echo "-e"
                    echo "-e"
                    done
                  done
                done
              done
            done
            echo "-e"
            echo "-e"
          done
        done
      done
    done
  done
done

#set args workloads/workloada512gb.uniform.spec 8 /home/spdk/sata/rocksdb/data /home/spdk/optane/rocksdb/log 4 1 0 logsdb_ll0
#set args workloads/workloada.spec 40 /home/spdk/sata/rocksdb/data /home/spdk/optane/rocksdb/log -2 1 0 rocksdb

