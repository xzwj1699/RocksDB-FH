

#workloads=("microbench512gb.spec" "workloada512gb.spec" "workloada512gb.uniform.spec" "workloadb512gb.spec" "workloade512gb.spec" "workloadf512gb.spec")

#workloads=("microbench512gb.spec" "workloada512gb.spec" "workloadb512gb.spec")

#workloads=("write95_2tb.spec" "write50_2tb.spec" "write5_2tb.spec" "write0_2tb.spec")

workloads=("workloadf512gb.spec")

data_dir=("sata" "nvme")

log_dir=("nvme2" "optane")

memorys=("1")

dbnames=("rocksdb")

ulimit -n 100000

rocksdb_base="/home/spdk/nvme/rocksdb_bak"

for db in "${dbnames[@]}";do
  if [ "$db" == "rocksdb" -o "$db" == "rocksdb_split" ];then
    base="$rocksdb_base"
    threads=("80")
    #threads=("20" "40" "80" "100" "120")
  elif [ "$db" == "logsdb_pl" ];then
    #threads=("4" "8" "12")
    threads=("8")
    base="$rocksdb_base"
  elif [ "$db" == "logsdb_ll0" ];then
    threads=("8")
    base="$rocksdb_base"
  elif [ "$db" == "logsdb_l0" ];then
    threads=("80")
    #base="$lo_base"
    base="$rocksdb_base"
  fi
  echo "$db"
  for workload in "${workloads[@]}";do

    result=$(echo $workload | grep "512gb")
    if [[ "$result" != "" ]]
    then
      base="/home/spdk/nvme_md/rocksdb_bak_512gb"
    else
      result=$(echo $workload | grep "1tb")
      if [[ "$result" != "" ]]
      then
        base="/home/spdk/sata/rocksdb_bak_1tb"
      else
        result=$(echo $workload | grep "2tb")
        if [[ "$result" != "" ]]
        then
          base="/home/spdk/nvme_md/rocksdb_bak_2tb"
        fi
      fi
    fi

    echo "$workload"
    echo "-e"
    for data in "${data_dir[@]}";do
      for log in "${log_dir[@]}";do
        for t in "${threads[@]}";do
          for m in "${memorys[@]}";do

            if [ "$data" == "nvme" -a "$log" == "nvme2" ];then
              continue;
            fi

            #if [ "$data" == "nvme" -a "$log" == "nvme" ];then
            #  continue;
            #fi

            if [ "$log" == "nologging" ];then
              outlog="nologging"
            else
              outlog="/home/spdk/""$log""/rocksdb/log"
            fi
            outdata="/home/spdk/""$data""/rocksdb/data"

            echo "$data""-""$log""-""$t""-""$m"

            isload="0"
            if [ "$db" == "logsdb_ll0" -o "$db" == "rocksdb_split" ]; then
              levels=("4")
            else
              levels=("-2")
            fi

            for i in "${levels[@]}"; do
              echo "-e"
              echo "start: ""`date '+%Y-%m-%d %H:%M:%S'`"

              #fstrim "$outdata"
              #fstrim "$outlog"
              rm "$outdata"/*
              if [ "$outlog" != "nologging" ];then
                rm "$outlog"/*
              fi
              if [ "$isload" == "0" ];then
                echo "Loading database from ""$base" to "$outdata"
                cp "$base"/* "$outdata"/
                echo "Loading finish"
              fi

              #output="$db""_""$workload"".txt"
              #output="rocksdb_2tb_sn.txt"
              output="$db""_""$data""_""$log"".txt"
              ./rocksdb2 "workloads/""$workload" "$t" "$outdata" "$outlog" "$i" "$m" "$isload" "$db" >> "$output"
              echo "-e" >> "$output"
              echo "-e" >> "$output"
              echo "-e" >> "$output"
              echo "-e" >> "$output"
              #cp "$outdata""/LOG" "$db""_""$workload"".log"

              sleep 10
              echo "-e"
              echo "-e"
              echo "-e"
            done
            echo "-e"
            echo "-e"
          done
        done
      done
    done
  done
done





