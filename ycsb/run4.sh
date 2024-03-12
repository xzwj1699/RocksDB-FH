
#workloads=("microbench.uniform.spec")
#workloads=("workloada.spec" "workloada.uniform.spec" "workloadb.spec" "workloade.spec" "workloadf.spec")
#workloads=("workloada.spec" "workloada.uniform.spec" "workloadb.spec" "workloade.spec" "workloadf.spec")
#workloads=("workloadb2tb.spec")
#workloads=("workloada1tb.spec" "workloadb1tb.spec")
#workloads=("workloadb512gb.spec" "workloada1tb.spec")
#workloads=("microbench2tb.spec" "workloada2tb.spec" "workloadb2tb.spec")
#workloads=("microbench32b.spec" "microbench128b.spec" "microbench512b.spec")
#workloads=("workloada512gb.spec" "workloada1tb.spec" "workloada2tb.spec")
workloads=("workloada512gb.spec")
#workloads=("workloada.spec")

data_dir=("sata")
#data_dir=("nvme1")
#data_dir=("sata1" "nvme1")

#log_dir=("nologging" "ramdisk" "nvme1" "optane")
log_dir=("optane")

#memorys=("0.512" "1" "2" "4")
memorys=("1")

#dbnames=("rocksdb" "logsdb_pl" "logsdb_ll0" "logsdb_l0")
#dbnames=("logsdb_ll0" "logsdb_pl")
dbnames=("logsdb_ll0")

ulimit -n 100000

#lo_base="/home/spdk/nvme1/rocksdb_lo_bak"
rocksdb_base="/home/spdk/nvme/rocksdb_bak"
#rocksdb_base="/home/spdk/nvme1/rocksdb_bak_512gb"
#rocksdb_base="/home/spdk/nvme1/rocksdb_bak_1tb"

for db in "${dbnames[@]}";do
  if [ "$db" == "rocksdb" ];then
    base="$rocksdb_base"
    threads=("40")
    #threads=("20" "40" "80" "100" "120")
  elif [ "$db" == "logsdb_pl" ];then
    #threads=("4" "8" "12")
    threads=("8")
    base="$rocksdb_base"
  elif [ "$db" == "logsdb_ll0" ];then
    threads=("6")
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
          base="/home/spdk/nvme/rocksdb_bak_2tb"
        fi
      fi
    fi

    echo "$workload"
    echo "-e"
    for data in "${data_dir[@]}";do
      for log in "${log_dir[@]}";do
        for t in "${threads[@]}";do
          for m in "${memorys[@]}";do

            if [ "$data" == "nvme1" -a "$log" == "nvme" ];then
              continue;
            fi

            if [ "$log" == "nologging" ];then
              outlog="nologging"
            else
              outlog="/home/spdk/""$log""/rocksdb/log"
            fi
            outdata="/home/spdk/""$data""/rocksdb/data"

            echo "$data""-""$log""-""$t""-""$m"

            isload="0"
            for i in 4;do

              echo "-e"
              echo "start: ""`date '+%Y-%m-%d %H:%M:%S'`"


              for x in 4000000;do
              #for x in 100000 200000 300000;do
                for y in 25;do

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

                #iostat -d "$outdata" -m -t 1 > dd.io.txt 2>&1 &

                echo "$x"-"$y"
                
                ./rocksdb4 "workloads/""$workload" "$t" "$outdata" "$outlog" "$i" "$m" "$isload" "$db" "$x" "$y" > "$db""_""$workload"".txt2"
                #> "results_""$db""_""$x""_""$y"".txt"

                cp "$outdata""/LOG" "$db""_""$workload"".log2"

                #kill $(ps aux | grep 'iostat' | awk 'NR==1 {print $2}')
                #cp "iops.txt" "results/""iops_""$db""_""$x""_""$y"".txt"

                done
              done


              #>> "$db""_""$data""_""$log"".txt"

              #cp "$outdata""/LOG" log.txt

              # >> "$db""_""$data""_""$log"".txt"
              #>> "$db""_""$data""_""$log""_""$workload"".txt"

              #mv logging_queue_length.txt "$db""_logging_queue_length.txt"
              #mv lo_queue_length.txt "$db""_lo_queue_length.txt"
              #mv sync_time.txt "$db""_sync_time.txt"
              #mv batch_size.txt "$db""_batch_size.txt"
              #mv spdk_logging_time.txt "$db""_spdk_logging_time.txt"

              #echo "$db""_""$workload"".txt"
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

#set args workloads/microbench.spec 8 /home/spdk/sata/rocksdb/data /home/spdk/optane/rocksdb/log 1 30 0 logsdb_pl



