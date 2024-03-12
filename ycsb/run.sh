
workloads=("microbench.spec")
#workloads=("workloada.spec")
#workloads=("workloada.spec" "workloada.uniform.spec" "workloadb.spec" "workloadb.uniform.spec")

data_dir=("nvmeraid0")
#data_dir=("nvme1")
#data_dir=("sata1" "nvme1")

#log_dir=("sata2")
log_dir=("optane")
#log_dir=("sata2" "nvme2" "optane")

threads=("4")

memorys=("8")

ulimit -n 10000


for workload in "${workloads[@]}";do
echo "$workload"
echo "-e"
for data in "${data_dir[@]}";do
    for log in "${log_dir[@]}";do
        for t in "${threads[@]}";do
            for m in "${memorys[@]}";do
                #if [ "$data" ==  "$log" ]; then
                #    continue;
                #fi
                if [ "$data" == "sata1" -a "$log" == "disk2" ];then
                    continue;
                fi
                if [ "$data" == "nvme1" -a "$log" == "disk2" ]; then
                    continue;
                fi
                if [ "$data" == "nvme1" -a "$log" == "sata2" ]; then
                    continue;
                fi
                #if [ "$data" == "disk1" -a "$log" == "nologging" ]; then
                #    continue;
                #fi
                #if [ "$data" == "disk1" -a "$log" == "ramdisk" ]; then
                #    continue;
                #fi
                #if [ "$data" == "disk1" -a "$log" == "sata2" ]; then
                #    continue;
                #fi

                if [ "$log" == "nologging" ];then
                    outlog="nologging"
                else
                    outlog="/home/chenhao/""$log""/rocksdb/log"
                fi
                outdata="/home/chenhao/""$data""/rocksdb/data"


                echo "$data""-""$log""-""$t""-""$m"

                isload="0"
                for((i=1;i<=1;i++)); do
                    echo "-e"
                    echo "Trial-""$i"

                    #rm "$outdata"/*
                    #if [ "$outlog" != "nologging" ];then
                    #    rm "$outlog"/*
                    #fi
                    if [ "$isload" == "0" ];then
                        echo "Loading database..."
                        #cp /home/chenhao/nvmeraid0/rocksdb_bak/* "$outdata"/
                        echo "Loading finish"
                    fi

                    #sync;echo 3 > /proc/sys/vm/drop_caches

                    ./rocksdb2 "workloads/""$workload" "$t" "$outdata" "$outlog" 1 "$m" "$isload"

                    #sleep 10
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


