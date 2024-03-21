sudo perf record -F 99 -p pid -g -- sleep 30
sudo perf script -i perf.data  | ./FlameGraph/stackcollapse-perf.pl --all | ./FlameGraph/flamegraph.pl > perf.svg