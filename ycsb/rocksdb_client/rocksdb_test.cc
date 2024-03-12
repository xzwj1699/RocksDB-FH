#include "rocksdb_client.h"
#include "iostream"

void ParseCommandLine(int argc, const char *argv[], utils::Properties &props);
void check_args(utils::Properties &props);

class MyMergeOperator : public rocksdb::AssociativeMergeOperator {
    public:
      virtual bool Merge(
        const rocksdb::Slice& key,
        const rocksdb::Slice* existing_value,
        const rocksdb::Slice& value,
        std::string* new_value,
        rocksdb::Logger* logger) const override {
        new_value->assign(value.data(), value.size());
        return true;
      }
    virtual const char* Name() const override {
        return "MyMergeOperator";
    }
};

int main(const int argc, const char *argv[]){
	utils::Properties props;
	ParseCommandLine(argc, argv, props);

	ycsbc::CoreWorkload wl;
	wl.Init(props);
	ycsbc::WorkloadProxy wp(&wl);

	int loader_threads = stoi(props.GetProperty("threadcount"));
	uint64_t load_num = stoull(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
	int worker_threads = stoi(props.GetProperty("threadcount"));
	uint64_t requests_num = stoull(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
	int buffer_size = stoi(props.GetProperty("buffer_size"));
	std::string log_dir = props.GetProperty("log_dir");
	std::string data_dir = props.GetProperty("data_dir");
	int is_load = stoi(props.GetProperty("is_load"));

	//0. options
	rocksdb::Options options;
	options.allow_concurrent_memtable_write = true;
	options.recycle_log_file_num = false;
	options.allow_2pc = false;
	options.compression = rocksdb::kNoCompression;
	options.IncreaseParallelism(64);
    options.max_background_compactions = 4;
    options.max_background_flushes = 1;
	options.max_open_files = 90000;
	options.max_total_wal_size =  buffer_size * (1ull << 30);
	options.write_buffer_size = buffer_size * (1ull << 30); //1ull << 32;
	options.merge_operator = std::make_shared<MyMergeOperator>();
	rocksdb::WriteOptions write_options;
	write_options.no_slowdown = false;


	if(log_dir != "nologging"){
		write_options.sync = true;
		write_options.disableWAL = false;
        options.wal_dir = log_dir;
	}else{
		write_options.sync = false;
		write_options.disableWAL = true;
        options.wal_dir = data_dir + "../log/";
	}
	system(("rm -r " + options.wal_dir + "*").c_str());
	system(("rm -r " + data_dir + "*").c_str());
	system(("sudo fstrim " + data_dir).c_str());
	system(("sudo fstrim " + options.wal_dir).c_str());

	if(is_load == 1){
		options.error_if_exists = true;
		options.create_if_missing = true;
	}else{
		options.error_if_exists = false;
		options.create_if_missing = false;
		printf("Loading database...\n");
		system(("cp " + data_dir + "../data_bak/* " + data_dir).c_str());
		printf("Loading finished\n");
	}

	rocksdb::TransactionDBOptions txndb_options = rocksdb::TransactionDBOptions();

	//1. default
	// printf("default\n");
	// options.enable_pipelined_write = false;
	// options.two_write_queues = false;
	// options.unordered_write = false;
	// txndb_options.write_policy = rocksdb::TxnDBWritePolicy::WRITE_COMMITTED;

	//2. write prepared and unorder
	options.enable_pipelined_write = false;
	options.two_write_queues = true;
	options.unordered_write = true;
	txndb_options.write_policy = rocksdb::TxnDBWritePolicy::WRITE_PREPARED;

	//3. async
	// options.enable_pipelined_write = false;
	// options.two_write_queues = true;
	// options.unordered_write = true;
	// options.enable_ssdlogging = true;
	// options.ssdlogging_type = "spdk";
	// options.spdk_recovery = false;
	// if(props.GetProperty("log_dir") == "/home/chenhao/optane/rocksdb/log/")
	// 	options.ssdlogging_path = "trtype:PCIe traddr:0000:83:00.0";
	// else if(props.GetProperty("log_dir") == "/home/chenhao/nvme/rocksdb/log/")
	// 	options.ssdlogging_path = "trtype:PCIe traddr:0000:04:00.0";
	// else
	// 	assert(0);
	// options.ssdlogging_num = 4;
	// options.logging_server_num = 1;
	// options.before_server_num = 3;
	// options.after_server_num = 8;
	// txndb_options.write_policy = rocksdb::TxnDBWritePolicy::ASYNC_WRITE_PREPARED;

	// Run
	ycsbc::RocksDBClient rocksdb_client(&wp, options, write_options, txndb_options, data_dir, loader_threads,
				  load_num, worker_threads, requests_num);

	if(is_load == 1){
		rocksdb_client.Load();
	}else{
		rocksdb_client.Work();
	}

	return 0;
}

void ParseCommandLine(int argc, const char *argv[], utils::Properties &props) {
	if(argc != 8){
		printf("usage: <workload file> <threads num> <data_dir> <log_dir> \
					   <enable_ssdlogging> <logs_num> <is_load>\n");
		exit(0);
	}
	//1. workload file
	std::ifstream input(argv[1]);
	try {
		props.Load(input);
	} catch (const std::string &message) {
		printf("%s\n", message.c_str());
		exit(0);
	}
	input.close();

	//2. threads number
	props.SetProperty("threadcount", argv[2]);

	//3. data directory
	props.SetProperty("data_dir", argv[3]);

	//4. log directory
	props.SetProperty("log_dir", argv[4]);

	//6. log number
	props.SetProperty("logs_num", argv[5]);

	//7. max buffer size
	props.SetProperty("buffer_size", argv[6]);

	//8. is_laod
	props.SetProperty("is_load", argv[7]);

	//check
	check_args(props);
}

void check_args(utils::Properties &props){
	;// TODO
}