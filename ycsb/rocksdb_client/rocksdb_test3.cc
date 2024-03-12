#include "rocksdb_client3.h"
#include "iostream"
#include "cmath"
//#include "spdk.h"

void ParseCommandLine(int argc, const char *argv[], utils::Properties &props);
void CheckArgs(utils::Properties &props);
void PrintWorkload(const char* filename);

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
	double buffer_size = stod(props.GetProperty("buffer_size"));
	std::string log_dir = props.GetProperty("log_dir");
	std::string data_dir = props.GetProperty("data_dir");
	int is_load = stoi(props.GetProperty("is_load"));
	std::string dbname = props.GetProperty("dbname");
	int async_num = 100;
	int reads = stoi(props.GetProperty("logs_num"));

	//===================common-setting==========
	rocksdb::Options options;
	rocksdb::WriteOptions write_options;
	rocksdb::ReadOptions read_options;
	options.allow_concurrent_memtable_write = true;
	options.recycle_log_file_num = false;
	options.allow_2pc = false;
	options.compression = rocksdb::kNoCompression;
	options.max_open_files = 500000;
	if(log_dir != "nologging"){
		write_options.sync = true;
		write_options.disableWAL = false;
        options.wal_dir = log_dir;
	}else{
		write_options.sync = false;
		write_options.disableWAL = true;
        options.wal_dir = data_dir;
	}
	if(is_load == 1){
		options.error_if_exists = true;
		options.create_if_missing = true;
	}else{
		options.error_if_exists = false;
		options.create_if_missing = false;
	}
	options.statistics = rocksdb::CreateDBStatistics();

	options.max_total_wal_size =  (uint64_t)(buffer_size * (1ull << 30));
	options.write_buffer_size = (uint64_t)(buffer_size * (1ull << 30)); //1ull << 32;

	auto env = rocksdb::Env::Default();
	env->SetBackgroundThreads(3, rocksdb::Env::HIGH);
	env->SetBackgroundThreads(12, rocksdb::Env::LOW);
	options.max_background_jobs = 15;
	options.max_subcompactions = 8;
	options.max_write_buffer_number = 4;


	// system(("sudo fstrim " + data_dir).c_str());
	// system(("sudo fstrim " + options.wal_dir).c_str());

  	//===================DB=======================================
  	std::string pcie_addr = "";
  	assert(dbname == "rocksdb" || dbname == "logsdb_pl" ||
  		   dbname == "logsdb_ll0" || dbname == "logsdb_l0");
  	printf("dbname: %s\n", dbname.c_str());
	if(dbname == "rocksdb"){
		//default
	}else{
		if(dbname == "logsdb_pl" || dbname == "logsdb_ll0"){
			options.enable_spdklogging = true;
			options.ssdlogging_type = "spdk";
			options.spdk_recovery = false;
			options.wal_dir = data_dir;
			if(props.GetProperty("log_dir") == "/home/spdk/optane/rocksdb/log"){
				pcie_addr = "trtype:PCIe traddr:0000:c9:00.0";
			}else if(props.GetProperty("log_dir") == "/home/spdk/nvme/rocksdb/log"){
				pcie_addr = "trtype:PCIe traddr:0000:25:00.0";
			}else{
				assert(0);
			}
			options.ssdlogging_path = pcie_addr;
			options.ssdlogging_num = 6;
			options.logging_server_num = 2;
			options.before_server_num = 0;
			options.after_server_num = 25 - options.logging_server_num - worker_threads;
			// async_num = round(reads * 20.0/worker_threads);
			// async_num = 500/worker_threads;
			// options.after_server_num = reads;
			async_num = 100;
			options.max_read_que_length = 1;
		}
		if(dbname == "logsdb_ll0" || dbname == "logsdb_l0"){
			options.lo_path = data_dir;
			options.l0_queue_num = 20;
			options.max_level = reads;
			options.max_compaction_bytes = 64ull<<20;
			assert(pcie_addr != "");
			if(is_load == 1){
				options.lo_env = rocksdb::NewSpdkEnv(rocksdb::Env::Default(), pcie_addr, options, false);
			}else{
				options.lo_env = rocksdb::NewSpdkEnv(rocksdb::Env::Default(), pcie_addr, options, true);
			}
		}
	}

	// Run
	{
		ycsbc::RocksDBClient3 rocksdb_client3(&wp, options, write_options, read_options, data_dir, loader_threads,
					  load_num, worker_threads, requests_num, async_num);
		// if(is_load == 1){
			// rocksdb_client2.Load();
		// }else{
			// rocksdb_client2.Warmup();
			rocksdb_client3.Work();
		// }

	}

	// delete
	if(dbname == "logsdb_ll0" || dbname == "logsdb_l0"){
		delete options.lo_env;
	}

	printf("\n\n\n");
	fflush(stdout);
	return 0;
}

void ParseCommandLine(int argc, const char *argv[], utils::Properties &props) {
	if(argc != 9){
		printf("usage: <workload file> <threads num> <data_dir> <log_dir> \
					   <enable_ssdlogging> <logs_num> <is_load> <dbname>\n");
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
	PrintWorkload(argv[1]);

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

	//8. is_load
	props.SetProperty("is_load", argv[7]);

	//9. dbname
	props.SetProperty("dbname", argv[8]);

	//check
	CheckArgs(props);
}

void CheckArgs(utils::Properties &props){
	;// TODO
}

void PrintWorkload(const char* filename){
	FILE *file = fopen(filename, "r");
	char line[201];
	fgets(line,200,file);
	printf("==================Workload=================\n");
	printf("%s\n", filename);
	while(!feof(file)){
		std::string s = std::string(line);
		if(s.find("#") != 0 && s != "\n" && s!=""){
			printf("%s", s.c_str());
		}
		fgets(line,200,file);
	}
	fclose(file);
	printf("==========================================\n");
	fflush(stdout);
}



