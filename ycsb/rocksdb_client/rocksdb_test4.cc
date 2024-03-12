#include "rocksdb_client4.h"
#include "iostream"
#include "cmath"

void ParseCommandLine(int argc, const char *argv[], utils::Properties &props);
void CheckArgs(utils::Properties &props);
void PrintWorkload(const char* filename);

int main(const int argc, const char *argv[]){
	utils::Properties props;
	ParseCommandLine(argc, argv, props);

	ycsbc::CoreWorkload wl;
	wl.Init(props);
	ycsbc::WorkloadProxy wp(&wl);

	const int loader_threads = stoi(props.GetProperty("threadcount"));
	const uint64_t load_num = stoull(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
	const int worker_threads = stoi(props.GetProperty("threadcount"));
	const uint64_t requests_num = stoull(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
	const double buffer_size = stod(props.GetProperty("buffer_size"));
	const std::string log_dir = props.GetProperty("log_dir");
	const std::string data_dir = props.GetProperty("data_dir");
	const int is_load = stoi(props.GetProperty("is_load"));
	const std::string dbname = props.GetProperty("dbname");
	const int max_level = stoi(props.GetProperty("max_level"));
	int async_num = 100;

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
	options.env = env;
	options.auto_config = false;
	options.dynamic_moving = true;
	if(dbname == "logsdb_ll0" && !options.auto_config){
		env->SetBgThreadCores(2, rocksdb::Env::HIGH);
		env->SetBgThreadCores(6, rocksdb::Env::LOW);
	}
	env->SetBackgroundThreads(2, rocksdb::Env::HIGH);
	env->SetBackgroundThreads(6, rocksdb::Env::LOW);
	options.max_background_jobs = 8;
	// options.max_subcompactions = 4;
	options.max_write_buffer_number = 4;

	// system(("rm " + data_dir + "/*").c_str());
	// system(("rm " + log_dir + "/*").c_str());
	// printf("loading database....\n");
	// if(is_load == 0)
	// 	system(("cp /home/spdk/nvme/rocksdb_bak/* " + data_dir + "/").c_str());
	// printf("loading finish\n");
	// system(("sudo fstrim " + data_dir).c_str());*/
	// system(("sudo fstrim " + options.wal_dir).c_str());

  	//===================DB=======================================
  	std::string pcie_addr = "";
  	printf("dbname: %s\n", dbname.c_str());
	if(dbname == "rocksdb"){
		options.auto_config = false;
	}if(dbname == "rocksdb_split"){
		assert(log_dir != "nologging");
		options.auto_config = false;
		options.max_level = max_level;
		options.lo_env = rocksdb::NewPosix2dEnv(options.wal_dir, max_level, data_dir);
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
			options.logging_server_num = 1;
			options.spandb_compactor_num = 0;
			options.spandb_worker_num = 40 - env->GetBgThreadCores(rocksdb::Env::HIGH) 
			 							   - env->GetBgThreadCores(rocksdb::Env::LOW)
			 							   - options.logging_server_num 
			 							   - worker_threads;
			async_num = 50;
			options.max_read_que_length = 2;
		}
		if(dbname == "logsdb_ll0" || dbname == "logsdb_l0"){
			options.max_level = max_level;
			options.lo_path = data_dir;
			options.l0_queue_num = 20;
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
		system("sync;echo 1 > /proc/sys/vm/drop_caches");
		system("echo 2 > /proc/sys/vm/drop_caches");
		system("echo 3 > /proc/sys/vm/drop_caches");
		fflush(stdout);
		printf("--------------memory usage----------------\n");
		fflush(stdout);
		system("free -h");
		fflush(stdout);
		printf("------------------------------------------\n");
		fflush(stdout);
		ycsbc::RocksDBClient4 rocksdb_client4(&wp, options, write_options, read_options, data_dir, loader_threads,
					  load_num, worker_threads, requests_num, async_num, stoi(props.GetProperty("n")),stoi(props.GetProperty("time")));
		// if(is_load == 1){
		// 	rocksdb_client4.Load();
		// }else{
			// rocksdb_client4.Warmup();
			rocksdb_client4.Work();
		// }
		// std::this_thread::sleep_for(std::chrono::seconds(100));
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
	if(argc != 11){
		printf("usage: <workload file> <threads num> <data_dir> <log_dir> \
					   <enable_ssdlogging> <logs_num> <is_load> <dbname> <n> <time>\n");
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
	props.SetProperty("max_level", argv[5]);

	//7. max buffer size
	props.SetProperty("buffer_size", argv[6]);

	//8. is_load
	props.SetProperty("is_load", argv[7]);

	//9. dbname
	props.SetProperty("dbname", argv[8]);

	//10. num
	props.SetProperty("n", argv[9]);

	//11. time
	props.SetProperty("time", argv[10]);

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



