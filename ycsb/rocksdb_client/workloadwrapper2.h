#pragma once

#include "core/workload_proxy.h"
#include "core/core_workload.h"


namespace ycsbc{

class WorkloadWrapper2{
  public:
  	struct Request{
  	 private:
  		ycsbc::Operation opt_;
  		const std::string key_;
  		const int len_; //value lenghth or scan lenghth
      uint64_t time_;
  	 public:
  		Request(ycsbc::Operation opt, std::string key, int len, uint64_t time):
  			opt_(opt),
  			key_(key), 
  			len_(len),
        time_(time){}

  		ycsbc::Operation Type(){ return opt_;}

  		std::string Key(){return key_;}

  		int Length(){return len_; }

      void SetType(ycsbc::Operation opt){opt_ = opt;}
      uint64_t Time(){return time_;}
  	};

  	WorkloadWrapper2(WorkloadProxy *workload_proxy, const uint64_t num, bool is_load, uint64_t n, double time):
  	   workload_proxy_(workload_proxy),
  	   num_(num),
  	   cursor_(0){
      assert(workload_proxy_ != nullptr);
  		requests_ = new Request*[num_];
  		ycsbc::Operation opt = NONE;
  		printf("loading workload (%ld)...\n", num_);
      uint64_t t = 0;
      uint64_t MICROS = 1000000;
  		for(uint64_t i=0; i<num_; i++){
        if(i % n == 0){
          t += (uint64_t)(time * MICROS);
        }
    		std::string table;
  			std::string key;
  			std::vector<std::string> fields;
  			std::vector<ycsbc::CoreWorkload::KVPair> values;
  		  int len;
  			if(is_load){
  				workload_proxy_->LoadInsertArgs(table, key, values);
  				assert(values.size() == 1);
  				requests_[i] = new Request(INSERT, key, values[0].second.length(), t);
  				continue;
  			}
    		opt = workload_proxy_->GetNextOperation();
    		if(opt == READ){
  				workload_proxy_->GetReadArgs(table, key, fields);
  				requests_[i] = new Request(opt, key, 0, t);
  			}else if(opt == UPDATE){
  				workload_proxy_->GetUpdateArgs(table, key, values);
  				assert(values.size() == 1);
  				requests_[i] = new Request(opt, key, values[0].second.length(), t);
  			}else if(opt == INSERT){
  				workload_proxy_->GetInsertArgs(table, key, values);
  				assert(values.size() == 1);
  				requests_[i] = new Request(opt, key, values[0].second.length(), t);
  			}else if(opt == READMODIFYWRITE){
  				workload_proxy_->GetReadModifyWriteArgs(table, key, fields, values);
  				assert(values.size() == 1);
  				requests_[i] = new Request(opt, key, values[0].second.length(), t);
  			}else if(opt == SCAN){
  				workload_proxy_->GetScanArgs(table, key, len, fields);
  				requests_[i] = new Request(opt, key, len, t);
  			}else{
  				throw utils::Exception("Operation request is not recognized!");
  			}
    	}
    	printf("loading workload finished\n");
  	}
  	~WorkloadWrapper2(){
  		for(uint64_t i=0; i<num_; i++){
  			if(requests_[i] != nullptr){
  				delete requests_[i];
  			} 
  		}
  		if(requests_ != nullptr){
  			delete requests_;
  		}
  	}

  	Request* GetNextRequest(){
  		uint64_t index = __sync_fetch_and_add(&cursor_, 1);
    //   if(index >= num_){
    //     printf("total_num: %ld, index: %ld\n", num_, index);
    //   }
  		assert(index < num_);
  		return requests_[index];
  	}

  private:
  	WorkloadProxy *workload_proxy_ = nullptr;
  	const uint64_t num_;
  	Request **requests_;
  	uint64_t cursor_;
};

}