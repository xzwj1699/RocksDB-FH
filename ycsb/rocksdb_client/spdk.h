#pragma once

#include "spdk/stdinc.h"
#include "spdk/nvme.h"
#include "spdk/vmd.h"
#include "spdk/env.h"
#include "cstdio"
#include "string"

#include "vector"
#include "thread"
#include "sys/syscall.h"
#include "functional"

const uint64_t SPDK_MAX_IO_SIZE = 1ull<<32;

inline void SetAffinity(int coreid){
  assert(coreid >= 1);
  if(coreid % 2 == 1)
    coreid = (coreid+1)/2 * 4;
  else
    coreid = coreid * 2 + 3;
	cpu_set_t mask;
	CPU_ZERO(&mask);
	CPU_SET(coreid, &mask);
	int rc = sched_setaffinity(syscall(__NR_gettid), sizeof(mask), &mask);
	assert(rc == 0);
}

struct NSEntry { // namespace
	struct spdk_nvme_ctrlr	*ctrlr;
	struct spdk_nvme_ns	*ns;
	struct NSEntry		*next;
	struct spdk_nvme_qpair	**qpair;
	uint32_t sector_size;
};

struct SPDKInfo{
	struct spdk_nvme_ctrlr *controller = nullptr;
	int num_ns;  // number of namespace
	struct NSEntry *namespaces = nullptr;
	char name[200];
	std::string addr;
	uint32_t num_io_queues;
};

struct SPDKData {
	struct NSEntry	*ns_entry;
	char *buf;
	bool is_completed;
};

static void cleanup(struct SPDKInfo *spdk_info){
	struct NSEntry *ns_entry = spdk_info->namespaces;
	while (ns_entry) {
		struct NSEntry *next = ns_entry->next;
		free(ns_entry);
		ns_entry = next;
	}

	struct spdk_nvme_ctrlr *ctrlr = spdk_info->controller;
	if (ctrlr) {		
		free(ctrlr);
	}
}

static void register_ns(struct SPDKInfo *spdk_info, struct spdk_nvme_ctrlr *ctrlr, struct spdk_nvme_ns *ns){
	if (!spdk_nvme_ns_is_active(ns)) {
		return;
	}

	struct NSEntry *entry;
	entry = (struct NSEntry *)malloc(sizeof(struct NSEntry));
	if (entry == NULL) {
		perror("ns_entry malloc");
		exit(1);
	}
	entry->sector_size = spdk_nvme_ns_get_sector_size(ns);
	entry->ctrlr = ctrlr;
	entry->ns = ns;
	entry->next = spdk_info->namespaces;
	spdk_info->namespaces = entry;

	printf("Namespace ID: %d size: %juGB\n", spdk_nvme_ns_get_id(ns),spdk_nvme_ns_get_size(ns) / 1000000000);
}

static bool probe_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
		struct spdk_nvme_ctrlr_opts *opts){
	printf("Attaching to %s\n", trid->traddr);
	//opts->arb_mechanism = SPDK_NVME_CC_AMS_WRR;
	return true;
}

static void attach_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
	  				struct spdk_nvme_ctrlr *ctrlr, const struct spdk_nvme_ctrlr_opts *opts){
	printf("Attached to %s\n", trid->traddr);
	printf("num_io_queues: %u\n", opts->num_io_queues);

	struct SPDKInfo *spdk_info = (struct SPDKInfo *)cb_ctx;

	spdk_info->controller = ctrlr;
	spdk_info->num_io_queues = opts->num_io_queues;

	const struct spdk_nvme_ctrlr_data *cdata;
	cdata = spdk_nvme_ctrlr_get_data(ctrlr);
	snprintf(spdk_info->name, sizeof(spdk_info->name), "%-20.20s (%-20.20s)", cdata->mn, cdata->sn);

	int num_ns = spdk_nvme_ctrlr_get_num_ns(ctrlr);
	spdk_info->num_ns = num_ns;

	printf("Using controller %s with %d namespaces.\n", spdk_info->name, num_ns);
	struct spdk_nvme_ns *ns;
	for (int nsid = 1; nsid <= num_ns; nsid++) {
		ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
		if (ns == NULL) {
			continue;
		}
		register_ns(spdk_info, ctrlr, ns);
	}
}

static void init_qpair(struct SPDKInfo *spdk_info, int logging_queue_num){
	struct NSEntry *ns_entry = spdk_info->namespaces;
	uint32_t max_xfer_size = spdk_nvme_ns_get_max_io_xfer_size(ns_entry->ns);
	struct spdk_nvme_io_qpair_opts opts;
	spdk_nvme_ctrlr_get_default_io_qpair_opts(ns_entry->ctrlr, &opts, sizeof(opts));
	printf("Controller IO queue size: %u\n", opts.io_queue_requests);
	uint32_t size = (SPDK_MAX_IO_SIZE - 1) / max_xfer_size + 2;
	size += 1;
	if(opts.io_queue_requests < size){
		opts.io_queue_requests = size;
	}
	opts.delay_pcie_doorbell = true;
	while (ns_entry) {
		int qpair_num = spdk_info->num_io_queues;
		ns_entry->qpair = (spdk_nvme_qpair **)calloc(qpair_num, sizeof(struct spdk_nvme_qpair *));
		for (int i = 0; i < qpair_num; ++i){
			/*if(i < logging_queue_num){
				opts.qprio = SPDK_NVME_QPRIO_URGENT;
			}else{
				opts.qprio = SPDK_NVME_QPRIO_LOW;
			}*/
			ns_entry->qpair[i] = spdk_nvme_ctrlr_alloc_io_qpair(ns_entry->ctrlr, &opts, sizeof(opts));
			if (ns_entry->qpair[i] == NULL) {
				printf("ERROR: spdk_nvme_ctrlr_alloc_io_qpair() failed\n");
				return;
			}
		}
		ns_entry = ns_entry->next;
	}
}

struct SPDKInfo * InitSPDK(std::string device_addr, int logging_queue_num){
	struct SPDKInfo *spdk_info = new struct SPDKInfo;
	if(spdk_info == nullptr){
		fprintf(stderr, "Unable to malloc space for spdk info\n");
		return nullptr;
	}
	spdk_info->addr = device_addr;

	struct spdk_env_opts opts;
	spdk_env_opts_init(&opts);
	opts.shm_id = 0;  //shared memory group ID
	if (spdk_env_init(&opts) < 0) {
		fprintf(stderr, "Unable to initialize SPDK env\n");
		return nullptr;
	}

	struct spdk_nvme_transport_id trid;
	int rc = spdk_nvme_transport_id_parse(&trid, device_addr.c_str());
	if(rc != 0){
		fprintf(stderr, "spdk_nvme_transport_id_parse() failed: %s\n",strerror(rc * -1));
		cleanup(spdk_info);
		return nullptr;
	}

	rc = spdk_nvme_probe(&trid, spdk_info, probe_cb , attach_cb , NULL);
	if(rc != 0){
		fprintf(stderr, "spdk_nvme_probe() failed: %s\n",strerror(rc * -1));
		cleanup(spdk_info);
		return nullptr;
	}

	if (spdk_info->controller == NULL) {
		fprintf(stderr, "no NVMe controllers found\n");
		cleanup(spdk_info);
		return nullptr;
	}
	init_qpair(spdk_info, logging_queue_num);
	return spdk_info;
}

