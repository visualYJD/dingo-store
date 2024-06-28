
// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <cstdint>
#include <cstdlib>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "brpc/channel.h"
#include "brpc/controller.h"
#include "bthread/bthread.h"
#include "client_v2/cli11.h"
#include "client_v2/client_helper.h"
#include "client_v2/client_interation.h"
#include "client_v2/coordinator_client_function.h"
#include "client_v2/store_client_function.h"
#include "client_v2/store_tool_dump.h"
#include "client_v2/subcommand_coordinator.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/version.h"
#include "fmt/core.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "proto/common.pb.h"

DEFINE_bool(log_each_request, false, "Print log for each request");
DEFINE_bool(use_bthread, false, "Use bthread to send requests");
DEFINE_int32(thread_num, 1, "Number of threads sending requests");
DEFINE_int64(timeout_ms, 60000, "Timeout for each request");
DEFINE_int32(req_num, 1, "Number of requests");
DEFINE_string(method, "", "Request method");
DEFINE_string(id, "", "Request parameter id, for example: table_id for CreateTable/DropTable");
DEFINE_string(host, "127.0.0.1", "Request parameter host");
DEFINE_int32(port, 18888, "Request parameter port");
DEFINE_string(peer, "", "Request parameter peer, for example: 127.0.0.1:22101");
DEFINE_string(peers, "", "Request parameter peer, for example: 127.0.0.1:22101,127.0.0.1:22002,127.0.0.1:22103");
DEFINE_string(name, "", "Request parameter name, for example: table_id for GetSchemaByName/GetTableByName");
DEFINE_string(comment, "", "Request parameter comment");
DEFINE_string(user, "", "Request parameter user");
DEFINE_string(level, "", "Request log level [DEBUG, INFO, WARNING, ERROR, FATAL]");
DEFINE_string(keyring, "", "Request parameter keyring");
DEFINE_string(new_keyring, "", "Request parameter new_keyring");
DEFINE_string(coordinator_addr, "", "coordinator servr addr, for example: 127.0.0.1:8001");
DEFINE_string(addr, "", "coordinator servr addr, for example: 127.0.0.1:8001");
DEFINE_string(group, "0", "Id of the replication group, now coordinator use 0 as groupid");
DEFINE_int64(split_from_id, 0, "split_from_id");
DEFINE_int64(split_to_id, 0, "split_to_id");
DEFINE_string(split_key, "", "split_water_shed_key");
DEFINE_int64(source_id, 0, "source id");
DEFINE_int64(target_id, 0, "target id");
DEFINE_int64(peer_add_store_id, 0, "peer_add_store_id");
DEFINE_int64(peer_del_store_id, 0, "peer_del_store_id");
DEFINE_int64(store_id, 0, "store_id");
DEFINE_int64(start_region_cmd_id, 0, "start_region_cmd_id");
DEFINE_int64(end_region_cmd_id, 0, "end_region_cmd_id");
DEFINE_int64(region_id, 0, "region_id");
DEFINE_int64(region_cmd_id, 0, "region_cmd_id");
DEFINE_int64(task_list_id, 0, "task_list_id");
DEFINE_string(store_ids, "1001,1002,1003", "store_ids splited by ,");
DEFINE_int64(index, 0, "index");
DEFINE_int32(service_type, 0, "service type for getting leader, 0: meta or coordinator, 2: auto increment");
DEFINE_string(start_key, "", "start_key");
DEFINE_string(end_key, "", "end_key");
DEFINE_string(coor_url, "", "coordinator url");
DEFINE_string(url, "", "coordinator url");
DEFINE_int64(schema_id, 0, "schema_id");
DEFINE_int64(replica, 0, "replica num");
DEFINE_string(state, "", "state string");
DEFINE_bool(is_force, false, "force");
DEFINE_int32(max_elements, 0, "max_elements");
DEFINE_int32(dimension, 0, "dimension");
DEFINE_int32(efconstruction, 0, "efconstruction");
DEFINE_int32(nlinks, 0, "nlinks");
DEFINE_int32(ncentroids, 10, "ncentroids default : 10");
DEFINE_int32(part_count, 1, "partition count");
DEFINE_bool(with_auto_increment, true, "with_auto_increment");
DEFINE_string(vector_index_type, "", "vector_index_type:flat, hnsw, ivf_flat");
DEFINE_int32(round_num, 1, "Round of requests");
DEFINE_string(store_addrs, "", "server addrs");
DEFINE_string(raft_addrs, "127.0.0.1:10101:0,127.0.0.1:10102:0,127.0.0.1:10103:0", "raft addrs");
DEFINE_string(key, "", "Request key");
DEFINE_bool(key_is_hex, false, "Request key is hex");
DEFINE_bool(value_is_hex, false, "Request value is hex");
DEFINE_string(value, "", "Request values");
DEFINE_string(prefix, "", "key prefix");
DEFINE_string(region_prefix, "", "region_prefix");
DEFINE_int64(region_count, 1, "region count");
DEFINE_int64(table_id, 0, "table id");
DEFINE_string(table_name, "", "table name");
DEFINE_int64(index_id, 0, "index id");
DEFINE_string(raft_group, "store_default_test", "raft group");
DEFINE_int64(partition_num, 1, "table partition num");
DEFINE_int64(start_id, 0, "start id");
DEFINE_int64(end_id, 0, "end id");
DEFINE_int64(count, 50, "count");
DEFINE_int64(vector_id, 0, "vector_id");
DEFINE_int64(document_id, 0, "document_id");
DEFINE_int32(topn, 10, "top n");
DEFINE_int32(batch_count, 5, "batch count");
DEFINE_int64(part_id, 0, "part_id");
DEFINE_bool(without_vector, false, "Search vector without output vector data");
DEFINE_bool(without_scalar, false, "Search vector without scalar data");
DEFINE_bool(without_table, false, "Search vector without table data");
DEFINE_int64(ef_search, 0, "hnsw index search ef");
DEFINE_bool(bruteforce, false, "use bruteforce search");
DEFINE_int64(vector_index_id, 0, "vector index id unique. default 0");
DEFINE_string(vector_index_add_cost_file, "./cost.txt", "exec batch vector add. cost time");
DEFINE_int32(step_count, 1024, "step_count");
DEFINE_bool(print_vector_search_delay, false, "print vector search delay");
DEFINE_int32(offset, 0, "offset");
DEFINE_int64(limit, 50, "limit");
DEFINE_bool(is_reverse, false, "is_revers");
DEFINE_string(scalar_filter_key, "", "Request scalar_filter_key");
DEFINE_string(scalar_filter_value, "", "Request scalar_filter_value");
DEFINE_string(scalar_filter_key2, "", "Request scalar_filter_key");
DEFINE_string(scalar_filter_value2, "", "Request scalar_filter_value");
DEFINE_int64(ttl, 0, "ttl");
DEFINE_bool(auto_split, false, "auto split");
DEFINE_string(engine, "rocksdb", "engine type for table and index, [rocksdb, bdb]");
DEFINE_string(raw_engine, "", "engine type for table and index, [rocksdb, bdb]");
DEFINE_int64(status, 0, "status");
DEFINE_int64(errcode, -1, "errcode");
DEFINE_string(errmsg, "", "errmsg");

DEFINE_string(alg_type, "faiss", "use alg type. such as faiss or hnsw");
DEFINE_string(metric_type, "L2", "metric type. such as L2 or IP or cosine");
DEFINE_int32(left_vector_size, 2, "left vector size. <= 0 error");
DEFINE_int32(right_vector_size, 3, "right vector size. <= 0 error");
DEFINE_bool(is_return_normlize, true, "is return normlize default true");

DEFINE_int64(revision, 0, "revision");
DEFINE_int64(sub_revision, 0, "sub_revision");
DEFINE_string(range_end, "", "range_end for coor kv");
DEFINE_bool(count_only, false, "count_only for coor kv");
DEFINE_bool(keys_only, false, "keys_only for coor kv");
DEFINE_bool(need_prev_kv, false, "need_prev_kv for coor kv");
DEFINE_bool(ignore_value, false, "ignore_value for coor kv");
DEFINE_bool(ignore_lease, false, "ignore_lease for coor kv");
DEFINE_int64(lease, 0, "lease for coor kv put");
DEFINE_bool(no_put, false, "watch no put");
DEFINE_bool(no_delete, false, "watch no delete");
DEFINE_bool(wait_on_not_exist_key, false, "watch wait for not exist key");
DEFINE_int32(max_watch_count, 10, "max_watch_count");
DEFINE_bool(with_vector_ids, false, "Search vector with vector ids list default false");
DEFINE_bool(with_scalar_pre_filter, false, "Search vector with scalar data pre filter");
DEFINE_bool(with_scalar_post_filter, false, "Search vector with scalar data post filter");
DEFINE_bool(with_table_pre_filter, false, "Search vector with table data pre filter");
DEFINE_string(scalar_key, "", "Request scalar_key");
DEFINE_string(scalar_value, "", "Request scalar_value");
DEFINE_int32(vector_ids_count, 100, "vector ids count");
DEFINE_string(csv_data, "", "csv data");
DEFINE_string(json_data, "", "json data");
DEFINE_string(vector_data, "", "vector data");
DEFINE_string(csv_output, "", "csv output");

DEFINE_string(lock_name, "", "Request lock_name");
DEFINE_string(client_uuid, "", "Request client_uuid");

DEFINE_bool(store_create_region, false, "store create region");
DEFINE_string(db_path, "", "rocksdb path");

DEFINE_bool(show_vector, false, "show vector data");
DEFINE_string(metrics_type, "L2", "metrics type");
DEFINE_int64(safe_point, 0, "gc safe point");
DEFINE_int64(safe_point2, 0, "gc safe point");
DEFINE_string(gc_flag, "", "gc_flag action, must be oneof [start, stop], if empty, no action will be taken");
DEFINE_int64(def_version, 0, "version");

DEFINE_int64(tso_save_physical, 0, "new tso save physical");
DEFINE_int64(tso_new_physical, 0, "new tso physical");
DEFINE_int64(tso_new_logical, 0, "new tso logical");
DEFINE_int32(nsubvector, 8, "ivf pq default subvector nums 8");
DEFINE_int32(nbits_per_idx, 8, "ivf pq default nbits_per_idx 8");
DEFINE_double(radius, 10.1, "range search radius");
DEFINE_double(rate, 0.0, "rate");

DEFINE_bool(force_read_only, false, "force read only");
DEFINE_string(force_read_only_reason, "", "force read only reason");
DEFINE_int64(scan_id, 1, "scan id client supply");

// for meta watch
DEFINE_int64(watch_id, 0, "watch id client supply");
DEFINE_int64(start_revision, 0, "start revision client supply");

// for tenant
DEFINE_int64(tenant_id, 0, "tenant id");
DEFINE_bool(get_all_tenant, false, "get all tenant");

// scalar key speed up
DEFINE_bool(with_scalar_schema, false, "create vector index with scalar schema");

DEFINE_bool(enable_rocks_engine, false, "create table with rocks engine");

DEFINE_bool(dryrun, true, "dryrun");
DEFINE_int32(store_type, 0, "store type");

DEFINE_bool(include_archive, false, "include history archive");

DEFINE_bool(show_pretty, false, "show pretty");

bvar::LatencyRecorder g_latency_recorder("dingo-store");

// const std::map<std::string, std::vector<std::string>> kParamConstraint = {
//     {"RaftGroup", {"AddRegion", "ChangeRegion", "BatchAddRegion", "TestBatchPutGet"}},
//     {"RaftAddrs", {"AddRegion", "ChangeRegion", "BatchAddRegion", "TestBatchPutGet"}},
//     {"ThreadNum", {"BatchAddRegion", "TestBatchPutGet", "TestBatchPutGet"}},
//     {"RegionCount", {"BatchAddRegion", "TestBatchPutGet"}},
//     {"ReqNum", {"KvBatchGet", "TestBatchPutGet", "TestBatchPutGet", "AutoTest"}},
//     {"TableName", {"AutoTest"}},
//     {"PartitionNum", {"AutoTest"}},
// };

// int ValidateParam() {
//   if (FLAGS_raft_group.empty()) {
//     auto methods = kParamConstraint.find("RaftGroup")->second;
//     for (const auto& method : methods) {
//       if (method == FLAGS_method) {
//         DINGO_LOG(ERROR) << "missing param raft_group error";
//         return -1;
//       }
//     }
//   }

//   if (FLAGS_raft_addrs.empty()) {
//     auto methods = kParamConstraint.find("RaftAddrs")->second;
//     for (const auto& method : methods) {
//       if (method == FLAGS_method) {
//         DINGO_LOG(ERROR) << "missing param raft_addrs error";
//         return -1;
//       }
//     }
//   }

//   if (FLAGS_thread_num == 0) {
//     auto methods = kParamConstraint.find("ThreadNum")->second;
//     for (const auto& method : methods) {
//       if (method == FLAGS_method) {
//         DINGO_LOG(ERROR) << "missing param thread_num error";
//         return -1;
//       }
//     }
//   }

//   if (FLAGS_region_count == 0) {
//     auto methods = kParamConstraint.find("RegionCount")->second;
//     for (const auto& method : methods) {
//       if (method == FLAGS_method) {
//         DINGO_LOG(ERROR) << "missing param region_count error";
//         return -1;
//       }
//     }
//   }

//   if (FLAGS_req_num == 0) {
//     auto methods = kParamConstraint.find("ReqNum")->second;
//     for (const auto& method : methods) {
//       if (method == FLAGS_method) {
//         DINGO_LOG(ERROR) << "missing param req_num error";
//         return -1;
//       }
//     }
//   }

//   if (FLAGS_table_name.empty()) {
//     auto methods = kParamConstraint.find("TableName")->second;
//     for (const auto& method : methods) {
//       if (method == FLAGS_method) {
//         DINGO_LOG(ERROR) << "missing param table_name error";
//         return -1;
//       }
//     }
//   }

//   if (FLAGS_partition_num == 0) {
//     auto methods = kParamConstraint.find("PartitionNum")->second;
//     for (const auto& method : methods) {
//       if (method == FLAGS_method) {
//         DINGO_LOG(ERROR) << "missing param partition_num error";
//         return -1;
//       }
//     }
//   }

//   return 0;
// }

// std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction;
// std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction_meta;
// std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction_version;

//
void PrintSubcommandHelp(const CLI::App& app, const std::string& subcommand_name) {
  CLI::App* subcommand = app.get_subcommand(subcommand_name);
  if (subcommand) {
    std::cout << subcommand->help() << std::endl;
  } else {
    std::cout << "Unknown subcommand: " << subcommand_name << std::endl;
  }
}

int InteractiveCli() {
  // CLI::App app{"Interactive CLI11 Example with Subcommands"};
  CLI::App app{"This is dingo_client_v2"};
  client_v2::SetUpSubcommandRaftAddPeer(app);
  client_v2::SetUpSubcommandGetRegionMap(app);
  client_v2::SetUpSubcommandLogLevel(app);

  std::string input;

  while (true) {
    std::cout << "> ";
    std::getline(std::cin, input);

    if (input == "exit" || input == "quit") {
      break;
    }

    if (input == "help") {
      std::cout << app.help() << std::endl;
      continue;
    } else if (input.rfind("help", 0) == 0) {
      std::string subcommand_name = input.substr(5);
      if (subcommand_name.empty()) {
        std::cout << app.help() << std::endl;
      } else {
        PrintSubcommandHelp(app, subcommand_name);
      }
      continue;
    }
    std::vector<std::string> args;
    std::istringstream iss(input);
    for (std::string s; iss >> s;) args.push_back(s);

    std::vector<char*> argv;
    for (auto& arg : args) argv.push_back(&arg[0]);
    argv.push_back(nullptr);
    CLI11_PARSE(app, argv.size() - 1, argv.data());
  }
  return 0;
}
int main(int argc, char* argv[]) {
  FLAGS_minloglevel = google::GLOG_INFO;
  FLAGS_logtostdout = false;
  FLAGS_colorlogtostdout = true;
  FLAGS_logbufsecs = 0;
  google::InitGoogleLogging(argv[0]);

  if (argc > 1) {
    CLI::App app{"This is dingo_client_v2"};
    app.get_formatter()->column_width(40);  // 列的宽度

    client_v2::SetUpSubcommandRaftAddPeer(app);
    client_v2::SetUpSubcommandGetRegionMap(app);
    client_v2::SetUpSubcommandLogLevel(app);
    client_v2::SetUpSubcommandKvGet(app);
    client_v2::SetUpSubcommandKvPut(app);
    CLI11_PARSE(app, argc, argv);
  } else {
    InteractiveCli();
  }

  return 0;
}
