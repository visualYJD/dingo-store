#include "subcommand_version.h"
#include <iostream>

#include "client/client_helper.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/version.h"

namespace client {
std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction_;
std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction_meta_;
std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction_version_;
int SetUp(std::string url) {
  if (url.empty()) {
    DINGO_LOG(ERROR) << "coordinator url is empty, try to use file://./coor_list";
    url = "file://./coor_list";
  }

  auto ctx = std::make_shared<Context>();
  if (!url.empty()) {
    std::string path = url;
    path = path.replace(path.find("file://"), 7, "");
    auto addrs = Helper::GetAddrsFromFile(path);
    if (addrs.empty()) {
      DINGO_LOG(ERROR) << "url not find addr, path=" << path;
      return -1;
    }

    auto coordinator_interaction_ = std::make_shared<ServerInteraction>();
    if (!coordinator_interaction_->Init(addrs)) {
      DINGO_LOG(ERROR) << "Fail to init coordinator_interaction, please check parameter --url=" << url;
      return -1;
    }

    InteractionManager::GetInstance().SetCoorinatorInteraction(coordinator_interaction_);
  }

  // this is for legacy coordinator_client use, will be removed in the future
  if (!url.empty()) {
    coordinator_interaction_ = std::make_shared<dingodb::CoordinatorInteraction>();
    if (!coordinator_interaction_->InitByNameService(
            url, dingodb::pb::common::CoordinatorServiceType::ServiceTypeCoordinator)) {
      DINGO_LOG(ERROR) << "Fail to init coordinator_interaction, please check parameter --url=" << url;
      return -1;
    }

    coordinator_interaction_meta_ = std::make_shared<dingodb::CoordinatorInteraction>();
    if (!coordinator_interaction_meta_->InitByNameService(
            url, dingodb::pb::common::CoordinatorServiceType::ServiceTypeMeta)) {
      DINGO_LOG(ERROR) << "Fail to init coordinator_interaction_meta, please check parameter --url=" << url;
      return -1;
    }

    coordinator_interaction_version_ = std::make_shared<dingodb::CoordinatorInteraction>();
    if (!coordinator_interaction_version_->InitByNameService(
            url, dingodb::pb::common::CoordinatorServiceType::ServiceTypeVersion)) {
      DINGO_LOG(ERROR) << "Fail to init coordinator_interaction_version, please check parameter --url=" << url;
      return -1;
    }
  }
  return 0;
}
void SetUp_Subcommand_RaftAddPeer(CLI::App &app) {
  auto opt = std::make_shared<RaftAddPeerCommandOptions>();
  auto coor = app.add_subcommand("RaftAddPeer", "coordinator RaftAddPeer")->group("Coordinator Manager Commands");
  coor->add_option("--peer", opt->peer, "Request parameter peer, for example: 127.0.0.1:22101")
      ->group("Coordinator Manager Commands");
  coor->add_option("--host", opt->host, "Request parameter host")
      ->default_str("127.0.0.1")
      ->group("Coordinator Manager Commands");
  coor->add_option("--port", opt->port, "Request parameter port")
      ->default_val(18888)
      ->group("Coordinator Manager Commands");
  coor->add_option("--index", opt->index, "Index")->expected(0, 1)->group("Coordinator Manager Commands");
  coor->add_option("--coor_addr", opt->coordinator_addr, "Coordinator servr addr, for example: 127.0.0.1:22001")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { Run_Subcommand_RaftAddPeer(*opt); });
}

void Run_Subcommand_RaftAddPeer(RaftAddPeerCommandOptions const &opt) {
  dingodb::pb::coordinator::RaftControlRequest request;
  dingodb::pb::coordinator::RaftControlResponse response;

  if (!opt.peer.empty()) {
    request.set_add_peer(opt.peer);
  } else if (!opt.host.empty() && opt.port != 0) {
    request.set_add_peer(opt.host + ":" + std::to_string(opt.port));
  } else {
    DINGO_LOG(ERROR) << "peer, host or port is empty";
    return;
  }
  request.set_op_type(::dingodb::pb::coordinator::RaftControlOp::AddPeer);

  if (opt.index == 0) {
    request.set_node_index(dingodb::pb::coordinator::RaftControlNodeIndex::CoordinatorNodeIndex);
  } else if (opt.index == 1) {
    request.set_node_index(dingodb::pb::coordinator::RaftControlNodeIndex::AutoIncrementNodeIndex);
  } else {
    DINGO_LOG(ERROR) << "index is error";
    return;
  }

  brpc::Controller cntl;
  cntl.set_timeout_ms(FLAGS_timeout_ms);

  if (opt.coordinator_addr.empty()) {
    DINGO_LOG(ERROR) << "Please set --addr or --coordinator_addr";
    return;
  }

  brpc::Channel channel;
  if (!GetBrpcChannel(opt.coordinator_addr, channel)) {
    return;
  }
  dingodb::pb::coordinator::CoordinatorService_Stub stub(&channel);

  stub.RaftControl(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }
  DINGO_LOG(INFO) << "Received response"
                  << " request_attachment=" << cntl.request_attachment().size()
                  << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
  DINGO_LOG(INFO) << response.DebugString();
  std::cout<<"response:"<< response.DebugString();
}

void SetUp_Subcommand_GetRegionMap(CLI::App &app) {
  auto opt = std::make_shared<GetRegionMapCommandOptions>();
  auto coor = app.add_subcommand("GetRegionMap", "Get region map")->group("Coordinator Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { Run_Subcommand_GetRegionMap(*opt); });
}
void Run_Subcommand_GetRegionMap(GetRegionMapCommandOptions const &opt) {
  if (SetUp(opt.coor_url) < 0) {
    DINGO_LOG(ERROR) << "Set Up failed coor_url=" << opt.coor_url;
    exit(-1);
  }
  dingodb::pb::coordinator::GetRegionMapRequest request;
  dingodb::pb::coordinator::GetRegionMapResponse response;

  request.set_epoch(1);

  auto status = coordinator_interaction_->SendRequest("GetRegionMap", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;

  // for (const auto& region : response.regionmap().regions()) {
  //   DINGO_LOG(INFO) << region.DebugString();
  // }

  std::cout << "\n";
  int64_t normal_region_count = 0;
  int64_t online_region_count = 0;
  for (const auto &region : response.regionmap().regions()) {
    std::cout << "id=" << region.id() << " name=" << region.definition().name()
              << " epoch=" << region.definition().epoch().conf_version() << "," << region.definition().epoch().version()
              << " state=" << dingodb::pb::common::RegionState_Name(region.state()) << ","
              << dingodb::pb::common::RegionHeartbeatState_Name(region.status().heartbeat_status()) << ","
              << dingodb::pb::common::ReplicaStatus_Name(region.status().replica_status()) << ","
              << dingodb::pb::common::RegionRaftStatus_Name(region.status().raft_status())
              << " leader=" << region.leader_store_id() << " create=" << region.create_timestamp()
              << " update=" << region.status().last_update_timestamp() << " range=[0x"
              << dingodb::Helper::StringToHex(region.definition().range().start_key()) << ",0x"
              << dingodb::Helper::StringToHex(region.definition().range().end_key()) << "]\n";

    if (region.metrics().has_vector_index_status()) {
      std::cout << "vector_id=" << region.id()
                << " vector_status=" << region.metrics().vector_index_status().ShortDebugString() << "\n";
    }

    if (region.state() == dingodb::pb::common::RegionState::REGION_NORMAL) {
      normal_region_count++;
    }

    if (region.status().heartbeat_status() == dingodb::pb::common::RegionHeartbeatState::REGION_ONLINE) {
      online_region_count++;
    }
  }

  std::cout << '\n'
            << "region_count=[" << response.regionmap().regions_size() << "], normal_region_count=["
            << normal_region_count << "], online_region_count=[" << online_region_count << "]\n\n";
}

void SetUp_Subcommand_LogLevel(CLI::App &app) {
  auto opt = std::make_shared<GetLogLevelCommandOptions>();
  auto coor = app.add_subcommand("GetLogLevel", "Get log level")->group("Coordinator Manager Commands");
  coor->add_option("--coor_addr", opt->coordinator_addr, "Coordinator servr addr, for example: 127.0.0.1:22001")
      ->required()
      ->group("Coordinator Manager Commands");
  coor->add_option("--timeout_ms", opt->timeout_ms, "Timeout for each request")
      ->default_val(60000)
      ->group("Coordinator Manager Commands");

  coor->callback([opt]() { Run_Subcommand_LogLevel(*opt); });
}
void Run_Subcommand_LogLevel(GetLogLevelCommandOptions const &opt) {
  dingodb::pb::node::GetLogLevelRequest request;
  dingodb::pb::node::GetLogLevelResponse response;

  brpc::Controller cntl;
  cntl.set_timeout_ms(opt.timeout_ms);

  brpc::Channel channel;
  if (!GetBrpcChannel(opt.coordinator_addr, channel)) {
    return;
  }
  dingodb::pb::node::NodeService_Stub stub(&channel);

  stub.GetLogLevel(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
  }
  std::cout<<"response:"<< response.DebugString();
  DINGO_LOG(INFO) << response.DebugString();
  DINGO_LOG(INFO) << ::dingodb::pb::node::LogLevel_descriptor()->FindValueByNumber(response.log_level())->name();
}
}  // namespace client
