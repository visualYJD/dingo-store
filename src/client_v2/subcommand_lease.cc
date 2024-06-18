#include <iostream>

#include "client_v2/client_helper.h"
#include "client_v2/subcommand_helper.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/version.h"
#include "proto/version.pb.h"
#include "subcommand_coordinator.h"
namespace client_v2 {

void SetUpSubcommandLeaseGrant(CLI::App &app) {
  auto opt = std::make_shared<LeaseGrantOptions>();
  auto coor = app.add_subcommand("LeaseGrant", "Lease grant")->group("Coordinator Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--id", opt->id, "Request parameter id")->required()->group("Coordinator Manager Commands");
  coor->add_option("--ttl", opt->ttl, "Request parameter ttl")->required()->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandLeaseGrant(*opt); });
}

void RunSubcommandLeaseGrant(LeaseGrantOptions const &opt) {
  if (SetUp(opt.coor_url) < 0) {
    DINGO_LOG(ERROR) << "Set Up failed coor_url=" << opt.coor_url;
    exit(-1);
  }
  dingodb::pb::version::LeaseGrantRequest request;
  dingodb::pb::version::LeaseGrantResponse response;
  request.set_id(opt.id);
  request.set_ttl(opt.ttl);

  auto status = coordinator_interaction_version_->SendRequest("LeaseGrant", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpSubcommandLeaseRevoke(CLI::App &app) {
  auto opt = std::make_shared<LeaseRevokeOptions>();
  auto coor = app.add_subcommand("LeaseRevoke", "Lease revoke")->group("Coordinator Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--id", opt->id, "Request parameter id")->required()->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandLeaseRevoke(*opt); });
}

void RunSubcommandLeaseRevoke(LeaseRevokeOptions const &opt) {
  if (SetUp(opt.coor_url) < 0) {
    DINGO_LOG(ERROR) << "Set Up failed coor_url=" << opt.coor_url;
    exit(-1);
  }
  dingodb::pb::version::LeaseRevokeRequest request;
  dingodb::pb::version::LeaseRevokeResponse response;

  request.set_id(opt.id);

  auto status = coordinator_interaction_version_->SendRequest("LeaseRevoke", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpSubcommandLeaseRenew(CLI::App &app) {
  auto opt = std::make_shared<LeaseRenewOptions>();
  auto coor = app.add_subcommand("LeaseRenew", "Lease renew")->group("Coordinator Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--id", opt->id, "Request parameter id")->required()->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandLeaseRenew(*opt); });
}

void RunSubcommandLeaseRenew(LeaseRenewOptions const &opt) {
  if (SetUp(opt.coor_url) < 0) {
    DINGO_LOG(ERROR) << "Set Up failed coor_url=" << opt.coor_url;
    exit(-1);
  }
  dingodb::pb::version::LeaseRenewRequest request;
  dingodb::pb::version::LeaseRenewResponse response;
  request.set_id(opt.id);

  auto status = coordinator_interaction_version_->SendRequest("LeaseRenew", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpSubcommandLeaseQuery(CLI::App &app) {
  auto opt = std::make_shared<LeaseQueryOptions>();
  auto coor = app.add_subcommand("LeaseQuery", "Lease query")->group("Coordinator Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->add_option("--id", opt->id, "Request parameter id")->required()->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandLeaseQuery(*opt); });
}

void RunSubcommandLeaseQuery(LeaseQueryOptions const &opt) {
  if (SetUp(opt.coor_url) < 0) {
    DINGO_LOG(ERROR) << "Set Up failed coor_url=" << opt.coor_url;
    exit(-1);
  }
  dingodb::pb::version::LeaseQueryRequest request;
  dingodb::pb::version::LeaseQueryResponse response;
  request.set_id(opt.id);
  request.set_keys(true);

  auto status = coordinator_interaction_version_->SendRequest("LeaseQuery", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}


void SetUpSubcommandListLeases(CLI::App &app) {
  auto opt = std::make_shared<ListLeasesOptions>();
  auto coor = app.add_subcommand("LeaseQuery", "Lease query")->group("Coordinator Manager Commands");
  coor->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  coor->callback([opt]() { RunSubcommandListLeases(*opt); });
}

void RunSubcommandListLeases(ListLeasesOptions const &opt) {
  if (SetUp(opt.coor_url) < 0) {
    DINGO_LOG(ERROR) << "Set Up failed coor_url=" << opt.coor_url;
    exit(-1);
  }
  dingodb::pb::version::ListLeasesRequest request;
  dingodb::pb::version::ListLeasesResponse response;

  auto status = coordinator_interaction_version_->SendRequest("ListLeases", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}



}  // namespace client_v2