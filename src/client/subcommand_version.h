#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>

#include "client/cli11.h"
#include "client/client_helper.h"
#include "client/client_interation.h"
#include "client/coordinator_client_function.h"
#include "client/store_client_function.h"
#include "coordinator/coordinator_interaction.h"
#include "proto/coordinator.pb.h"

#ifndef DINGODB_SUBCOMMAND_VERSION_H_
#define DINGODB_SUBCOMMAND_VERSION_H_
namespace client {

int SetUp(std::string url);
struct RaftAddPeerCommandOptions {
  std::string coordinator_addr;
  std::string host;
  std::string peer;
  int32_t port;
  int index;
};

void SetUp_Subcommand_RaftAddPeer(CLI::App &app);
void Run_Subcommand_RaftAddPeer(RaftAddPeerCommandOptions const &opt);

struct GetRegionMapCommandOptions {
  std::string coor_url;
};

void SetUp_Subcommand_GetRegionMap(CLI::App &app);
void Run_Subcommand_GetRegionMap(GetRegionMapCommandOptions const &opt);

struct GetLogLevelCommandOptions {
  std::string coordinator_addr;
  int64_t timeout_ms;
};

void SetUp_Subcommand_LogLevel(CLI::App &app);
void Run_Subcommand_LogLevel(GetLogLevelCommandOptions const &opt);

}  // namespace client
#endif  // DINGODB_SUBCOMMAND_VERSION_H_