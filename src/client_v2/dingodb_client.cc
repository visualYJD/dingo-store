
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

bvar::LatencyRecorder g_latency_recorder("dingo-store");

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
