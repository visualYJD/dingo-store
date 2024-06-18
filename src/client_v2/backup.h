
// refactor
/*
class SubCommand {
 public:
  SubCommand() = default;
  virtual ~SubCommand() = default;
  virtual std::string GetName() = 0;
  virtual std::string GetDescription() = 0;
  virtual std::string GetGroupName() = 0;
  virtual void SetUpSubCommand() = 0;
  virtual void Run() = 0;

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

      auto coordinator_interaction = std::make_shared<ServerInteraction>();
      if (!coordinator_interaction->Init(addrs)) {
        DINGO_LOG(ERROR) << "Fail to init coordinator_interaction, please check parameter --url=" << url;
        return -1;
      }

      InteractionManager::GetInstance().SetCoorinatorInteraction(coordinator_interaction);
    }

    // this is for legacy coordinator_client use, will be removed in the future
    if (!url.empty()) {
      coordinator_interaction = std::make_shared<dingodb::CoordinatorInteraction>();
      if (!coordinator_interaction->InitByNameService(
              url, dingodb::pb::common::CoordinatorServiceType::ServiceTypeCoordinator)) {
        DINGO_LOG(ERROR) << "Fail to init coordinator_interaction, please check parameter --url=" << url;
        return -1;
      }

      coordinator_interaction_meta = std::make_shared<dingodb::CoordinatorInteraction>();
      if (!coordinator_interaction_meta->InitByNameService(
              url, dingodb::pb::common::CoordinatorServiceType::ServiceTypeMeta)) {
        DINGO_LOG(ERROR) << "Fail to init coordinator_interaction_meta, please check parameter --url=" << url;
        return -1;
      }

      coordinator_interaction_version = std::make_shared<dingodb::CoordinatorInteraction>();
      if (!coordinator_interaction_version->InitByNameService(
              url, dingodb::pb::common::CoordinatorServiceType::ServiceTypeVersion)) {
        DINGO_LOG(ERROR) << "Fail to init coordinator_interaction_version, please check parameter --url=" << url;
        return -1;
      }
    }
    return 0;
  }
  std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction;
  std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction_meta;
  std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction_version;
};

class RaftAddPeerCommand : public SubCommand {
 public:
  RaftAddPeerCommand(std::shared_ptr<CLI::App> app);
  ~RaftAddPeerCommand() override = default;
  std::string GetName() override { return "RaftAddPeer"; };
  std::string GetDescription() override { return " Raft add peer"; };
  std::string GetGroupName() override { return "Coordinator Manager Commands"; };
  void SetUpSubCommand() override;
  void Run() override;

 private:
  std::shared_ptr<CLI::App> app_;
  RaftAddPeerCommandOptions opt_;
};

class RaftRemovePeerCommand : public SubCommand {
 public:
  RaftRemovePeerCommand(std::shared_ptr<CLI::App> app);
  ~RaftRemovePeerCommand() override = default;
  std::string GetName() override { return "RaftRemovePeer"; };
  std::string GetDescription() override { return "Raft remove peer"; };
  std::string GetGroupName() override { return "Coordinator Manager Commands"; };
  void SetUpSubCommand() override;
  void Run() override;

 private:
  std::shared_ptr<CLI::App> app_;
  SendRaftRemovePeer opt_;
};

class GetRegionMapCommand : public SubCommand {
 public:
  GetRegionMapCommand(std::shared_ptr<CLI::App> app);
  ~GetRegionMapCommand() override = default;
  std::string GetName() override { return "GetRegionMap"; };
  std::string GetDescription() override { return "Get region map"; };
  std::string GetGroupName() override { return "Coordinator Manager Commands"; };
  void SetUpSubCommand() override;
  void Run() override;

 private:
  std::shared_ptr<CLI::App> app_;
  GetRegionMapCommandOptions opt_;
};

class GetLogLevelCommand : public SubCommand {
 public:
  GetLogLevelCommand(std::shared_ptr<CLI::App> app);
  ~GetLogLevelCommand() override = default;
  std::string GetName() override { return "GetLogLevel"; };
  std::string GetDescription() override { return "Get log level"; };
  std::string GetGroupName() override { return "Coordinator Manager Commands"; };
  void SetUpSubCommand() override;
  void Run() override;

 private:
  std::shared_ptr<CLI::App> app_;
  GetLogLevelCommandOptions opt_;
};
*/