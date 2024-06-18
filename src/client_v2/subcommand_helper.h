#include "subcommand_coordinator.h"

#include <iostream>

#include "client_v2/client_helper.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/version.h"
namespace client_v2 {
class SubcommandHelper{
public:

static dingodb::pb::common::Engine GetEngine(const std::string &engine_name) {
  if (engine_name == "rocksdb") {
    return dingodb::pb::common::Engine::ENG_ROCKSDB;
  } else if (engine_name == "bdb") {
    return dingodb::pb::common::Engine::ENG_BDB;
  } else {
    DINGO_LOG(FATAL) << "engine_name is illegal, please input -engine=[rocksdb, bdb]";
  }
}

static int GetCreateTableIds(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction, int64_t count,
                      std::vector<int64_t> &table_ids) {
  dingodb::pb::meta::CreateTableIdsRequest request;
  dingodb::pb::meta::CreateTableIdsResponse response;

  auto *schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  request.set_count(count);

  auto status = coordinator_interaction->SendRequest("CreateTableIds", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();

  if (response.table_ids_size() > 0) {
    for (const auto &id : response.table_ids()) {
      table_ids.push_back(id.entity_id());
    }
    return 0;
  } else {
    return -1;
  }
}

static dingodb::pb::common::RawEngine GetRawEngine(const std::string &engine_name) {
  if (engine_name == "rocksdb") {
    return dingodb::pb::common::RawEngine::RAW_ENG_ROCKSDB;
  } else if (engine_name == "bdb") {
    return dingodb::pb::common::RawEngine::RAW_ENG_BDB;
  } else if (engine_name == "xdp") {
    return dingodb::pb::common::RawEngine::RAW_ENG_XDPROCKS;
  } else {
    DINGO_LOG(FATAL) << "raw_engine_name is illegal, please input -raw-engine=[rocksdb, bdb]";
  }

  return dingodb::pb::common::RawEngine::RAW_ENG_ROCKSDB;
}

static int GetCreateTableId(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction, int64_t& table_id) {
  dingodb::pb::meta::CreateTableIdRequest request;
  dingodb::pb::meta::CreateTableIdResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  auto status = coordinator_interaction->SendRequest("CreateTableId", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();

  if (response.has_table_id()) {
    table_id = response.table_id().entity_id();
    return 0;
  } else {
    return -1;
  }
}

};

}