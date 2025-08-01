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

#include "server/document_service.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "butil/compiler_specific.h"
#include "butil/status.h"
#include "common/constant.h"
#include "common/context.h"
#include "common/helper.h"
#include "common/synchronization.h"
#include "common/version.h"
#include "document/codec.h"
#include "engine/storage.h"
#include "fmt/core.h"
#include "gflags/gflags.h"
#include "meta/store_meta_manager.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
#include "proto/store.pb.h"
#include "server/server.h"
#include "server/service_helper.h"

using dingodb::pb::error::Errno;

namespace dingodb {

DEFINE_int64(document_max_batch_count, 4096, "document max batch count in one request");
DEFINE_int64(document_max_request_size, 33554432, "document max batch count in one request");
DEFINE_bool(enable_async_document_search, true, "enable async document search");
DEFINE_bool(enable_async_document_count, true, "enable async document count");
DEFINE_bool(enable_async_document_operation, true, "enable async document operation");

static void IndexRpcDone(BthreadCond* cond) { cond->DecreaseSignal(); }

DECLARE_int64(max_prewrite_count);
DECLARE_int64(stream_message_max_limit_size);
DECLARE_int64(document_max_background_task_count);

DECLARE_bool(dingo_log_switch_scalar_speed_up_detail);

DocumentServiceImpl::DocumentServiceImpl() = default;

bool DocumentServiceImpl::IsBackgroundPendingTaskCountExceed() {
  return document_index_manager_->GetBackgroundPendingTaskCount() > FLAGS_document_max_background_task_count;
}

static butil::Status ValidateDocumentBatchQueryRequest(StoragePtr storage,
                                                       const pb::document::DocumentBatchQueryRequest* request,
                                                       store::RegionPtr region) {
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->context().region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  if (request->document_ids().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param document_ids is error");
  }

  if (request->document_ids().size() > FLAGS_document_max_batch_count) {
    return butil::Status(pb::error::EDOCUMENT_EXCEED_MAX_BATCH_COUNT,
                         fmt::format("Param document_ids size {} is exceed max batch count {}",
                                     request->document_ids().size(), FLAGS_document_max_batch_count));
  }

  if (request->ts() < 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param ts is error");
  }

  status = storage->ValidateLeader(region);
  if (!status.ok()) {
    return status;
  }

  return ServiceHelper::ValidateDocumentRegion(region, Helper::PbRepeatedToVector(request->document_ids()));
}

void DoDocumentBatchQuery(StoragePtr storage, google::protobuf::RpcController* controller,
                          const pb::document::DocumentBatchQueryRequest* request,
                          pb::document::DocumentBatchQueryResponse* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateDocumentBatchQueryRequest(storage, request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Engine::DocumentReader::Context>();
  ctx->partition_id = region->PartitionId();
  ctx->region_id = region->Id();
  ctx->region_range = region->Range(false);
  ctx->document_ids = Helper::PbRepeatedToVector(request->document_ids());
  ctx->selected_scalar_keys = Helper::PbRepeatedToVector(request->selected_keys());
  ctx->with_scalar_data = !request->without_scalar_data();
  ctx->with_table_data = !request->without_table_data();
  ctx->raw_engine_type = region->GetRawEngineType();
  ctx->store_engine_type = region->GetStoreEngineType();
  ctx->ts = request->ts();

  std::vector<pb::common::DocumentWithId> document_with_ids;
  status = storage->DocumentBatchQuery(ctx, document_with_ids);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    return;
  }

  for (auto& document_with_id : document_with_ids) {
    response->add_doucments()->Swap(&document_with_id);
  }

  tracker->SetReadStoreTime();
}

void DocumentServiceImpl::DocumentBatchQuery(google::protobuf::RpcController* controller,
                                             const pb::document::DocumentBatchQueryRequest* request,
                                             pb::document::DocumentBatchQueryResponse* response,
                                             google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_document_operation) {
    return DoDocumentBatchQuery(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoDocumentBatchQuery(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateDocumentSearchRequest(StoragePtr storage,
                                                   const pb::document::DocumentSearchRequest* request,
                                                   store::RegionPtr region) {
  if (region == nullptr) {
    return butil::Status(
        pb::error::EREGION_NOT_FOUND,
        fmt::format("Not found region {} at server {}", request->context().region_id(), Server::GetInstance().Id()));
  }

  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->context().region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  if (request->parameter().top_n() > FLAGS_document_max_batch_count) {
    return butil::Status(pb::error::EDOCUMENT_EXCEED_MAX_BATCH_COUNT,
                         fmt::format("Param top_n {} is exceed max batch count {}", request->parameter().top_n(),
                                     FLAGS_document_max_batch_count));
  }

  // we limit the max request size to 4M and max batch count to 1024
  // for response, the limit is 10 times of request, which may be 40M
  // this size is less than the default max message size 64M
  if (request->parameter().top_n() > FLAGS_document_max_batch_count * 10) {
    return butil::Status(pb::error::EDOCUMENT_EXCEED_MAX_BATCH_COUNT,
                         fmt::format("Param top_n {} is exceed max batch count {} * 10", request->parameter().top_n(),
                                     FLAGS_document_max_batch_count));
  }

  if (request->parameter().top_n() < 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param top_n is error");
  }

  status = storage->ValidateLeader(region);
  if (!status.ok()) {
    return status;
  }

  if (!region->DocumentIndexWrapper()->IsReady()) {
    if (region->DocumentIndexWrapper()->IsBuildError()) {
      return butil::Status(pb::error::EDOCUMENT_INDEX_BUILD_ERROR,
                           fmt::format("Document index {} build error, please wait for recover.", region->Id()));
    }
    return butil::Status(pb::error::EDOCUMENT_INDEX_NOT_READY,
                         fmt::format("Document index {} not ready, please retry.", region->Id()));
  }

  return ServiceHelper::ValidateRegionState(region);
}

void DoDocumentSearch(StoragePtr storage, google::protobuf::RpcController* controller,
                      const pb::document::DocumentSearchRequest* request,
                      pb::document::DocumentSearchResponse* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateDocumentSearchRequest(storage, request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }
  if (request->parameter().top_n() == 0) {
    return;
  }

  auto* mut_request = const_cast<pb::document::DocumentSearchRequest*>(request);
  auto ctx = std::make_shared<Engine::DocumentReader::Context>();
  ctx->partition_id = region->PartitionId();
  ctx->region_id = region->Id();
  ctx->document_index = region->DocumentIndexWrapper();
  ctx->region_range = region->Range(false);
  ctx->parameter.Swap(mut_request->mutable_parameter());
  ctx->raw_engine_type = region->GetRawEngineType();
  ctx->store_engine_type = region->GetStoreEngineType();

  std::vector<pb::common::DocumentWithScore> document_results;
  status = storage->DocumentSearch(ctx, document_results);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    return;
  }

  for (auto& document_with_score : document_results) {
    *(response->add_document_with_scores()) = document_with_score;
  }
}

void DocumentServiceImpl::DocumentSearch(google::protobuf::RpcController* controller,
                                         const pb::document::DocumentSearchRequest* request,
                                         pb::document::DocumentSearchResponse* response,
                                         google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_document_search) {
    return DoDocumentSearch(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoDocumentSearch(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->ExecuteLeastQueue(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateDocumentSearchAllRequest(StoragePtr storage,
                                                      const pb::document::DocumentSearchAllRequest* request,
                                                      store::RegionPtr region) {
  if (region == nullptr) {
    return butil::Status(
        pb::error::EREGION_NOT_FOUND,
        fmt::format("Not found region {} at server {}", request->context().region_id(), Server::GetInstance().Id()));
  }

  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->context().region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }
  if (request->stream_meta().limit() <= 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "param limit is invalid");
  }
  if (request->stream_meta().limit() > FLAGS_stream_message_max_limit_size) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "param limit beyond max limit");
  }

  status = storage->ValidateLeader(region);
  if (!status.ok()) {
    return status;
  }

  if (!region->DocumentIndexWrapper()->IsReady()) {
    if (region->DocumentIndexWrapper()->IsBuildError()) {
      return butil::Status(pb::error::EDOCUMENT_INDEX_BUILD_ERROR,
                           fmt::format("Document index {} build error, please wait for recover.", region->Id()));
    }
    return butil::Status(pb::error::EDOCUMENT_INDEX_NOT_READY,
                         fmt::format("Document index {} not ready, please retry.", region->Id()));
  }

  return ServiceHelper::ValidateRegionState(region);
}

void DoDocumentSearchAll(StoragePtr storage, google::protobuf::RpcController* controller,
                         const pb::document::DocumentSearchAllRequest* request,
                         pb::document::DocumentSearchAllResponse* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateDocumentSearchAllRequest(storage, request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  auto* mut_request = const_cast<pb::document::DocumentSearchAllRequest*>(request);
  auto ctx = std::make_shared<Engine::DocumentReader::Context>();
  ctx->partition_id = region->PartitionId();
  ctx->region_id = region->Id();
  ctx->document_index = region->DocumentIndexWrapper();
  ctx->region_range = region->Range(false);
  ctx->parameter.Swap(mut_request->mutable_parameter());
  ctx->raw_engine_type = region->GetRawEngineType();
  ctx->store_engine_type = region->GetStoreEngineType();

  std::vector<pb::common::DocumentWithScore> document_results;
  bool has_more = false;
  status = storage->DocumentSearchAll(ctx, mut_request->stream_meta(), has_more, document_results);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    return;
  }

  for (auto& document_with_score : document_results) {
    *(response->add_document_with_scores()) = document_with_score;
  }
  auto stream = ctx->Stream();
  CHECK(stream != nullptr) << fmt::format("[region({})] stream is nullptr.", region_id);

  auto* mut_stream_meta = response->mutable_stream_meta();
  mut_stream_meta->set_stream_id(stream->StreamId());
  mut_stream_meta->set_has_more(has_more);
}

void DocumentServiceImpl::DocumentSearchAll(google::protobuf::RpcController* controller,
                                            const pb::document::DocumentSearchAllRequest* request,
                                            pb::document::DocumentSearchAllResponse* response,
                                            google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_document_search) {
    return DoDocumentSearchAll(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoDocumentSearchAll(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->ExecuteLeastQueue(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateDocumentAddRequest(StoragePtr storage, const pb::document::DocumentAddRequest* request,
                                                store::RegionPtr region) {
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->context().region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  if (request->documents().empty()) {
    return butil::Status(pb::error::EDOCUMENT_EMPTY, "Document quantity is empty");
  }

  if (request->documents_size() > FLAGS_document_max_batch_count) {
    return butil::Status(pb::error::EDOCUMENT_EXCEED_MAX_BATCH_COUNT,
                         fmt::format("Param documents size {} is exceed max batch count {}", request->documents_size(),
                                     FLAGS_document_max_batch_count));
  }

  if (request->ByteSizeLong() > FLAGS_document_max_request_size) {
    return butil::Status(pb::error::EDOCUMENT_EXCEED_MAX_REQUEST_SIZE,
                         fmt::format("Param documents size {} is exceed max batch size {}", request->ByteSizeLong(),
                                     FLAGS_document_max_request_size));
  }
  if (request->ttl() < 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param ttl is error");
  }

  status = storage->ValidateLeader(region);
  if (!status.ok()) {
    return status;
  }

  auto document_index_wrapper = region->DocumentIndexWrapper();
  if (!document_index_wrapper->IsReady()) {
    if (region->DocumentIndexWrapper()->IsBuildError()) {
      return butil::Status(pb::error::EDOCUMENT_INDEX_BUILD_ERROR,
                           fmt::format("Document index {} build error, please wait for recover.", region->Id()));
    }
    return butil::Status(pb::error::EDOCUMENT_INDEX_NOT_READY,
                         fmt::format("Document index {} not ready, please retry.", region->Id()));
  }

  for (const auto& document : request->documents()) {
    if (BAIDU_UNLIKELY(!DocumentCodec::IsLegalDocumentId(document.id()))) {
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS,
                           "Param document id is not allowed to be zero, INT64_MAX or negative");
    }
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  std::vector<int64_t> documents_ids;
  for (const auto& document : request->documents()) {
    documents_ids.push_back(document.id());
  }

  return ServiceHelper::ValidateDocumentRegion(region, documents_ids);
}

void DoDocumentAdd(StoragePtr storage, google::protobuf::RpcController* controller,
                   const pb::document::DocumentAddRequest* request, pb::document::DocumentAddResponse* response,
                   TrackClosure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  auto status = ValidateDocumentAddRequest(storage, request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id());
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());
  if (request->ttl() > 0) {
    ctx->SetTtl(Helper::TimestampMs() + request->ttl());
  }

  std::vector<pb::common::DocumentWithId> documents;
  for (const auto& document : request->documents()) {
    documents.push_back(document);
  }

  status = storage->DocumentAdd(ctx, is_sync, documents, request->is_update());
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    if (!is_sync) done->Run();
  }
}

void DocumentServiceImpl::DocumentAdd(google::protobuf::RpcController* controller,
                                      const pb::document::DocumentAddRequest* request,
                                      pb::document::DocumentAddResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (IsBackgroundPendingTaskCountExceed()) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "Background pending task count is full, please wait and retry");
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoDocumentAdd(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateDocumentDeleteRequest(StoragePtr storage,
                                                   const pb::document::DocumentDeleteRequest* request,
                                                   store::RegionPtr region) {
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->context().region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  if (request->ids().empty()) {
    return butil::Status(pb::error::EDOCUMENT_EMPTY, "Document id quantity is empty");
  }

  if (request->ids_size() > FLAGS_document_max_batch_count) {
    return butil::Status(pb::error::EDOCUMENT_EXCEED_MAX_BATCH_COUNT,
                         fmt::format("Param ids size {} is exceed max batch count {}", request->ids_size(),
                                     FLAGS_document_max_batch_count));
  }

  status = storage->ValidateLeader(region);
  if (!status.ok()) {
    return status;
  }

  auto document_index_wrapper = region->DocumentIndexWrapper();
  if (!document_index_wrapper->IsReady()) {
    if (region->DocumentIndexWrapper()->IsBuildError()) {
      return butil::Status(pb::error::EDOCUMENT_INDEX_BUILD_ERROR,
                           fmt::format("Document index {} build error, please wait for recover.", region->Id()));
    }
    return butil::Status(pb::error::EDOCUMENT_INDEX_NOT_READY,
                         fmt::format("Document index {} not ready, please retry.", region->Id()));
  }

  return ServiceHelper::ValidateDocumentRegion(region, Helper::PbRepeatedToVector(request->ids()));
}

void DoDocumentDelete(StoragePtr storage, google::protobuf::RpcController* controller,
                      const pb::document::DocumentDeleteRequest* request,
                      pb::document::DocumentDeleteResponse* response, TrackClosure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  auto status = ValidateDocumentDeleteRequest(storage, request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id());
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  status = storage->DocumentDelete(ctx, is_sync, region, Helper::PbRepeatedToVector(request->ids()));
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    if (!is_sync) done->Run();
  }
}

void DocumentServiceImpl::DocumentDelete(google::protobuf::RpcController* controller,
                                         const pb::document::DocumentDeleteRequest* request,
                                         pb::document::DocumentDeleteResponse* response,
                                         google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoDocumentDelete(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateDocumentGetBorderIdRequest(StoragePtr storage,
                                                        const pb::document::DocumentGetBorderIdRequest* request,
                                                        store::RegionPtr region) {
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->context().region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }
  if (request->ts() < 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param ts is error");
  }

  status = storage->ValidateLeader(region);
  if (!status.ok()) {
    return status;
  }

  return ServiceHelper::ValidateDocumentRegion(region, {});
}

void DoDocumentGetBorderId(StoragePtr storage, google::protobuf::RpcController* controller,
                           const pb::document::DocumentGetBorderIdRequest* request,
                           pb::document::DocumentGetBorderIdResponse* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateDocumentGetBorderIdRequest(storage, request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }
  int64_t document_id = 0;
  status = storage->DocumentGetBorderId(region, request->get_min(), request->ts(), document_id);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    return;
  }

  response->set_id(document_id);

  tracker->SetReadStoreTime();
}

void DocumentServiceImpl::DocumentGetBorderId(google::protobuf::RpcController* controller,
                                              const pb::document::DocumentGetBorderIdRequest* request,
                                              pb::document::DocumentGetBorderIdResponse* response,
                                              google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_document_operation) {
    return DoDocumentGetBorderId(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoDocumentGetBorderId(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateDocumentScanQueryRequest(StoragePtr storage,
                                                      const pb::document::DocumentScanQueryRequest* request,
                                                      store::RegionPtr region) {
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->context().region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  if (request->document_id_start() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param document_id_start is error");
  }

  if (request->max_scan_count() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param max_scan_count can't be 0");
  }

  if (request->max_scan_count() > FLAGS_document_max_batch_count) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param max_scan_count is bigger than %ld",
                         FLAGS_document_max_batch_count);
  }

  if (request->ts() < 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param ts is error");
  }

  status = storage->ValidateLeader(region);
  if (!status.ok()) {
    return status;
  }

  // for DocumentScanQuery, client can do scan from any id, so we don't need to check document id
  // sdk will merge, sort, limit_cut of all the results for user.
  return ServiceHelper::ValidateDocumentRegion(region, {});
}

void DoDocumentScanQuery(StoragePtr storage, google::protobuf::RpcController* controller,
                         const pb::document::DocumentScanQueryRequest* request,
                         pb::document::DocumentScanQueryResponse* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateDocumentScanQueryRequest(storage, request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  auto ctx = std::make_shared<Engine::DocumentReader::Context>();
  ctx->partition_id = region->PartitionId();
  ctx->region_id = region->Id();
  ctx->region_range = region->Range(false);
  ctx->selected_scalar_keys = Helper::PbRepeatedToVector(request->selected_keys());
  ctx->with_scalar_data = !request->without_scalar_data();
  ctx->with_table_data = !request->without_table_data();
  ctx->start_id = request->document_id_start();
  ctx->end_id = request->document_id_end();
  ctx->is_reverse = request->is_reverse_scan();
  ctx->limit = request->max_scan_count();
  ctx->raw_engine_type = region->GetRawEngineType();
  ctx->store_engine_type = region->GetStoreEngineType();
  ctx->ts = request->ts();

  std::vector<pb::common::DocumentWithId> document_with_ids;
  status = storage->DocumentScanQuery(ctx, document_with_ids);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    return;
  }

  for (auto& document_with_id : document_with_ids) {
    response->add_documents()->Swap(&document_with_id);
  }

  tracker->SetReadStoreTime();
}

void DocumentServiceImpl::DocumentScanQuery(google::protobuf::RpcController* controller,
                                            const pb::document::DocumentScanQueryRequest* request,
                                            pb::document::DocumentScanQueryResponse* response,
                                            google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_document_operation) {
    return DoDocumentScanQuery(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoDocumentScanQuery(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateDocumentGetRegionMetricsRequest(
    StoragePtr storage, const pb::document::DocumentGetRegionMetricsRequest* request, store::RegionPtr region) {
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->context().region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  status = storage->ValidateLeader(region);
  if (!status.ok()) {
    return status;
  }

  auto document_index_wrapper = region->DocumentIndexWrapper();
  if (!document_index_wrapper->IsReady()) {
    if (region->DocumentIndexWrapper()->IsBuildError()) {
      return butil::Status(pb::error::EDOCUMENT_INDEX_BUILD_ERROR,
                           fmt::format("Document index {} build error, please wait for recover.", region->Id()));
    }
    return butil::Status(pb::error::EDOCUMENT_INDEX_NOT_READY,
                         fmt::format("Document index {} not ready, please retry.", region->Id()));
  }

  return ServiceHelper::ValidateDocumentRegion(region, {});
}

void DoDocumentGetRegionMetrics(StoragePtr storage, google::protobuf::RpcController* controller,
                                const pb::document::DocumentGetRegionMetricsRequest* request,
                                pb::document::DocumentGetRegionMetricsResponse* response, TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateDocumentGetRegionMetricsRequest(storage, request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  pb::common::DocumentIndexMetrics metrics;
  status = storage->DocumentGetRegionMetrics(region, region->DocumentIndexWrapper(), metrics);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    return;
  }

  *(response->mutable_metrics()) = metrics;
}

void DocumentServiceImpl::DocumentGetRegionMetrics(google::protobuf::RpcController* controller,
                                                   const pb::document::DocumentGetRegionMetricsRequest* request,
                                                   pb::document::DocumentGetRegionMetricsResponse* response,
                                                   google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_document_operation) {
    return DoDocumentGetRegionMetrics(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoDocumentGetRegionMetrics(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateDocumentCountRequest(StoragePtr storage, const pb::document::DocumentCountRequest* request,
                                                  store::RegionPtr region) {
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->context().region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  if (request->document_id_start() > request->document_id_end()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param document_id_start/document_id_end range is error");
  }

  if (request->ts() < 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param ts is error");
  }

  status = storage->ValidateLeader(region);
  if (!status.ok()) {
    return status;
  }

  std::vector<int64_t> document_ids;
  if (request->document_id_start() != 0) {
    document_ids.push_back(request->document_id_start());
  }
  if (request->document_id_end() != 0) {
    document_ids.push_back(request->document_id_end() - 1);
  }

  return ServiceHelper::ValidateDocumentRegion(region, document_ids);
}

static pb::common::Range GenCountRange(store::RegionPtr region, int64_t start_document_id, int64_t end_document_id) {
  pb::common::Range result;

  auto range = region->Range(false);
  auto prefix = region->GetKeyPrefix();
  auto partition_id = region->PartitionId();
  if (start_document_id == 0) {
    result.set_start_key(range.start_key());
  } else {
    std::string key = DocumentCodec::PackageDocumentKey(prefix, partition_id, start_document_id);
    result.set_start_key(key);
  }

  if (end_document_id == 0) {
    result.set_end_key(range.end_key());
  } else {
    std::string key = DocumentCodec::PackageDocumentKey(prefix, partition_id, end_document_id);
    result.set_end_key(key);
  }

  return result;
}

void DoDocumentCount(StoragePtr storage, google::protobuf::RpcController* controller,
                     const pb::document::DocumentCountRequest* request, pb::document::DocumentCountResponse* response,
                     TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();

  butil::Status status = ValidateDocumentCountRequest(storage, request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  int64_t count = 0;
  status = storage->DocumentCount(
      region, GenCountRange(region, request->document_id_start(), request->document_id_end()), request->ts(), count);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    return;
  }

  response->set_count(count);

  tracker->SetReadStoreTime();
}

void DocumentServiceImpl::DocumentCount(google::protobuf::RpcController* controller,
                                        const pb::document::DocumentCountRequest* request,
                                        pb::document::DocumentCountResponse* response,
                                        ::google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (!FLAGS_enable_async_document_count) {
    return DoDocumentCount(storage_, controller, request, response, svr_done);
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoDocumentCount(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

// txn
static butil::Status ValidateTxnGetRequest(const pb::store::TxnGetRequest* request, store::RegionPtr region) {
  // check if region_epoch is match
  auto epoch_ret = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!epoch_ret.ok()) {
    return epoch_ret;
  }

  if (request->key().empty()) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  std::vector<std::string_view> keys = {request->key()};
  auto status = ServiceHelper::ValidateRegion(region, keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnGetDocument(StoragePtr storage, google::protobuf::RpcController* controller,
                      const pb::store::TxnGetRequest* request, pb::store::TxnGetResponse* response,
                      TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();
  region->SetTxnAccessMaxTs(request->start_ts());
  butil::Status status = ValidateTxnGetRequest(request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  std::vector<std::string> keys;
  auto* mut_request = const_cast<pb::store::TxnGetRequest*>(request);
  keys.emplace_back(std::move(*mut_request->release_key()));

  std::set<int64_t> resolved_locks;
  for (const auto& lock : request->context().resolved_locks()) {
    resolved_locks.insert(lock);
  }

  pb::store::TxnResultInfo txn_result_info;

  // read key check
  if (request->context().isolation_level() == pb::store::IsolationLevel::SnapshotIsolation &&
      region->CheckKeys(keys, request->context().isolation_level(), request->start_ts(), resolved_locks,
                        txn_result_info)) {
    ServiceHelper::SetError(response->mutable_error(), pb::error::Errno::ETXN_MEMORY_LOCK_CONFLICT,
                            fmt::format("Meet memory lock, please try later"));
    *response->mutable_txn_result() = txn_result_info;
    return;
  }

  auto ctx = std::make_shared<Context>();
  ctx->SetRegionId(request->context().region_id());
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  std::vector<pb::common::KeyValue> kvs;
  status = storage->TxnBatchGet(ctx, request->start_ts(), keys, resolved_locks, txn_result_info, kvs);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    return;
  }

  if (!kvs.empty()) {
    for (auto& kv : kvs) {
      pb::common::DocumentWithId document_with_id;

      if (!kv.value().empty()) {
        auto parse_ret = document_with_id.ParseFromString(kv.value());
        if (!parse_ret) {
          auto* err = response->mutable_error();
          err->set_errcode(static_cast<Errno>(pb::error::EINTERNAL));
          err->set_errmsg("parse document_with_id failed");
          return;
        }
      }

      response->mutable_document()->Swap(&document_with_id);
    }
  }
  *response->mutable_txn_result() = txn_result_info;
}

void DocumentServiceImpl::TxnGet(google::protobuf::RpcController* controller, const pb::store::TxnGetRequest* request,
                                 pb::store::TxnGetResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTxnGetDocument(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateTxnScanRequestIndex(const pb::store::TxnScanRequest* request, store::RegionPtr region,
                                                 const pb::common::Range& req_range) {
  if (request->limit() <= 0 && request->stream_meta().limit() <= 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "param limit is invalid");
  }
  if (request->limit() > FLAGS_stream_message_max_limit_size ||
      request->stream_meta().limit() > FLAGS_stream_message_max_limit_size) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "param limit beyond max limit");
  }
  if (request->start_ts() < 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "param start_ts is invalid");
  }

  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Not found region");
  }

  status = ServiceHelper::ValidateRange(req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRangeInRange(region->Range(false), req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRegionState(region);
  if (!status.ok()) {
    return status;
  }

  if (request->has_coprocessor()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Not support scan document with coprocessor");
  }

  return butil::Status();
}

void DoTxnScanDocument(StoragePtr storage, google::protobuf::RpcController* controller,
                       const pb::store::TxnScanRequest* request, pb::store::TxnScanResponse* response,
                       TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();
  region->SetTxnAccessMaxTs(request->start_ts());
  auto uniform_range = Helper::TransformRangeWithOptions(request->range());
  butil::Status status = ValidateTxnScanRequestIndex(request, region, uniform_range);
  if (!status.ok()) {
    if (pb::error::ERANGE_INVALID == static_cast<pb::error::Errno>(status.error_code())) {
      return;
    }
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  std::set<int64_t> resolved_locks;
  for (const auto& lock : request->context().resolved_locks()) {
    resolved_locks.insert(lock);
  }

  pb::store::TxnResultInfo txn_result_info;

  auto correction_range = Helper::IntersectRange(region->Range(false), uniform_range);
  // read key check
  if (request->context().isolation_level() == pb::store::IsolationLevel::SnapshotIsolation &&
      region->CheckRange(correction_range.start_key(), correction_range.end_key(), request->context().isolation_level(),
                         request->start_ts(), resolved_locks, txn_result_info)) {
    ServiceHelper::SetError(response->mutable_error(), pb::error::Errno::ETXN_MEMORY_LOCK_CONFLICT,
                            "Meet memory lock, please try later");
    *response->mutable_txn_result() = txn_result_info;
    return;
  }

  auto ctx = std::make_shared<Context>();
  ctx->SetRegionId(request->context().region_id());
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  std::vector<pb::common::KeyValue> kvs;
  bool has_more = false;
  std::string end_key{};

  status = storage->TxnScan(ctx, request->stream_meta(), request->start_ts(), correction_range, request->limit(),
                            request->key_only(), request->is_reverse(), resolved_locks, txn_result_info, kvs, has_more,
                            end_key, !request->has_coprocessor(), request->coprocessor());
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    return;
  }

  if (!kvs.empty()) {
    for (auto& kv : kvs) {
      pb::common::DocumentWithId document_with_id;

      if (!kv.value().empty()) {
        auto parse_ret = document_with_id.ParseFromString(kv.value());
        if (!parse_ret) {
          auto* err = response->mutable_error();
          err->set_errcode(static_cast<Errno>(pb::error::EINTERNAL));
          err->set_errmsg("parse document_with_id failed");
          return;
        }
      }

      response->add_documents()->Swap(&document_with_id);
    }
  }

  if (txn_result_info.ByteSizeLong() > 0) {
    *response->mutable_txn_result() = txn_result_info;
  }
  response->set_end_key(end_key);
  response->set_has_more(has_more);

  auto stream = ctx->Stream();
  CHECK(stream != nullptr) << fmt::format("[region({})] stream is nullptr.", region_id);

  auto* mut_stream_meta = response->mutable_stream_meta();
  mut_stream_meta->set_stream_id(stream->StreamId());
  mut_stream_meta->set_has_more(has_more);

  tracker->SetReadStoreTime();
}

void DocumentServiceImpl::TxnScan(google::protobuf::RpcController* controller, const pb::store::TxnScanRequest* request,
                                  pb::store::TxnScanResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTxnScanDocument(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateDocumentTxnPessimisticLockRequest(
    StoragePtr storage, const dingodb::pb::store::TxnPessimisticLockRequest* request, store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->mutations_size() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "mutations is empty");
  }

  if (request->mutations_size() > FLAGS_max_prewrite_count) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "mutations size is too large, max=1024");
  }

  if (request->primary_lock().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "primary_lock is empty");
  }

  if (request->start_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_ts is 0");
  }

  if (request->lock_ttl() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "lock_ttl is 0");
  }

  if (request->for_update_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "for_update_ts is 0");
  }

  status = storage->ValidateLeader(region);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  if (!region->DocumentIndexWrapper()->IsReady()) {
    if (region->DocumentIndexWrapper()->IsBuildError()) {
      return butil::Status(pb::error::EDOCUMENT_INDEX_BUILD_ERROR,
                           fmt::format("Document index {} build error, please wait for recover.", region->Id()));
    }
    return butil::Status(pb::error::EDOCUMENT_INDEX_NOT_READY,
                         fmt::format("Document index {} not ready, please retry.", region->Id()));
  }

  std::vector<std::string_view> keys;
  for (const auto& mutation : request->mutations()) {
    if (mutation.key().empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
    }
    keys.push_back(mutation.key());

    if (mutation.value().size() > 8192) {
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "value size is too large, max=8192");
    }

    if (mutation.op() != pb::store::Op::Lock) {
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "op is not Lock");
    }
  }
  status = ServiceHelper::ValidateRegion(region, keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoDocumentTxnPessimisticLock(StoragePtr storage, google::protobuf::RpcController* controller,
                                  const dingodb::pb::store::TxnPessimisticLockRequest* request,
                                  dingodb::pb::store::TxnPessimisticLockResponse* response, TrackClosure* done,
                                  bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();
  auto status = ValidateDocumentTxnPessimisticLockRequest(storage, request, region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  // check latches
  std::vector<std::string> keys_for_lock;
  for (const auto& mutation : request->mutations()) {
    keys_for_lock.push_back(mutation.key());
  }

  LatchContext latch_ctx(region, keys_for_lock);
  ServiceHelper::LatchesAcquire(latch_ctx, true);
  DEFER(ServiceHelper::LatchesRelease(latch_ctx));

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(region_id);
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  std::vector<pb::store::Mutation> mutations;
  for (const auto& mutation : request->mutations()) {
    mutations.emplace_back(mutation);
  }

  std::vector<pb::common::KeyValue> kvs;

  status = storage->TxnPessimisticLock(ctx, mutations, request->primary_lock(), request->start_ts(),
                                       request->lock_ttl(), request->for_update_ts(), request->return_values(), kvs);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    if (!is_sync) done->Run();
  }
  if (request->return_values() && !kvs.empty()) {
    for (auto& kv : kvs) {
      pb::common::DocumentWithId document_with_id;

      if (!kv.value().empty()) {
        auto parse_ret = document_with_id.ParseFromString(kv.value());
        if (!parse_ret) {
          auto* err = response->mutable_error();
          err->set_errcode(static_cast<Errno>(pb::error::EINTERNAL));
          err->set_errmsg("parse document_with_id failed");
          return;
        }
      }

      response->add_documents()->Swap(&document_with_id);
    }
  }
}

void DocumentServiceImpl::TxnPessimisticLock(google::protobuf::RpcController* controller,
                                             const pb::store::TxnPessimisticLockRequest* request,
                                             pb::store::TxnPessimisticLockResponse* response,
                                             google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoDocumentTxnPessimisticLock(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

butil::Status ValidateTxnPessimisticRollbackRequest(const dingodb::pb::store::TxnPessimisticRollbackRequest* request);

void DoTxnPessimisticRollback(StoragePtr storage, google::protobuf::RpcController* controller,
                              const dingodb::pb::store::TxnPessimisticRollbackRequest* request,
                              dingodb::pb::store::TxnPessimisticRollbackResponse* response, TrackClosure* done,
                              bool is_sync);

void DocumentServiceImpl::TxnPessimisticRollback(google::protobuf::RpcController* controller,
                                                 const pb::store::TxnPessimisticRollbackRequest* request,
                                                 pb::store::TxnPessimisticRollbackResponse* response,
                                                 google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTxnPessimisticRollback(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateDocumentTxnPrewriteRequest(StoragePtr storage,
                                                        const pb::store::TxnPrewriteRequest* request,
                                                        store::RegionPtr region) {
  // check if region_epoch is match
  auto epoch_ret = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!epoch_ret.ok()) {
    return epoch_ret;
  }

  if (request->mutations_size() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "mutations is empty");
  }

  if (request->mutations_size() > FLAGS_max_prewrite_count) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "mutations size is too large, max=1024");
  }

  if (request->primary_lock().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "primary_lock is empty");
  }

  if (request->start_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_ts is 0");
  }

  if (request->lock_ttl() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "lock_ttl is 0");
  }

  if (request->txn_size() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "txn_size is 0");
  }

  std::vector<std::string_view> keys;
  for (const auto& mutation : request->mutations()) {
    if (BAIDU_UNLIKELY(mutation.key().empty())) {
      return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
    }
    keys.push_back(mutation.key());
  }

  auto status = ServiceHelper::ValidateRegion(region, keys);
  if (!status.ok()) {
    return status;
  }

  if (BAIDU_UNLIKELY(request->mutations_size() > FLAGS_document_max_batch_count)) {
    return butil::Status(pb::error::EDOCUMENT_EXCEED_MAX_BATCH_COUNT,
                         fmt::format("Param documents size {} is exceed max batch count {}", request->mutations_size(),
                                     FLAGS_document_max_batch_count));
  }

  if (BAIDU_UNLIKELY(request->ByteSizeLong() > FLAGS_document_max_request_size)) {
    return butil::Status(pb::error::EDOCUMENT_EXCEED_MAX_REQUEST_SIZE,
                         fmt::format("Param documents size {} is exceed max batch size {}", request->ByteSizeLong(),
                                     FLAGS_document_max_request_size));
  }

  status = storage->ValidateLeader(region);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  if (!region->DocumentIndexWrapper()->IsReady()) {
    if (region->DocumentIndexWrapper()->IsBuildError()) {
      return butil::Status(pb::error::EDOCUMENT_INDEX_BUILD_ERROR,
                           fmt::format("Document index {} build error, please wait for recover.", region->Id()));
    }
    return butil::Status(pb::error::EDOCUMENT_INDEX_NOT_READY,
                         fmt::format("Document index {} not ready, please retry.", region->Id()));
  }

  auto document_index_wrapper = region->DocumentIndexWrapper();

  std::vector<int64_t> document_ids;

  for (const auto& mutation : request->mutations()) {
    // check document_id is correctly encoded in key of mutation
    int64_t document_id = DocumentCodec::UnPackageDocumentId(mutation.key());

    if (BAIDU_UNLIKELY(!DocumentCodec::IsLegalDocumentId(document_id))) {
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS,
                           "Param document id is not allowed to be zero, INT64_MAX or negative, please check the "
                           "document_id encoded in mutation key");
    }

    document_ids.push_back(document_id);

    // check if document_id is legal
    const auto& document = mutation.document();
    if (mutation.op() == pb::store::Op::Put || mutation.op() == pb::store::PutIfAbsent) {
      if (BAIDU_UNLIKELY(!DocumentCodec::IsLegalDocumentId(document_id))) {
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS,
                             "Param  ector id is not allowed to be zero, INT64_MAX or negative, please check the "
                             "document_id in DocumentWithId");
      }

      if (BAIDU_UNLIKELY(document.id() != document_id)) {
        return butil::Status(
            pb::error::EILLEGAL_PARAMTETERS,
            "Param document id in DocumentWithId is not equal to document_id in mutation key, please check "
            "the mutation key and DocumentWithId");
      }

      if (BAIDU_UNLIKELY(document.document().document_data().empty())) {
        return butil::Status(pb::error::EDOCUMENT_EMPTY, "document is empty");
      }

      // TODO: check schema before txn prewrite
      //   auto scalar_schema = region->ScalarSchema();
      //   DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_scalar_speed_up_detail)
      //       << fmt::format("document txn prewrite scalar schema: {}", scalar_schema.ShortDebugString());
      //   if (0 != scalar_schema.fields_size()) {
      //     status = DocumentIndexUtils::ValidateDocumentScalarData(scalar_schema, document.scalar_data());
      //     if (!status.ok()) {
      //       DINGO_LOG(ERROR) << status.error_cstr();
      //       return status;
      //     }
      //   }

    } else if (mutation.op() == pb::store::Op::Delete || mutation.op() == pb::store::Op::CheckNotExists) {
      if (BAIDU_UNLIKELY(!DocumentCodec::IsLegalDocumentId(document_id))) {
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS,
                             "Param document id is not allowed to be zero, INT64_MAX or negative, please check the "
                             "document_id encoded in mutation key");
      }

      continue;
    } else {
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param op of mutation is error");
    }
  }

  return ServiceHelper::ValidateDocumentRegion(region, document_ids);
}

void DoTxnPrewriteDocument(StoragePtr storage, google::protobuf::RpcController* controller,
                           const pb::store::TxnPrewriteRequest* request, pb::store::TxnPrewriteResponse* response,
                           TrackClosure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();
  auto status = ValidateDocumentTxnPrewriteRequest(storage, request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  // check latches
  std::vector<std::string> keys_for_lock;
  for (const auto& mutation : request->mutations()) {
    keys_for_lock.push_back(std::to_string(mutation.document().id()));
  }

  LatchContext latch_ctx(region, keys_for_lock);
  ServiceHelper::LatchesAcquire(latch_ctx, true);
  DEFER(ServiceHelper::LatchesRelease(latch_ctx));

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id());
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  std::vector<pb::store::Mutation> mutations;
  mutations.reserve(request->mutations_size());
  for (const auto& mutation : request->mutations()) {
    pb::store::Mutation store_mutation;
    store_mutation.set_op(mutation.op());
    store_mutation.set_key(mutation.key());
    store_mutation.set_value(mutation.document().SerializeAsString());
    mutations.push_back(store_mutation);
  }

  std::map<int64_t, int64_t> for_update_ts_checks;
  for (const auto& for_update_ts_check : request->for_update_ts_checks()) {
    for_update_ts_checks.insert_or_assign(for_update_ts_check.index(), for_update_ts_check.expected_for_update_ts());
  }

  std::map<int64_t, std::string> lock_extra_datas;
  for (const auto& lock_extra_data : request->lock_extra_datas()) {
    lock_extra_datas.insert_or_assign(lock_extra_data.index(), lock_extra_data.extra_data());
  }

  std::vector<int64_t> pessimistic_checks;
  pessimistic_checks.reserve(request->pessimistic_checks_size());
  for (const auto& pessimistic_check : request->pessimistic_checks()) {
    pessimistic_checks.push_back(pessimistic_check);
  }
  std::vector<std::string> secondaries;
  secondaries.reserve(request->secondaries_size());
  if (request->use_async_commit()) {
    for (const auto& secondary : request->secondaries()) {
      secondaries.push_back(secondary);
    }
  }

  std::vector<pb::common::KeyValue> kvs;
  status = storage->TxnPrewrite(ctx, region, mutations, request->primary_lock(), request->start_ts(),
                                request->lock_ttl(), request->txn_size(), request->try_one_pc(),
                                request->min_commit_ts(), request->max_commit_ts(), pessimistic_checks,
                                for_update_ts_checks, lock_extra_datas, secondaries);

  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    if (!is_sync) done->Run();
  }
}

void DocumentServiceImpl::TxnPrewrite(google::protobuf::RpcController* controller,
                                      const pb::store::TxnPrewriteRequest* request,
                                      pb::store::TxnPrewriteResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (IsBackgroundPendingTaskCountExceed()) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "Background pending task count is full, please wait and retry");
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTxnPrewriteDocument(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateTxnCommitRequest(const pb::store::TxnCommitRequest* request, store::RegionPtr region) {
  // check if region_epoch is match
  auto epoch_ret = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!epoch_ret.ok()) {
    return epoch_ret;
  }

  if (request->start_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_ts is 0");
  }

  if (request->commit_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "commit_ts is 0");
  }

  if (request->keys().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "keys is empty");
  }

  if (request->keys_size() > FLAGS_document_max_batch_count) {
    return butil::Status(pb::error::EDOCUMENT_EXCEED_MAX_BATCH_COUNT,
                         fmt::format("Param documents size {} is exceed max batch count {}", request->keys_size(),
                                     FLAGS_document_max_batch_count));
  }

  if (request->ByteSizeLong() > FLAGS_document_max_request_size) {
    return butil::Status(pb::error::EDOCUMENT_EXCEED_MAX_REQUEST_SIZE,
                         fmt::format("Param documents size {} is exceed max batch size {}", request->ByteSizeLong(),
                                     FLAGS_document_max_request_size));
  }

  if (!region->DocumentIndexWrapper()->IsReady()) {
    if (region->DocumentIndexWrapper()->IsBuildError()) {
      return butil::Status(pb::error::EDOCUMENT_INDEX_BUILD_ERROR,
                           fmt::format("Document index {} build error, please wait for recover.", region->Id()));
    }
    return butil::Status(pb::error::EDOCUMENT_INDEX_NOT_READY,
                         fmt::format("Document index {} not ready, please retry.", region->Id()));
  }

  auto status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  std::vector<int64_t> document_ids;
  for (const auto& key : request->keys()) {
    int64_t document_id = DocumentCodec::UnPackageDocumentId(key);
    if (document_id == 0) {
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param document id is error");
    }
    document_ids.push_back(document_id);
  }

  auto ret1 = ServiceHelper::ValidateDocumentRegion(region, document_ids);
  if (!ret1.ok()) {
    return ret1;
  }

  std::vector<std::string_view> keys;
  for (const auto& key : request->keys()) {
    if (key.empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
    }
    keys.push_back(key);
  }
  status = ServiceHelper::ValidateRegion(region, keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status::OK();
}

void DoTxnCommit(StoragePtr storage, google::protobuf::RpcController* controller,
                 const pb::store::TxnCommitRequest* request, pb::store::TxnCommitResponse* response, TrackClosure* done,
                 bool is_sync);

void DocumentServiceImpl::TxnCommit(google::protobuf::RpcController* controller,
                                    const pb::store::TxnCommitRequest* request, pb::store::TxnCommitResponse* response,
                                    google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (IsBackgroundPendingTaskCountExceed()) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "Background pending task count is full, please wait and retry");
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTxnCommit(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status DocumentValidateTxnCheckTxnStatusRequest(const pb::store::TxnCheckTxnStatusRequest* request,
                                                              store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  if (request->primary_key().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "primary_key is empty");
  }

  if (request->lock_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "lock_ts is 0");
  }

  if (request->caller_start_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "caller_start_ts is 0");
  }

  if (request->current_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "current_ts is 0");
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  std::vector<std::string_view> keys;
  keys.push_back(request->primary_key());
  status = ServiceHelper::ValidateRegion(region, keys);
  if (!status.ok()) {
    return status;
  }

  if (!region->DocumentIndexWrapper()->IsReady()) {
    if (region->DocumentIndexWrapper()->IsBuildError()) {
      return butil::Status(pb::error::EDOCUMENT_INDEX_BUILD_ERROR,
                           fmt::format("Document index {} build error, please wait for recover.", region->Id()));
    }
    return butil::Status(pb::error::EDOCUMENT_INDEX_NOT_READY,
                         fmt::format("Document index {} not ready, please retry.", region->Id()));
  }

  return butil::Status();
}

void DoTxnCheckTxnStatus(StoragePtr storage, google::protobuf::RpcController* controller,
                         const pb::store::TxnCheckTxnStatusRequest* request,
                         pb::store::TxnCheckTxnStatusResponse* response, TrackClosure* done, bool is_sync);

void DocumentServiceImpl::TxnCheckTxnStatus(google::protobuf::RpcController* controller,
                                            const pb::store::TxnCheckTxnStatusRequest* request,
                                            pb::store::TxnCheckTxnStatusResponse* response,
                                            google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (IsBackgroundPendingTaskCountExceed()) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "Background pending task count is full, please wait and retry");
    return;
  }

  auto region = svr_done->GetRegion();
  int64_t region_id = request->context().region_id();

  auto status = DocumentValidateTxnCheckTxnStatusRequest(request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTxnCheckTxnStatus(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status DocumentValidateTxnResolveLockRequest(const pb::store::TxnResolveLockRequest* request,
                                                           store::RegionPtr region) {
  // check if region_epoch is match
  auto epoch_ret = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!epoch_ret.ok()) {
    return epoch_ret;
  }

  if (request->start_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_ts is 0, it's illegal");
  }

  if (request->commit_ts() < 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "commit_ts < 0, it's illegal");
  }

  if (request->commit_ts() > 0 && request->commit_ts() < request->start_ts()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "commit_ts < start_ts, it's illegal");
  }

  if (request->keys_size() > 0) {
    for (const auto& key : request->keys()) {
      if (key.empty()) {
        return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
      }
      std::vector<std::string_view> keys;
      keys.push_back(key);
      auto status = ServiceHelper::ValidateRegion(region, keys);
      if (!status.ok()) {
        return status;
      }
    }
  }

  if (!region->DocumentIndexWrapper()->IsReady()) {
    if (region->DocumentIndexWrapper()->IsBuildError()) {
      return butil::Status(pb::error::EDOCUMENT_INDEX_BUILD_ERROR,
                           fmt::format("Document index {} build error, please wait for recover.", region->Id()));
    }
    return butil::Status(pb::error::EDOCUMENT_INDEX_NOT_READY,
                         fmt::format("Document index {} not ready, please retry.", region->Id()));
  }

  auto status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnResolveLock(StoragePtr storage, google::protobuf::RpcController* controller,
                      const pb::store::TxnResolveLockRequest* request, pb::store::TxnResolveLockResponse* response,
                      TrackClosure* done, bool is_sync);

void DocumentServiceImpl::TxnResolveLock(google::protobuf::RpcController* controller,
                                         const pb::store::TxnResolveLockRequest* request,
                                         pb::store::TxnResolveLockResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  if (IsBackgroundPendingTaskCountExceed()) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "Background pending task count is full, please wait and retry");
    return;
  }

  auto region = svr_done->GetRegion();
  int64_t region_id = request->context().region_id();

  auto status = DocumentValidateTxnResolveLockRequest(request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTxnResolveLock(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateTxnBatchGetRequest(const pb::store::TxnBatchGetRequest* request, store::RegionPtr region) {
  // check if region_epoch is match
  auto epoch_ret = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!epoch_ret.ok()) {
    return epoch_ret;
  }

  if (request->keys_size() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Keys is empty");
  }

  if (request->start_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_ts is 0");
  }

  std::vector<std::string_view> keys;
  for (const auto& key : request->keys()) {
    if (key.empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
    }
    keys.push_back(key);
  }
  auto status = ServiceHelper::ValidateRegion(region, keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnBatchGetDocument(StoragePtr storage, google::protobuf::RpcController* controller,
                           const pb::store::TxnBatchGetRequest* request, pb::store::TxnBatchGetResponse* response,
                           TrackClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();
  int64_t region_id = request->context().region_id();
  region->SetTxnAccessMaxTs(request->start_ts());
  butil::Status status = ValidateTxnBatchGetRequest(request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  std::vector<std::string> keys;
  for (const auto& key : request->keys()) {
    keys.emplace_back(key);
  }

  std::set<int64_t> resolved_locks;
  for (const auto& lock : request->context().resolved_locks()) {
    resolved_locks.insert(lock);
  }

  pb::store::TxnResultInfo txn_result_info;

  // read key check
  if (request->context().isolation_level() == pb::store::IsolationLevel::SnapshotIsolation &&
      region->CheckKeys(keys, request->context().isolation_level(), request->start_ts(), resolved_locks,
                        txn_result_info)) {
    ServiceHelper::SetError(response->mutable_error(), pb::error::Errno::ETXN_MEMORY_LOCK_CONFLICT,
                            fmt::format("Meet memory lock, please try later"));
    *response->mutable_txn_result() = txn_result_info;
    return;
  }

  auto ctx = std::make_shared<Context>();
  ctx->SetRegionId(request->context().region_id());
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  std::vector<pb::common::KeyValue> kvs;
  status = storage->TxnBatchGet(ctx, request->start_ts(), keys, resolved_locks, txn_result_info, kvs);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

    return;
  }

  if (!kvs.empty()) {
    for (auto& kv : kvs) {
      pb::common::DocumentWithId document_with_id;

      if (!kv.value().empty()) {
        auto parse_ret = document_with_id.ParseFromString(kv.value());
        if (!parse_ret) {
          auto* err = response->mutable_error();
          err->set_errcode(static_cast<Errno>(pb::error::EINTERNAL));
          err->set_errmsg("parse document_with_id failed");
          return;
        }
      }

      response->add_documents()->Swap(&document_with_id);
    }
  }
  *response->mutable_txn_result() = txn_result_info;
}

void DocumentServiceImpl::TxnBatchGet(google::protobuf::RpcController* controller,
                                      const pb::store::TxnBatchGetRequest* request,
                                      pb::store::TxnBatchGetResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTxnBatchGetDocument(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateTxnBatchRollbackRequest(const pb::store::TxnBatchRollbackRequest* request,
                                                     store::RegionPtr region) {
  // check if region_epoch is match
  auto epoch_ret = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!epoch_ret.ok()) {
    return epoch_ret;
  }

  if (request->keys_size() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Keys is empty");
  }

  if (request->start_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_ts is 0");
  }

  std::vector<std::string_view> keys;
  for (const auto& key : request->keys()) {
    if (key.empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
    }
    keys.push_back(key);
  }
  auto status = ServiceHelper::ValidateRegion(region, keys);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnBatchRollback(StoragePtr storage, google::protobuf::RpcController* controller,
                        const pb::store::TxnBatchRollbackRequest* request,
                        pb::store::TxnBatchRollbackResponse* response, TrackClosure* done, bool is_sync);

void DocumentServiceImpl::TxnBatchRollback(google::protobuf::RpcController* controller,
                                           const pb::store::TxnBatchRollbackRequest* request,
                                           pb::store::TxnBatchRollbackResponse* response,
                                           google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTxnBatchRollback(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateTxnScanLockRequest(const pb::store::TxnScanLockRequest* request, store::RegionPtr region) {
  // check if region_epoch is match
  auto epoch_ret = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!epoch_ret.ok()) {
    return epoch_ret;
  }

  if (request->max_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "max_ts is 0");
  }

  if (request->limit() <= 0 && request->stream_meta().limit() <= 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "param limit is invalid");
  }
  if (request->limit() > FLAGS_stream_message_max_limit_size ||
      request->stream_meta().limit() > FLAGS_stream_message_max_limit_size) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "param limit beyond max limit");
  }

  if (request->start_key().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_key is empty");
  }

  if (request->end_key().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "end_key is empty");
  }

  if (request->start_key() >= request->end_key()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_key >= end_key");
  }

  std::vector<std::string_view> keys;
  keys.push_back(request->start_key());
  keys.push_back(request->end_key());

  auto status = ServiceHelper::ValidateRegion(region, keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnScanLock(StoragePtr storage, google::protobuf::RpcController* controller,
                   const pb::store::TxnScanLockRequest* request, pb::store::TxnScanLockResponse* response,
                   TrackClosure* done);

void DocumentServiceImpl::TxnScanLock(google::protobuf::RpcController* controller,
                                      const pb::store::TxnScanLockRequest* request,
                                      pb::store::TxnScanLockResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTxnScanLock(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateTxnHeartBeatRequest(const pb::store::TxnHeartBeatRequest* request,
                                                 store::RegionPtr region) {
  // check if region_epoch is match
  auto epoch_ret = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!epoch_ret.ok()) {
    return epoch_ret;
  }

  if (request->primary_lock().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "primary_lock is empty");
  }

  if (request->start_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_ts is 0");
  }

  if (request->advise_lock_ttl() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "advise_lock_ttl is 0");
  }

  std::vector<std::string_view> keys;
  keys.push_back(request->primary_lock());

  auto status = ServiceHelper::ValidateRegion(region, keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void DoTxnHeartBeat(StoragePtr storage, google::protobuf::RpcController* controller,
                    const pb::store::TxnHeartBeatRequest* request, pb::store::TxnHeartBeatResponse* response,
                    TrackClosure* done, bool is_sync);

void DocumentServiceImpl::TxnHeartBeat(google::protobuf::RpcController* controller,
                                       const pb::store::TxnHeartBeatRequest* request,
                                       pb::store::TxnHeartBeatResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTxnHeartBeat(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status DocumentValidateTxnGcRequest(const pb::store::TxnGcRequest* request, store::RegionPtr region) {
  // check if region_epoch is match
  auto epoch_ret = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!epoch_ret.ok()) {
    return epoch_ret;
  }

  if (request->safe_point_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "safe_point_ts is 0");
  }

  return butil::Status();
}

void DoTxnGc(StoragePtr storage, google::protobuf::RpcController* controller, const pb::store::TxnGcRequest* request,
             pb::store::TxnGcResponse* response, TrackClosure* done, bool is_sync);

void DocumentServiceImpl::TxnGc(google::protobuf::RpcController* controller, const pb::store::TxnGcRequest* request,
                                pb::store::TxnGcResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  auto region = svr_done->GetRegion();
  int64_t region_id = request->context().region_id();

  auto status = DocumentValidateTxnGcRequest(request, region);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTxnGc(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateTxnDeleteRangeRequest(const pb::store::TxnDeleteRangeRequest* request,
                                                   store::RegionPtr region) {
  // check if region_epoch is match
  auto epoch_ret = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!epoch_ret.ok()) {
    return epoch_ret;
  }

  if (request->start_key().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_key is empty");
  }

  if (request->end_key().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "end_key is empty");
  }

  if (request->start_key() == request->end_key()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_key is equal to end_key");
  }

  if (request->start_key().compare(request->end_key()) > 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_key is greater than end_key");
  }

  return butil::Status();
}

void DoTxnDeleteRange(StoragePtr storage, google::protobuf::RpcController* controller,
                      const pb::store::TxnDeleteRangeRequest* request, pb::store::TxnDeleteRangeResponse* response,
                      TrackClosure* done, bool is_sync);

void DocumentServiceImpl::TxnDeleteRange(google::protobuf::RpcController* controller,
                                         const pb::store::TxnDeleteRangeRequest* request,
                                         pb::store::TxnDeleteRangeResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTxnDeleteRange(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}
static butil::Status ValidateBackupDataRangeRequest(const dingodb::pb::store::BackupDataRequest* request,
                                                    store::RegionPtr region) {
  // check if region_epoch is match
  auto status = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!status.ok()) {
    return status;
  }

  pb::common::Range req_range;
  req_range.set_start_key(request->start_key());
  req_range.set_end_key(request->end_key());

  status = ServiceHelper::ValidateRange(req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRangeInRange(region->Range(false), req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRegionState(region);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateClusterReadOnly();
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

static void DoBackupData(StoragePtr storage, google::protobuf::RpcController* controller,
                         const dingodb::pb::store::BackupDataRequest* request,
                         dingodb::pb::store::BackupDataResponse* response, TrackClosure* done, bool is_sync) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  auto tracker = done->Tracker();
  tracker->SetServiceQueueWaitTime();

  auto region = done->GetRegion();

  auto status = ValidateBackupDataRangeRequest(request, region);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    ServiceHelper::GetStoreRegionInfo(region, response->mutable_error());
    return;
  }

  // check leader if need
  if (request->need_leader()) {
    status = storage->ValidateLeader(region);
    if (!status.ok()) {
      ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
      return;
    }
  }

  auto ctx = std::make_shared<Context>(cntl, is_sync ? nullptr : done_guard.release(), request, response);
  ctx->SetRegionId(request->context().region_id());
  ctx->SetTracker(tracker);
  ctx->SetCfName(Constant::kStoreDataCF);
  ctx->SetRegionEpoch(request->context().region_epoch());
  ctx->SetIsolationLevel(request->context().isolation_level());
  ctx->SetRawEngineType(region->GetRawEngineType());
  ctx->SetStoreEngineType(region->GetStoreEngineType());

  status = storage->BackupData(ctx, region, request->region_type(), request->backup_ts(), request->backup_tso(),
                               request->storage_path(), request->storage_backend(), request->compression_type(),
                               request->compression_level(), response);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    if (!is_sync) done->Run();
  }
}

void DocumentServiceImpl::BackupData(google::protobuf::RpcController* controller,
                                     const dingodb::pb::store::BackupDataRequest* request,
                                     dingodb::pb::store::BackupDataResponse* response,
                                     google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoBackupData(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void DoRestoreData(StoragePtr storage, google::protobuf::RpcController* controller,
                   const dingodb::pb::store::RestoreDataRequest* request,
                   dingodb::pb::store::RestoreDataResponse* response, TrackClosure* done, bool is_sync);

void DocumentServiceImpl::RestoreData(google::protobuf::RpcController* controller,
                                      const dingodb::pb::store::RestoreDataRequest* request,
                                      dingodb::pb::store::RestoreDataResponse* response,
                                      google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }
  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoRestoreData(storage_, controller, request, response, svr_done, true);
  });
  bool ret = write_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

static butil::Status ValidateTxnDumpRequest(const pb::store::TxnDumpRequest* request, store::RegionPtr region) {
  // check if region_epoch is match
  auto epoch_ret = ServiceHelper::ValidateRegionEpoch(request->context().region_epoch(), region);
  if (!epoch_ret.ok()) {
    return epoch_ret;
  }

  if (request->start_key().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_key is empty");
  }

  if (request->end_key().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "end_key is empty");
  }

  if (request->start_key() == request->end_key()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_key is equal to end_key");
  }

  if (request->start_key().compare(request->end_key()) > 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_key is greater than end_key");
  }

  if (request->end_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "end_ts is 0");
  }

  return butil::Status();
}

void DoTxnDump(StoragePtr storage, google::protobuf::RpcController* controller,
               const pb::store::TxnDumpRequest* request, pb::store::TxnDumpResponse* response, TrackClosure* done);

void DocumentServiceImpl::TxnDump(google::protobuf::RpcController* controller, const pb::store::TxnDumpRequest* request,
                                  pb::store::TxnDumpResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  if (BAIDU_UNLIKELY(svr_done->GetRegion() == nullptr)) {
    brpc::ClosureGuard done_guard(svr_done);
    return;
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTxnDump(storage_, controller, request, response, svr_done);
  });
  bool ret = read_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void DoHello(google::protobuf::RpcController* controller, const dingodb::pb::document::HelloRequest* request,
             dingodb::pb::document::HelloResponse* response, TrackClosure* done, bool is_get_memory_info = false) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  *response->mutable_version_info() = GetVersionInfo();
  if (request->is_just_version_info() && !is_get_memory_info) {
    return;
  }

  auto raft_engine = Server::GetInstance().GetRaftStoreEngine();
  if (raft_engine == nullptr) {
    return;
  }

  auto regions = Server::GetInstance().GetAllAliveRegion();
  response->set_region_count(regions.size());

  int64_t leader_count = 0;
  for (const auto& region : regions) {
    if (raft_engine->IsLeader(region->Id())) {
      leader_count++;
    }
  }
  response->set_region_leader_count(leader_count);

  if (request->get_region_metrics() || is_get_memory_info) {
    auto store_metrics_manager = Server::GetInstance().GetStoreMetricsManager();
    if (store_metrics_manager == nullptr) {
      return;
    }

    auto store_region_metrics = store_metrics_manager->GetStoreRegionMetrics();
    if (store_region_metrics == nullptr) {
      return;
    }

    auto region_metrics = store_region_metrics->GetAllMetrics();
    for (const auto& region_metrics : region_metrics) {
      auto* new_region_metrics = response->add_region_metrics();
      *new_region_metrics = region_metrics->InnerRegionMetrics();
    }

    auto store_metrics_ptr = store_metrics_manager->GetStoreMetrics();
    if (store_metrics_ptr == nullptr) {
      return;
    }

    auto store_own_metrics = store_metrics_ptr->Metrics();
    *(response->mutable_store_own_metrics()) = store_own_metrics.store_own_metrics();
  }
}

void DocumentServiceImpl::Hello(google::protobuf::RpcController* controller, const pb::document::HelloRequest* request,
                                pb::document::HelloResponse* response, google::protobuf::Closure* done) {
  // Run in queue.
  auto* svr_done = new ServiceClosure<pb::document::HelloRequest, pb::document::HelloResponse, false>(
      __func__, done, request, response);

  auto task = std::make_shared<ServiceTask>([=]() { DoHello(controller, request, response, svr_done); });

  bool ret = read_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void DocumentServiceImpl::GetMemoryInfo(google::protobuf::RpcController* controller,
                                        const pb::document::HelloRequest* request,
                                        pb::document::HelloResponse* response, google::protobuf::Closure* done) {
  // Run in queue.
  auto* svr_done = new ServiceClosure<pb::document::HelloRequest, pb::document::HelloResponse, false>(
      __func__, done, request, response);

  auto task = std::make_shared<ServiceTask>([=]() { DoHello(controller, request, response, svr_done, true); });

  bool ret = read_worker_set_->ExecuteRR(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

}  // namespace dingodb
