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

#include "vector/vector_index_flat.h"

#include <cstddef>
#include <cstdint>
#include <future>
#include <iterator>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "butil/status.h"
#include "common/logging.h"
#include "common/synchronization.h"
#include "faiss/Index.h"
#include "faiss/IndexFlat.h"
#include "faiss/IndexIDMap.h"
#include "faiss/MetricType.h"
#include "faiss/impl/AuxIndexStructures.h"
#include "faiss/impl/IDSelector.h"
#include "faiss/index_io.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
#include "vector/vector_index_utils.h"

namespace dingodb {

DEFINE_int64(flat_need_save_count, 10000, "flat need save count");

bvar::LatencyRecorder g_flat_upsert_latency("dingo_flat_upsert_latency");
bvar::LatencyRecorder g_flat_search_latency("dingo_flat_search_latency");
bvar::LatencyRecorder g_flat_range_search_latency("dingo_flat_range_search_latency");
bvar::LatencyRecorder g_flat_delete_latency("dingo_flat_delete_latency");
bvar::LatencyRecorder g_flat_load_latency("dingo_flat_load_latency");

template std::vector<faiss::idx_t> VectorIndexFlat::GetExistVectorIds(const std::unique_ptr<faiss::idx_t[]>& ids,
                                                                      size_t size);

template std::vector<faiss::idx_t> VectorIndexFlat::GetExistVectorIds(const std::vector<faiss::idx_t>& ids,
                                                                      size_t size);

VectorIndexFlat::VectorIndexFlat(int64_t id, const pb::common::VectorIndexParameter& vector_index_parameter,
                                 const pb::common::RegionEpoch& epoch, const pb::common::Range& range,
                                 ThreadPoolPtr thread_pool)
    : VectorIndex(id, vector_index_parameter, epoch, range, thread_pool) {
  metric_type_ = vector_index_parameter.flat_parameter().metric_type();
  dimension_ = vector_index_parameter.flat_parameter().dimension();

  normalize_ = false;

  if (pb::common::MetricType::METRIC_TYPE_L2 == metric_type_) {
    raw_index_ = std::make_unique<faiss::IndexFlatL2>(dimension_);
  } else if (pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT == metric_type_) {
    raw_index_ = std::make_unique<faiss::IndexFlatIP>(dimension_);
  } else if (pb::common::MetricType::METRIC_TYPE_COSINE == metric_type_) {
    normalize_ = true;
    raw_index_ = std::make_unique<faiss::IndexFlatIP>(dimension_);
  } else {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.flat][id({})] not support metric type({}), use L2.", Id(),
                                      pb::common::MetricType_Name(metric_type_));
    raw_index_ = std::make_unique<faiss::IndexFlatL2>(dimension_);
  }

  index_id_map2_ = std::make_unique<faiss::IndexIDMap2>(raw_index_.get());
}

VectorIndexFlat::~VectorIndexFlat() { index_id_map2_->reset(); }

butil::Status VectorIndexFlat::AddOrUpsertWrapper(const std::vector<pb::common::VectorWithId>& vector_with_ids,
                                                  bool is_upsert) {
  return AddOrUpsert(vector_with_ids, is_upsert);
}

butil::Status VectorIndexFlat::AddOrUpsert(const std::vector<pb::common::VectorWithId>& vector_with_ids,
                                           bool /*is_upsert*/) {
  if (vector_with_ids.empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "vector_with_ids is empty");
  }
  auto status = VectorIndexUtils::CheckVectorDimension(vector_with_ids, dimension_);
  if (!status.ok()) {
    return status;
  }

  const auto& ids = VectorIndexUtils::ExtractVectorId(vector_with_ids);
  status = VectorIndexUtils::CheckVectorIdDuplicated(ids, vector_with_ids.size());
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }
  const auto& vector_values = VectorIndexUtils::ExtractVectorValue(vector_with_ids, dimension_, normalize_);

  BvarLatencyGuard bvar_guard(&g_flat_upsert_latency);
  RWLockWriteGuard guard(&rw_lock_);

  // delete id exists.
  if (!index_id_map2_->rev_map.empty()) {
    std::vector<faiss::idx_t> internal_ids = GetExistVectorIds(ids, vector_with_ids.size());

    if (!internal_ids.empty()) {
      faiss::IDSelectorBatch sel(internal_ids.size(), internal_ids.data());
      index_id_map2_->remove_ids(sel);
    }
  }
  index_id_map2_->add_with_ids(vector_with_ids.size(), vector_values.get(), ids.get());

  return butil::Status::OK();
}

butil::Status VectorIndexFlat::Upsert(const std::vector<pb::common::VectorWithId>& vector_with_ids) {
  return AddOrUpsertWrapper(vector_with_ids, true);
}

butil::Status VectorIndexFlat::Add(const std::vector<pb::common::VectorWithId>& vector_with_ids) {
  return AddOrUpsertWrapper(vector_with_ids, false);
}

butil::Status VectorIndexFlat::Delete(const std::vector<int64_t>& delete_ids) {
  if (delete_ids.empty()) {
    return butil::Status::OK();
  }

  const auto& ids = VectorIndexUtils::CastVectorId(delete_ids);

  {
    BvarLatencyGuard bvar_guard(&g_flat_delete_latency);
    RWLockWriteGuard guard(&rw_lock_);

    // delete id exists.
    if (!index_id_map2_->rev_map.empty()) {
      std::vector<faiss::idx_t> internal_ids = GetExistVectorIds(delete_ids, delete_ids.size());

      if (!internal_ids.empty()) {
        faiss::IDSelectorBatch sel(internal_ids.size(), internal_ids.data());
        auto remove_count = index_id_map2_->remove_ids(sel);
        if (0 == remove_count) {
          DINGO_LOG(WARNING) << fmt::format("[vector_index.flat][id({})] remove not found vector id.", Id());
          return butil::Status(pb::error::Errno::EVECTOR_INVALID, "remove not found vector id");
        }
      }
    }

    return butil::Status::OK();
  }
}

butil::Status VectorIndexFlat::Search(const std::vector<pb::common::VectorWithId>& vector_with_ids, uint32_t topk,
                                      const std::vector<std::shared_ptr<FilterFunctor>>& filters, bool,
                                      const pb::common::VectorSearchParameter&,
                                      std::vector<pb::index::VectorWithDistanceResult>& results) {
  if (vector_with_ids.empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "vector_with_ids is empty");
  }
  if (topk <= 0) return butil::Status::OK();
  auto status = VectorIndexUtils::CheckVectorDimension(vector_with_ids, dimension_);
  if (!status.ok()) {
    return status;
  }

  std::vector<faiss::Index::distance_t> distances;
  distances.resize(topk * vector_with_ids.size(), 0.0f);
  std::vector<faiss::idx_t> labels;
  labels.resize(topk * vector_with_ids.size(), -1);

  const auto& vector_values = VectorIndexUtils::ExtractVectorValue(vector_with_ids, dimension_, normalize_);

  {
    BvarLatencyGuard bvar_guard(&g_flat_search_latency);
    RWLockReadGuard guard(&rw_lock_);

    if (!filters.empty()) {
      // use faiss's search_param to do pre-filter
      auto flat_filter = filters.empty() ? nullptr : std::make_shared<FlatIDSelector>(filters);
      faiss::SearchParameters flat_search_parameters;
      flat_search_parameters.sel = flat_filter.get();
      index_id_map2_->search(vector_with_ids.size(), vector_values.get(), topk, distances.data(), labels.data(),
                             &flat_search_parameters);
    } else {
      index_id_map2_->search(vector_with_ids.size(), vector_values.get(), topk, distances.data(), labels.data());
    }
  }

  VectorIndexUtils::FillSearchResult(vector_with_ids, topk, distances, labels, metric_type_, dimension_, results);

  DINGO_LOG(DEBUG) << fmt::format("[vector_index.flat][id({})] result size {}", Id(), results.size());

  return butil::Status::OK();
}

butil::Status VectorIndexFlat::RangeSearch(const std::vector<pb::common::VectorWithId>& vector_with_ids, float radius,
                                           const std::vector<std::shared_ptr<VectorIndex::FilterFunctor>>& filters,
                                           bool /*reconstruct*/, const pb::common::VectorSearchParameter& /*parameter*/,
                                           std::vector<pb::index::VectorWithDistanceResult>& results) {
  if (vector_with_ids.empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "vector_with_ids is empty");
  }
  auto status = VectorIndexUtils::CheckVectorDimension(vector_with_ids, dimension_);
  if (!status.ok()) {
    return status;
  }

  const auto& vector_values = VectorIndexUtils::ExtractVectorValue(vector_with_ids, dimension_, normalize_);

  std::unique_ptr<faiss::RangeSearchResult> range_search_result =
      std::make_unique<faiss::RangeSearchResult>(vector_with_ids.size());

  if (metric_type_ == pb::common::MetricType::METRIC_TYPE_COSINE ||
      metric_type_ == pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT) {
    radius = 1.0F - radius;
  }

  {
    BvarLatencyGuard bvar_guard(&g_flat_range_search_latency);
    RWLockReadGuard guard(&rw_lock_);

    try {
      std::unique_ptr<faiss::SearchParameters> params;
      std::unique_ptr<FlatIDSelector> flat_filter;
      if (!filters.empty()) {
        params = std::make_unique<faiss::SearchParameters>();
        flat_filter = std::make_unique<FlatIDSelector>(filters);
        params->sel = flat_filter.get();
      }
      index_id_map2_->range_search(vector_with_ids.size(), vector_values.get(), radius, range_search_result.get(),
                                   params.get());
    } catch (std::exception& e) {
      return butil::Status(pb::error::Errno::EINTERNAL, fmt::format("range search exception, {}", e.what()));
    }
  }

  VectorIndexUtils::FillRangeSearchResult(range_search_result, metric_type_, dimension_, results);

  DINGO_LOG(DEBUG) << fmt::format("[vector_index.flat][id({})] result size {}", Id(), results.size());

  return butil::Status::OK();
}

void VectorIndexFlat::LockWrite() { rw_lock_.LockWrite(); }

void VectorIndexFlat::UnlockWrite() { rw_lock_.UnlockWrite(); }

bool VectorIndexFlat::SupportSave() { return true; }

butil::Status VectorIndexFlat::Save(const std::string& path) {
  // Warning : read me first !!!!
  // Currently, the save function is executed in the fork child process.
  // When calling glog,
  // the child process will hang.
  // Remove glog temporarily.
  if (path.empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "path is empty");
  }

  // The outside has been locked. Remove the locking operation here.
  try {
    faiss::write_index(index_id_map2_.get(), path.c_str());
  } catch (std::exception& e) {
    return butil::Status(pb::error::Errno::EINTERNAL, fmt::format("write index exception: {}", e.what()));
  }

  return butil::Status();
}

butil::Status VectorIndexFlat::Load(const std::string& path) {
  if (path.empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "path is empty");
  }

  BvarLatencyGuard bvar_guard(&g_flat_load_latency);

  // The outside has been locked. Remove the locking operation here.
  faiss::Index* internal_raw_index = nullptr;
  try {
    internal_raw_index = faiss::read_index(path.c_str(), 0);
  } catch (std::exception& e) {
    delete internal_raw_index;
    return butil::Status(pb::error::Errno::EINTERNAL, fmt::format("read index exception: {} {}", path, e.what()));
  }

  faiss::IndexIDMap2* internal_index = dynamic_cast<faiss::IndexIDMap2*>(internal_raw_index);
  if (BAIDU_UNLIKELY(!internal_index)) {
    if (internal_raw_index) {
      delete internal_raw_index;
      internal_raw_index = nullptr;
    }
    return butil::Status(pb::error::Errno::EINTERNAL, fmt::format("type cast failed"));
  }

  // avoid mem leak!!!
  std::unique_ptr<faiss::IndexIDMap2> internal_index_id_map2(internal_index);

  // double check
  if (BAIDU_UNLIKELY(internal_index->d != dimension_)) {
    return butil::Status(pb::error::Errno::EINTERNAL,
                         fmt::format("dimension not match, {} {}", internal_index->d, dimension_));
  }

  if (BAIDU_UNLIKELY(!internal_index->is_trained)) {
    return butil::Status(pb::error::Errno::EINTERNAL, "already trained");
  }

  switch (metric_type_) {
    case pb::common::METRIC_TYPE_NONE:
      [[fallthrough]];
    case pb::common::METRIC_TYPE_L2: {
      if (BAIDU_UNLIKELY(internal_index->metric_type != faiss::MetricType::METRIC_L2)) {
        std::string s = fmt::format("metric type not match, {} {}", static_cast<int>(internal_index->metric_type),
                                    pb::common::MetricType_Name(metric_type_));
        return butil::Status(pb::error::Errno::EINTERNAL, s);
      }
      break;
    }

    case pb::common::METRIC_TYPE_INNER_PRODUCT:
    case pb::common::METRIC_TYPE_COSINE: {
      if (BAIDU_UNLIKELY(internal_index->metric_type != faiss::MetricType::METRIC_INNER_PRODUCT)) {
        std::string s = fmt::format("metric type not match, {} {}", static_cast<int>(internal_index->metric_type),
                                    pb::common::MetricType_Name(metric_type_));
        return butil::Status(pb::error::Errno::EINTERNAL, s);
      }
      break;
    }
    case pb::common::MetricType_INT_MIN_SENTINEL_DO_NOT_USE_:
      [[fallthrough]];
    case pb::common::MetricType_INT_MAX_SENTINEL_DO_NOT_USE_:
      [[fallthrough]];
    default: {
      std::string s = fmt::format("metric type not match, {} {}", static_cast<int>(internal_index->metric_type),
                                  pb::common::MetricType_Name(metric_type_));
      return butil::Status(pb::error::Errno::EINTERNAL, s);
    }
  }

  raw_index_.reset();
  index_id_map2_ = std::move(internal_index_id_map2);

  if (pb::common::MetricType::METRIC_TYPE_COSINE == metric_type_) {
    normalize_ = true;
  }

  DINGO_LOG(INFO) << fmt::format("[vector_index.flat][id({})] load finsh, path: {}", Id(), path);

  return butil::Status::OK();
}

int32_t VectorIndexFlat::GetDimension() { return this->dimension_; }

pb::common::MetricType VectorIndexFlat::GetMetricType() { return this->metric_type_; }

butil::Status VectorIndexFlat::GetCount(int64_t& count) {
  RWLockReadGuard guard(&rw_lock_);
  count = index_id_map2_->id_map.size();
  return butil::Status::OK();
}

butil::Status VectorIndexFlat::GetDeletedCount(int64_t& deleted_count) {
  deleted_count = 0;
  return butil::Status::OK();
}

butil::Status VectorIndexFlat::GetMemorySize(int64_t& memory_size) {
  RWLockReadGuard guard(&rw_lock_);
  auto count = index_id_map2_->ntotal;
  if (count == 0) {
    memory_size = 0;
    return butil::Status::OK();
  }

  memory_size = count * sizeof(faiss::idx_t) + count * dimension_ * sizeof(faiss::Index::component_t) +
                (sizeof(faiss::idx_t) + sizeof(faiss::idx_t)) * index_id_map2_->rev_map.size();
  return butil::Status::OK();
}

bool VectorIndexFlat::IsExceedsMaxElements() { return false; }

bool VectorIndexFlat::NeedToSave(int64_t last_save_log_behind) {
  RWLockReadGuard guard(&rw_lock_);

  int64_t element_count = 0;

  element_count = index_id_map2_->id_map.size();

  if (element_count == 0) {
    return false;
  }

  if (last_save_log_behind > FLAGS_flat_need_save_count) {
    return true;
  }

  return false;
}

template <typename T>
std::vector<faiss::idx_t> VectorIndexFlat::GetExistVectorIds(const T& ids, size_t size) {
  std::vector<faiss::idx_t> internal_ids;
  internal_ids.reserve(size);
  for (int i = 0; i < size; i++) {
    if (0 != index_id_map2_->rev_map.count(ids[i])) {
      internal_ids.push_back(ids[i]);
    }
  }
  return internal_ids;
}

}  // namespace dingodb
