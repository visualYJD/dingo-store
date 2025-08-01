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

#ifndef DINGODB_STORE_META_MANAGER_H_
#define DINGODB_STORE_META_MANAGER_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "braft/file_system_adaptor.h"
#include "bthread/types.h"
#include "butil/endpoint.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/latch.h"
#include "common/safe_map.h"
#include "document/document_index.h"
#include "engine/concurrency_manager.h"
#include "engine/gc_safe_point.h"
#include "meta/meta_reader.h"
#include "meta/meta_writer.h"
#include "meta/transform_kv_able.h"
#include "proto/common.pb.h"
#include "proto/node.pb.h"
#include "proto/raft.pb.h"
#include "proto/store_internal.pb.h"

namespace dingodb {

class VectorIndexWrapper;
using VectorIndexWrapperPtr = std::shared_ptr<VectorIndexWrapper>;

namespace store {

class Region;
using RegionPtr = std::shared_ptr<Region>;

// Warp pb region for atomic/metux
class Region {
 public:
  struct Statistics {
    std::atomic<int32_t> serving_request_count{0};
    std::atomic<int64_t> last_serving_time_s{0};
  };

  Region(int64_t region_id);
  ~Region();

  Region(const Region&) = delete;
  void operator=(const Region&) = delete;

  static RegionPtr New(int64_t region_id);
  static RegionPtr New(const pb::common::RegionDefinition& definition);

  bool Recover();

  std::string Serialize();
  void DeSerialize(const std::string& data);

  int64_t Id() const { return inner_region_.id(); }
  const std::string& Name() const { return inner_region_.definition().name(); }
  pb::common::RegionType Type() { return inner_region_.region_type(); }

  pb::common::RawEngine GetRawEngineType();
  pb::common::StorageEngine GetStoreEngineType();

  bool IsTxn();
  bool IsExecutorTxn();
  bool IsClientTxn();

  pb::common::RegionEpoch Epoch(bool lock = true);
  std::string EpochToString();
  void SetEpochVersionAndRange(int64_t version, const pb::common::Range& range);
  void GetEpochAndRange(pb::common::RegionEpoch& epoch, pb::common::Range& range);

  void SetEpochConfVersion(int64_t version);
  void SetSnapshotEpochVersion(int64_t version);

  void LockRegionMeta();
  void UnlockRegionMeta();

  int64_t LeaderId();
  void SetLeaderId(int64_t leader_id);

  // encode user key range
  pb::common::Range Range(bool is_encode = false, bool lock = true);
  std::string RangeToString(bool is_encode = false);
  bool CheckKeyInRange(const std::string& key);

  char GetKeyPrefix();

  void SetIndexParameter(const pb::common::IndexParameter& index_parameter);

  std::vector<pb::common::Peer> Peers();
  void SetPeers(std::vector<pb::common::Peer>& peers);

  pb::common::StoreRegionState State() const;
  void SetState(pb::common::StoreRegionState state);
  void AppendHistoryState(pb::common::StoreRegionState state);

  bool NeedBootstrapDoSnapshot();
  void SetNeedBootstrapDoSnapshot(bool need_do_snapshot);

  bool IsSupportSplitAndMerge();

  bool DisableChange();
  void SetDisableChange(bool disable_change);

  bool TemporaryDisableChange();
  void SetTemporaryDisableChange(bool disable_change);

  pb::raft::SplitStrategy SplitStrategy();
  void SetSplitStrategy(pb::raft::SplitStrategy split_strategy);

  int64_t LastSplitTimestamp();
  void UpdateLastSplitTimestamp();

  int64_t ParentId();
  void SetParentId(int64_t region_id);

  int64_t PartitionId();

  int64_t SnapshotEpochVersion();

  pb::store_internal::Region InnerRegion();
  pb::common::RegionDefinition Definition();

  VectorIndexWrapperPtr VectorIndexWrapper() { return vector_index_wapper_; }
  void SetVectorIndexWrapper(VectorIndexWrapperPtr vector_index_wapper) { vector_index_wapper_ = vector_index_wapper; }

  DocumentIndexWrapperPtr DocumentIndexWrapper() { return document_index_wapper_; }
  void SetDocumentIndexWrapper(DocumentIndexWrapperPtr document_index_wapper) {
    document_index_wapper_ = document_index_wapper;
  }

  scoped_refptr<braft::FileSystemAdaptor> snapshot_adaptor = nullptr;

  void SetLastChangeJobId(int64_t job_id);
  int64_t LastChangeJobId();

  bool LatchesAcquire(Lock* lock, uint64_t who);

  void LatchesRelease(Lock* lock, uint64_t who,
                      std::optional<std::pair<uint64_t, Lock*>> keep_latches_for_next_cmd = std::nullopt);

  pb::common::ScalarSchema ScalarSchema();

  int32_t GetServingRequestCount() { return statistics_.serving_request_count.load(std::memory_order_relaxed); }
  void IncServingRequestCount() { statistics_.serving_request_count.fetch_add(1, std::memory_order_relaxed); }
  void DecServingRequestCount() { statistics_.serving_request_count.fetch_sub(1, std::memory_order_relaxed); }

  int64_t GetLastServingTime() const { return statistics_.last_serving_time_s.load(std::memory_order_relaxed); }
  void UpdateLastServingTime() {
    statistics_.last_serving_time_s.store(Helper::Timestamp(), std::memory_order_relaxed);
  }

  void SetRawAppliedMaxTs(int64_t ts) {
    if (ts > raw_applied_max_ts_.load(std::memory_order_acquire)) {
      raw_applied_max_ts_.store(ts, std::memory_order_release);
    }
  }
  int64_t RawAppliedMaxTs() { return raw_applied_max_ts_.load(std::memory_order_acquire); }

  void SetTxnAccessMaxTs(int64_t ts) {
    if (ts > txn_access_max_ts_.load(std::memory_order_acquire)) {
      txn_access_max_ts_.store(ts, std::memory_order_release);
    }
  }
  int64_t TxnAccessMaxTs() { return txn_access_max_ts_.load(std::memory_order_acquire); }

  // memory_lock_manager
  void LockKey(const std::string& key, ConcurrencyManager::LockEntryPtr lock_entry);
  void UnlockKeys(const std::vector<std::string>& keys);

  bool CheckKeys(const std::vector<std::string>& keys, pb::store::IsolationLevel isolation_level, int64_t start_ts,
                 const std::set<int64_t>& resolved_locks, pb::store::TxnResultInfo& txn_result_info);
  bool CheckRange(const std::string& start_key, const std::string& end_key, pb::store::IsolationLevel isolation_level,
                  int64_t start_ts, const std::set<int64_t>& resolved_locks, pb::store::TxnResultInfo& txn_result_info);

 private:
  bthread_mutex_t mutex_;
  pb::store_internal::Region inner_region_;
  std::atomic<pb::common::StoreRegionState> state_;

  std::atomic<int64_t> raw_applied_max_ts_{0};

  std::atomic<int64_t> txn_access_max_ts_{0};

  pb::raft::SplitStrategy split_strategy_{};

  VectorIndexWrapperPtr vector_index_wapper_{nullptr};
  DocumentIndexWrapperPtr document_index_wapper_{nullptr};

  // latches is for multi request concurrency control
  Latches latches_;

  Statistics statistics_;
  ConcurrencyManager concurrency_manager_;
};

class RaftMeta {
 public:
  RaftMeta(int64_t region_id);
  ~RaftMeta();

  static std::shared_ptr<RaftMeta> New(int64_t region_id);

  int64_t RegionId();
  int64_t Term();
  int64_t AppliedId();
  void SetTermAndAppliedId(int64_t term, int64_t applied_id);

  std::string Serialize();
  void DeSerialize(const std::string& data);

  pb::store_internal::RaftMeta InnerRaftMeta();

 private:
  bthread_mutex_t mutex_;
  pb::store_internal::RaftMeta raft_meta_;
};

using RaftMetaPtr = std::shared_ptr<RaftMeta>;

}  // namespace store

class RegionChangeRecorder : public TransformKvAble {
 public:
  RegionChangeRecorder(MetaReaderPtr meta_reader, MetaWriterPtr meta_writer);
  ~RegionChangeRecorder() override;

  bool Init();

  void AddChangeRecord(const pb::coordinator::RegionCmd& cmd);
  void AddChangeRecord(const pb::raft::SplitRequest& request);
  void AddChangeRecord(const pb::raft::PrepareMergeRequest& request, int64_t source_id);
  void AddChangeRecord(const pb::raft::CommitMergeRequest& request, int64_t target_id);

  void AddChangeRecordTimePoint(int64_t job_id, const std::string& event);

  pb::store_internal::RegionChangeRecord ChangeRecord(int64_t job_id);

  std::vector<pb::store_internal::RegionChangeRecord> GetChangeRecord(int64_t region_id);
  std::vector<pb::store_internal::RegionChangeRecord> GetAllChangeRecord();

 private:
  std::shared_ptr<pb::common::KeyValue> TransformToKv(std::any obj) override;
  void TransformFromKv(const std::vector<pb::common::KeyValue>& kvs) override;

  bool IsExist(int64_t job_id);
  void Upsert(const pb::store_internal::RegionChangeRecord& record, const std::string& event);

  void Save(const pb::store_internal::RegionChangeRecord& record);

  // key: job_id
  std::unordered_map<int64_t, pb::store_internal::RegionChangeRecord> records_;
  bthread_mutex_t mutex_;

  // Read meta data from persistence storage.
  MetaReaderPtr meta_reader_;
  // Write meta data to persistence storage.
  MetaWriterPtr meta_writer_;
};

// Manage store server store data
class StoreServerMeta {
 public:
  StoreServerMeta() { bthread_mutex_init(&mutex_, nullptr); }
  ~StoreServerMeta() { bthread_mutex_destroy(&mutex_); }

  StoreServerMeta(const StoreServerMeta&) = delete;
  const StoreServerMeta& operator=(const StoreServerMeta&) = delete;

  bool Init();

  int64_t GetEpoch() const;
  StoreServerMeta& SetEpoch(int64_t epoch);

  bool IsExist(int64_t store_id);

  void AddStore(std::shared_ptr<pb::common::Store> store);
  void UpdateStore(std::shared_ptr<pb::common::Store> store);
  void DeleteStore(int64_t store_id);
  std::shared_ptr<pb::common::Store> GetStore(int64_t store_id);
  std::map<int64_t, std::shared_ptr<pb::common::Store>> GetAllStore();

  pb::node::NodeInfo GetNodeInfoByRaftEndPoint(const butil::EndPoint& endpoint);
  pb::node::NodeInfo GetNodeInfoByServerEndPoint(const butil::EndPoint& endpoint);

 private:
  int64_t epoch_;
  bthread_mutex_t mutex_;
  std::map<int64_t, std::shared_ptr<pb::common::Store>> stores_;
};

// Manage store server region meta data
class StoreRegionMeta : public TransformKvAble {
 public:
  StoreRegionMeta(std::shared_ptr<MetaReader> meta_reader, std::shared_ptr<MetaWriter> meta_writer)
      : TransformKvAble(Constant::kStoreRegionMetaPrefix), meta_reader_(meta_reader), meta_writer_(meta_writer) {
    regions_.Init(Constant::kStoreRegionMetaInitCapacity);
  }
  ~StoreRegionMeta() override = default;

  StoreRegionMeta(const StoreRegionMeta&) = delete;
  void operator=(const StoreRegionMeta&) = delete;

  bool Init();

  static int64_t GetEpoch();

  void AddRegion(store::RegionPtr region);
  void DeleteRegion(int64_t region_id);
  void UpdateRegion(store::RegionPtr region);

  void UpdateState(store::RegionPtr region, pb::common::StoreRegionState new_state);
  void UpdateState(int64_t region_id, pb::common::StoreRegionState new_state);

  static void UpdateLeaderId(store::RegionPtr region, int64_t leader_id);
  void UpdateLeaderId(int64_t region_id, int64_t leader_id);

  void UpdatePeers(store::RegionPtr region, std::vector<pb::common::Peer>& peers);
  void UpdatePeers(int64_t region_id, std::vector<pb::common::Peer>& peers);

  void UpdateEpochVersionAndRange(store::RegionPtr region, int64_t version, const pb::common::Range& range,
                                  const std::string& trace);
  void UpdateEpochVersionAndRange(int64_t region_id, int64_t version, const pb::common::Range& range,
                                  const std::string& trace);
  void UpdateEpochConfVersion(store::RegionPtr region, int64_t version);
  void UpdateEpochConfVersion(int64_t region_id, int64_t version);
  void UpdateSnapshotEpochVersion(store::RegionPtr region, int64_t version, const std::string& trace);

  void UpdateNeedBootstrapDoSnapshot(store::RegionPtr region, bool need_do_snapshot);
  void UpdateDisableChange(store::RegionPtr region, bool disable_change);
  void UpdateTemporaryDisableChange(store::RegionPtr region, bool disable_change);

  void UpdateLastChangeJobId(store::RegionPtr region, int64_t job_id);

  bool IsExistRegion(int64_t region_id);
  store::RegionPtr GetRegion(int64_t region_id);
  std::vector<store::RegionPtr> GetAllRegion();
  std::vector<store::RegionPtr> GetAllAliveRegion();
  std::vector<store::RegionPtr> GetAllMetricsRegion();

 private:
  std::shared_ptr<pb::common::KeyValue> TransformToKv(std::any obj) override;
  void TransformFromKv(const std::vector<pb::common::KeyValue>& kvs) override;

  // Read meta data from persistence storage.
  std::shared_ptr<MetaReader> meta_reader_;
  // Write meta data to persistence storage.
  std::shared_ptr<MetaWriter> meta_writer_;

  // Store all region meta data in this server.
  using RegionMap = DingoSafeMap<int64_t, store::RegionPtr>;
  RegionMap regions_;
};

class StoreRaftMeta : public TransformKvAble {
 public:
  StoreRaftMeta(std::shared_ptr<MetaReader> meta_reader, std::shared_ptr<MetaWriter> meta_writer)
      : TransformKvAble(Constant::kStoreRaftMetaPrefix), meta_reader_(meta_reader), meta_writer_(meta_writer) {
    bthread_mutex_init(&mutex_, nullptr);
  }
  ~StoreRaftMeta() override { bthread_mutex_destroy(&mutex_); }

  StoreRaftMeta(const StoreRaftMeta&) = delete;
  void operator=(const StoreRaftMeta&) = delete;

  bool Init();

  void AddRaftMeta(store::RaftMetaPtr raft_meta);
  void UpdateRaftMeta(store::RaftMetaPtr raft_meta);
  void SaveRaftMeta(int64_t region_id);
  void DeleteRaftMeta(int64_t region_id);
  store::RaftMetaPtr GetRaftMeta(int64_t region_id);
  std::vector<store::RaftMetaPtr> GetAllRaftMeta();

 private:
  std::shared_ptr<pb::common::KeyValue> TransformToKv(std::any obj) override;
  void TransformFromKv(const std::vector<pb::common::KeyValue>& kvs) override;

  // Read meta data from persistence storage.
  std::shared_ptr<MetaReader> meta_reader_;
  // Write meta data to persistence storage.
  std::shared_ptr<MetaWriter> meta_writer_;

  bthread_mutex_t mutex_;

  using RaftMetaMap = std::map<int64_t, store::RaftMetaPtr>;
  RaftMetaMap raft_metas_;
};

// Manage store server meta data, like store and region.
// the data will report periodic.
class StoreMetaManager {
 public:
  StoreMetaManager(std::shared_ptr<MetaReader> meta_reader, std::shared_ptr<MetaWriter> meta_writer)
      : server_meta_(std::make_shared<StoreServerMeta>()),
        region_meta_(std::make_shared<StoreRegionMeta>(meta_reader, meta_writer)),
        raft_meta_(std::make_shared<StoreRaftMeta>(meta_reader, meta_writer)),
        region_change_recorder_(std::make_shared<RegionChangeRecorder>(meta_reader, meta_writer)),
        gc_safe_point_manager_(std::make_shared<GCSafePointManager>()) {}
  ~StoreMetaManager() = default;

  StoreMetaManager(const StoreMetaManager&) = delete;
  void operator=(const StoreMetaManager&) = delete;

  bool Init();

  std::shared_ptr<StoreServerMeta> GetStoreServerMeta();
  std::shared_ptr<StoreRegionMeta> GetStoreRegionMeta();
  std::shared_ptr<StoreRaftMeta> GetStoreRaftMeta();
  std::shared_ptr<RegionChangeRecorder> GetRegionChangeRecorder();

  // get gc meta ptr
  std::shared_ptr<GCSafePointManager> GetGCSafePointManager();

 private:
  // Store server meta data, like id/state/endpoint etc.
  std::shared_ptr<StoreServerMeta> server_meta_;
  // Store manage region meta data.
  std::shared_ptr<StoreRegionMeta> region_meta_;
  // Store raft meta.
  std::shared_ptr<StoreRaftMeta> raft_meta_;
  // Region change recorder
  std::shared_ptr<RegionChangeRecorder> region_change_recorder_;

  // gc meta
  std::shared_ptr<GCSafePointManager> gc_safe_point_manager_;
};

}  // namespace dingodb

#endif  // DINGODB_STORE_META_MANAGER_H_