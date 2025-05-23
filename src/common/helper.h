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

#ifndef DINGODB_COMMON_HELPER_H_
#define DINGODB_COMMON_HELPER_H_

#include <any>
#include <cstdint>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "braft/configuration.h"
#include "butil/endpoint.h"
#include "butil/status.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/node.pb.h"
#include "proto/store_internal.pb.h"

namespace dingodb {

class Helper {
  using Errno = pb::error::Errno;
  using PbError = pb::error::Error;

 public:
  static int GetCoreNum();
  static bool IsIp(const std::string& s);
  static bool IsIpInAddr(const std::string& addr);

  static bool IsExecutorRaw(const std::string& key);
  static bool IsExecutorTxn(const std::string& key);
  static bool IsClientRaw(const std::string& key);
  static bool IsClientTxn(const std::string& key);

  static char GetKeyPrefix(const std::string& key);
  static char GetKeyPrefix(const pb::common::Range& range);

  // static butil::EndPoint GetEndPoint(const std::string& host, int port);
  // static butil::EndPoint GetEndPoint(const std::string& addr);

  static std::string Ip2HostName(const std::string& ip);

  // string(ip:port/ip+port)/braft::PeerId/butil::EndPoint/pb::common::Location/pb::common::Peer transform function
  static pb::common::Location StringToLocation(const std::string& addr);
  static pb::common::Location StringToLocation(const std::string& ip_or_host, int port, int index = 0);
  static std::string LocationToString(const pb::common::Location& location);
  static std::string LocationsToString(const std::vector<pb::common::Location>& locations);

  static butil::EndPoint StringToEndPoint(const std::string& addr);
  static butil::EndPoint StringToEndPoint(const std::string& ip_or_host, int port);
  static std::vector<butil::EndPoint> StringToEndpoints(const std::string& addrs);
  static std::string EndPointToString(const butil::EndPoint& endpoint);

  static braft::PeerId StringToPeerId(const std::string& addr);
  static braft::PeerId StringToPeerId(const std::string& ip_or_host, int port);
  static pb::common::Location PeerIdToLocation(const braft::PeerId& peer_id);
  static std::string PeerIdsToString(const std::vector<braft::PeerId>& peer_ids);

  static butil::EndPoint LocationToEndPoint(const pb::common::Location& location);
  static pb::common::Location EndPointToLocation(const butil::EndPoint& endpoint);

  static braft::PeerId LocationToPeerId(const pb::common::Location& location);

  static bool IsDifferenceEndPoint(const butil::EndPoint& location, const butil::EndPoint& other_location);
  static bool IsDifferenceLocation(const pb::common::Location& location, const pb::common::Location& other_location);
  static bool IsDifferencePeers(const std::vector<pb::common::Peer>& peers,
                                const std::vector<pb::common::Peer>& other_peers);
  static bool IsDifferencePeers(const pb::common::RegionDefinition& src_definition,
                                const pb::common::RegionDefinition& dst_definition);

  static void SortPeers(std::vector<pb::common::Peer>& peers);

  static std::vector<pb::common::Location> ExtractRaftLocations(
      const google::protobuf::RepeatedPtrField<pb::common::Peer>& peers);
  static std::vector<pb::common::Location> ExtractRaftLocations(const std::vector<pb::common::Peer>& peers);
  static std::vector<braft::PeerId> ExtractPeerIds(const braft::Configuration& conf);

  static std::shared_ptr<PbError> Error(Errno errcode, const std::string& errmsg);
  static bool Error(Errno errcode, const std::string& errmsg, PbError& err);
  static bool Error(Errno errcode, const std::string& errmsg, std::shared_ptr<PbError> err);

  static bool IsEqualIgnoreCase(const std::string& str1, const std::string& str2);

  // protobuf transform
  template <typename T>
  static std::vector<T> PbRepeatedToVector(const google::protobuf::RepeatedPtrField<T>& data) {
    std::vector<T> vec;
    vec.reserve(data.size());
    for (auto& item : data) {
      vec.emplace_back(std::move(item));
    }

    return vec;
  }

  template <typename T>
  static std::vector<T> PbRepeatedToVector(google::protobuf::RepeatedPtrField<T>* data) {
    std::vector<T> vec;
    vec.reserve(data->size());
    for (auto& item : *data) {
      vec.emplace_back(std::move(item));
    }

    return vec;
  }

  template <typename T>
  static std::vector<T> PbRepeatedToVector(const google::protobuf::RepeatedField<T>& data) {
    std::vector<T> vec;
    vec.reserve(data.size());
    for (auto& item : data) {
      vec.push_back(item);
    }

    return vec;
  }

  template <typename T>
  static std::vector<T> PbRepeatedToVector(google::protobuf::RepeatedField<T>* data) {
    std::vector<T> vec;
    vec.reserve(data->size());
    for (auto& item : *data) {
      vec.push_back(item);
    }

    return vec;
  }

  template <typename T>
  static void VectorToPbRepeated(const std::vector<T>& vec, google::protobuf::RepeatedPtrField<T>* out) {
    for (auto& item : vec) {
      *(out->Add()) = item;
    }
  }

  template <typename T>
  static void VectorToPbRepeated(const std::vector<T>& vec, google::protobuf::RepeatedField<T>* out) {
    for (auto& item : vec) {
      out->Add(item);
    }
  }

  template <typename T>
  static std::string VectorToString(const std::vector<T>& vec) {
    std::string str;
    for (int i = 0; i < vec.size(); ++i) {
      str += fmt::format("{}", vec[i]);
      if (i + 1 < vec.size()) {
        str += ",";
      }
    }
    return str;
  }

  static std::string PrefixNext(const std::string& input);
  static std::string PrefixNext(const std::string_view& input);

  // generate min and max start key of dingo-store
  // partition_id cannot be 0 and INT64_MAX
  static std::string GenMaxStartKey();
  static std::string GenMinStartKey();

  // Transform RangeWithOptions to Range for scan/deleteRange
  static pb::common::Range TransformRangeWithOptions(const pb::common::RangeWithOptions& range_with_options);

  // Take range intersection
  static pb::common::Range IntersectRange(const pb::common::Range& range1, const pb::common::Range& range2);

  static std::string RangeToString(const pb::common::Range& range);

  // range1 contain range2
  static bool IsContainRange(const pb::common::Range& range1, const pb::common::Range& range2);

  // range1 and range2 has intersection
  static bool IsConflictRange(const pb::common::Range& range1, const pb::common::Range& range2);

  static bool InvalidRange(const pb::common::Range& range);

  static butil::Status CheckRange(const pb::common::Range& range);

  static std::string StringToHex(const std::string& str);
  static std::string StringToHex(const std::string_view& str);
  static std::string HexToString(const std::string& hex_str);

  static void SetPbMessageError(butil::Status status, google::protobuf::Message* message);

  template <typename T>
  static void SetPbMessageErrorLeader(const pb::node::NodeInfo& node_info, T* message) {
    message->mutable_error()->set_store_id(node_info.id());
    if (!node_info.server_location().host().empty()) {
      auto leader_location = message->mutable_error()->mutable_leader_location();
      *leader_location = node_info.server_location();
    }
  }

  static std::string MessageToJsonString(const google::protobuf::Message& message);

  // use raft_location to get server_location
  // in: raft_location
  // out: server_location
  static void GetNodeInfoByRaftLocation(const pb::common::Location& raft_location, pb::node::NodeInfo& node_info);
  static void GetNodeInfoByServerLocation(const pb::common::Location& server_location, pb::node::NodeInfo& node_info);
  static void GetServerLocation(const pb::common::Location& raft_location, pb::common::Location& server_location);
  static void GetRaftLocation(const pb::common::Location& server_location, pb::common::Location& raft_location);
  static pb::common::Peer GetPeerInfo(const butil::EndPoint& endpoint);

  // generate random string for keyring
  static std::string GenerateRandomString(int length);
  static int64_t GenerateRealRandomInteger(int64_t min_value, int64_t max_value);
  static int64_t GenerateRandomInteger(int64_t min_value, int64_t max_value);
  static float GenerateRandomFloat(float min_value, float max_value);
  static int64_t GenId();
  static std::vector<float> GenerateFloatVector(int dimension);
  static std::vector<uint8_t> GenerateInt8Vector(int dimension);

  // Gen coordinator new_table_check_name
  static std::string GenNewTableCheckName(int64_t schema_id, const std::string& table_name);
  static std::string GenNewTenantCheckName(int64_t tenant_id, const std::string& name);

  static std::vector<std::string> GetColumnFamilyNamesByRole();
  static std::vector<std::string> GetColumnFamilyNamesExecptMetaByRole();
  static std::vector<std::string> GetColumnFamilyNames(const std::string& key);
  static void GetColumnFamilyNames(const std::string& key, std::vector<std::string>& raw_cf_names,
                                   std::vector<std::string>& txn_cf_names);
  static bool IsTxnColumnFamilyName(const std::string& cf_name);

  // Create hard link
  static bool Link(const std::string& old_path, const std::string& new_path);

  // nanosecond timestamp
  static int64_t TimestampNs();
  // microseconds
  static int64_t TimestampUs();
  // millisecond timestamp
  static int64_t TimestampMs();
  // second timestamp
  static int64_t Timestamp();
  static std::string NowTime();
  static int NowHour();
  static std::string PastDate(int64_t day);

  // format millisecond timestamp
  static std::string FormatMsTime(int64_t timestamp, const std::string& format);
  static std::string FormatMsTime(int64_t timestamp);
  // format second timestamp
  static std::string FormatTime(int64_t timestamp, const std::string& format);

  // format: "2021-01-01T00:00:00.000Z"
  static std::string GetNowFormatMsTime();

  // end key of all table
  // We are based on this assumption. In general, it is rare to see all 0xFF
  static bool KeyIsEndOfAllTable(const std::string& key);

  static bool GetSystemDiskCapacity(const std::string& path, std::map<std::string, int64_t>& output);
  static bool GetSystemMemoryInfo(std::map<std::string, int64_t>& output);
  static bool GetSystemCpuUsage(std::map<std::string, int64_t>& output);
  static bool GetSystemDiskIoUtil(const std::string& device_name, std::map<std::string, int64_t>& output);
  static bool GetProcessMemoryInfo(std::map<std::string, int64_t>& output);

  static void AlignByteArrays(std::string& a, std::string& b);
  // Notice: String will add one element as a prefix of the result, this element is for the carry
  // if you want the equal length of your input, you need to do substr by yourself
  static std::string StringAdd(const std::string& input_a, const std::string& input_b);
  static std::string StringSubtract(const std::string& input_a, const std::string& input_b);

  // Notice: if array % 2 != 0, the result size is array.size() + 1
  static std::string StringDivideByTwo(const std::string& array);

  static void RightAlignByteArrays(std::string& a, std::string& b);
  static std::string StringAddRightAlign(const std::string& input_a, const std::string& input_b);
  static std::string StringSubtractRightAlign(const std::string& input_a, const std::string& input_b);
  static std::string StringDivideByTwoRightAlign(const std::string& array);

  static std::string CalculateMiddleKey(const std::string& start_key, const std::string& end_key);

  static std::vector<uint8_t> SubtractByteArrays(const std::vector<uint8_t>& a, const std::vector<uint8_t>& b);
  static std::vector<uint8_t> DivideByteArrayByTwo(const std::vector<uint8_t>& array);
  // Notice: AddByteArrays will add one element as a prefix of the result, this element is for the carry
  // if you want the equal length of your input, you need to do substr by yourself
  static std::vector<uint8_t> AddByteArrays(const std::vector<uint8_t>& a, const std::vector<uint8_t>& b);

  // filesystem operations
  static std::string ConcatPath(const std::string& path1, const std::string& path2);
  static std::vector<std::string> TraverseDirectory(const std::string& path, bool ignore_dir = false,
                                                    bool ignore_file = false);
  static std::vector<std::string> TraverseDirectory(const std::string& path, const std::string& prefix,
                                                    bool ignore_dir = false, bool ignore_file = false);
  static std::string FindFileInDirectory(const std::string& dirpath, const std::string& prefix);
  static bool CreateDirectory(const std::string& path);
  static butil::Status CreateDirectories(const std::string& path);
  static bool RemoveFileOrDirectory(const std::string& path);
  static bool RemoveAllFileOrDirectory(const std::string& path);
  static butil::Status Rename(const std::string& src_path, const std::string& dst_path, bool is_force = true);
  static bool IsExistPath(const std::string& path);
  static int64_t GetFileSize(const std::string& path);
  static butil::Status SavePBFile(const std::string& path, const ::google::protobuf::Message* message);
  static butil::Status LoadPBFile(const std::string& path, google::protobuf::Message* message);

  // vector scalar index value
  static bool IsEqualVectorScalarValue(const pb::common::ScalarValue& value1, const pb::common::ScalarValue& value2);

  // Upper string
  static std::string ToUpper(const std::string& str);
  // Lower string
  static std::string ToLower(const std::string& str);
  // String trim
  static std::string Ltrim(const std::string& s, const std::string& delete_str);
  static std::string Rtrim(const std::string& s, const std::string& delete_str);
  static std::string Trim(const std::string& s, const std::string& delete_str);

  // string type cast
  static bool StringToBool(const std::string& str);
  static int32_t StringToInt32(const std::string& str);
  static int64_t StringToInt64(const std::string& str);
  static float StringToFloat(const std::string& str);
  static double StringToDouble(const std::string& str);

  static std::vector<float> StringToVector(const std::string& str);
  static std::vector<uint8_t> StringToVectorBinary(const std::string& str);

  // Clean string first slash, e.g. /name.txt -> name.txt
  static std::string CleanFirstSlash(const std::string& str);

  // Parallel run task, e.g. load vector index.
  using TaskFunctor = void* (*)(void*);
  static bool ParallelRunTask(TaskFunctor task, void* arg, int concurrency);

  // Validate raft status whether suitable or not region split/merge.
  static butil::Status ValidateRaftStatusForSplitMerge(std::shared_ptr<pb::common::BRaftStatus> raft_status);

  static butil::Status ParseRaftSnapshotRegionMeta(const std::string& snapshot_path,
                                                   pb::store_internal::RaftSnapshotRegionMeta& meta);

  // 0: src_epoch == dst_epoch
  // -1: src_epoch < dst_epoch
  // 1: src_epoch > dst_epoch
  static int CompareRegionEpoch(const pb::common::RegionEpoch& src_epoch, const pb::common::RegionEpoch& dst_epoch);
  static bool IsEqualRegionEpoch(const pb::common::RegionEpoch& src_epoch, const pb::common::RegionEpoch& dst_epoch);
  static std::string RegionEpochToString(const pb::common::RegionEpoch& epoch);

  static std::string PrintStatus(const butil::Status& status);

  // check if number in set is continuous
  static bool IsContinuous(const std::set<int64_t>& numbers);

  static void SplitString(const std::string& str, char c, std::vector<std::string>& vec);
  static void SplitString(const std::string& str, char c, std::vector<int64_t>& vec);

  static float DingoFaissInnerProduct(const float* x, const float* y, size_t d);

  static float DingoFaissL2sqr(const float* x, const float* y, size_t d);

  static float DingoHnswInnerProduct(const float* p_vect1, const float* p_vect2, size_t d);

  static float DingoHnswInnerProductDistance(const float* p_vect1, const float* p_vect2, size_t d);

  static float DingoHnswL2Sqr(const float* p_vect1v, const float* p_vect2v, size_t d);

  template <typename T>
  static std::string SetToString(const std::set<T>& set) {
    std::stringstream ss;
    size_t i = 0;
    for (auto& elem : set) {
      if (i++ != 0) ss << ", ";
      ss << elem;
    }
    return ss.str();
  }

  static bool SaveFile(const std::string& filepath, const std::string& data);

  static void PrintHtmlTable(std::ostream& os, bool use_html, const std::vector<std::string>& table_header,
                             const std::vector<int32_t>& min_widths,
                             const std::vector<std::vector<std::string>>& table_contents,
                             const std::vector<std::vector<std::string>>& table_urls);

  static void PrintHtmlLines(std::ostream& os, bool use_html, const std::vector<std::string>& table_header,
                             const std::vector<int32_t>& min_widths,
                             const std::vector<std::vector<std::string>>& table_contents,
                             const std::vector<std::vector<std::string>>& table_urls);

  static int32_t GetCores();
  static int64_t GetPid();
  static std::vector<int64_t> GetThreadIds(int64_t pid);
  static std::vector<std::string> GetThreadNames(int64_t pid);
  static std::vector<std::string> GetThreadNames(int64_t pid, const std::string& filter_name);

  template <typename T>
  static void ShuffleVector(std::vector<T>& vec) {
    std::random_device rd;
    std::shuffle(vec.begin(), vec.end(), rd);
  }

  static LogLevel LogLevelPB2LogLevel(const pb::node::LogLevel& level);

  static std::string ConvertColumnValueToString(const pb::meta::ColumnDefinition& column_definition,
                                                const std::any& value);

  static std::string ConvertColumnValueToStringV2(const pb::meta::ColumnDefinition& column_definition,
                                                const std::any& value);

  static pb::common::Schema::Type TransformSchemaType(const std::string& name);

  static bool IsSupportSplitAndMerge(const pb::common::RegionDefinition& definition);

  static void CalSha1CodeWithString(const std::string& source, std::string& hash_code);
  static butil::Status CalSha1CodeWithFile(const std::string& path, std::string& hash_code);
  static butil::Status CalSha1CodeWithFileEx(const std::string& path, std::string& hash_code);

  static bool StringConvertTrue(const std::string& str);
  static bool StringConvertFalse(const std::string& str);
  static void HandleBoolControlConfigVariable(const pb::common::ControlConfigVariable& variable,
                                              pb::common::ControlConfigVariable& config, bool& gflags_var);

  static size_t FindReEnd(const std::string& s, size_t start_pos);
  static std::string Base64Encode(const std::string& input);
  static std::string EncodeREContent(const std::string& input);
};

}  // namespace dingodb

#endif
