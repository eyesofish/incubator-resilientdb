/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "interface/rdbc/transaction_constructor.h"

#include <glog/logging.h>
#include <google/protobuf/descriptor.h>
#include <set>

namespace resdb {

namespace {

bool IsNotLeaderResponse(const google::protobuf::Message& response) {
  const auto* descriptor = response.GetDescriptor();
  const auto* reflection = response.GetReflection();
  if (descriptor == nullptr || reflection == nullptr) {
    return false;
  }
  const auto* value_field = descriptor->FindFieldByName("value");
  if (value_field == nullptr ||
      value_field->cpp_type() !=
          google::protobuf::FieldDescriptor::CPPTYPE_STRING) {
    return false;
  }
  const std::string value = reflection->GetString(response, value_field);
  return value == "NOT_LEADER";
}

}  // namespace

TransactionConstructor::TransactionConstructor(const ResDBConfig& config)
    : NetChannel("", 0),
      config_(config),
      timeout_ms_(
          config.GetClientTimeoutMs()) {  // default 2s for process timeout
  socket_->SetRecvTimeout(timeout_ms_);
  // Fail fast on a replica and move to the next one.
  SetMaxRetryCount(1);
}

absl::StatusOr<std::string> TransactionConstructor::GetResponseData(
    const Response& response) {
  std::string hash_;
  std::set<int64_t> hash_counter;
  std::string resp_str;
  for (const auto& each_resp : response.resp()) {
    // Check signature
    std::string hash = SignatureVerifier::CalculateHash(each_resp.data());

    if (!hash_.empty() && hash != hash_) {
      LOG(ERROR) << "hash not the same";
      return absl::InvalidArgumentError("hash not match");
    }
    if (hash_.empty()) {
      hash_ = hash;
      resp_str = each_resp.data();
    }
    hash_counter.insert(each_resp.signature().node_id());
  }
  // LOG(INFO) << "recv hash:" << hash_counter.size()
  //         << " need:" << config_.GetMinClientReceiveNum()
  //        << " data len:" << resp_str.size();
  if (hash_counter.size() >=
      static_cast<size_t>(config_.GetMinClientReceiveNum())) {
    return resp_str;
  }
  return absl::InvalidArgumentError("data not enough");
}

int TransactionConstructor::SendRequest(
    const google::protobuf::Message& message, Request::Type type) {
  const auto& replicas = config_.GetReplicaInfos();
  if (replicas.empty()) {
    LOG(ERROR) << "client config contains no replicas";
    return -1;
  }
  const size_t replica_count = replicas.size();
  size_t start_index = next_replica_index_.load();
  if (start_index >= replica_count) {
    start_index = 0;
  }
  for (size_t offset = 0; offset < replica_count; ++offset) {
    const auto& replica = replicas[(start_index + offset) % replica_count];
    NetChannel::SetDestReplicaInfo(replica);
    int ret = NetChannel::SendRequest(message, type, false);
    if (ret >= 0) {
      next_replica_index_.store((start_index + offset) % replica_count);
      return ret;
    }
    // Close the socket so the next attempt re-establishes a connection.
    Close();
    LOG(WARNING) << "send request to replica " << replica.id()
                 << " failed, trying next replica";
  }
  return -1;
}

int TransactionConstructor::SendRequest(
    const google::protobuf::Message& message,
    google::protobuf::Message* response, Request::Type type) {
  const auto& replicas = config_.GetReplicaInfos();
  if (replicas.empty()) {
    LOG(ERROR) << "client config contains no replicas";
    return -1;
  }
  const size_t replica_count = replicas.size();
  size_t start_index = next_replica_index_.load();
  if (start_index >= replica_count) {
    start_index = 0;
  }
  for (size_t offset = 0; offset < replica_count; ++offset) {
    const auto& replica = replicas[(start_index + offset) % replica_count];
    NetChannel::SetDestReplicaInfo(replica);
    int ret = NetChannel::SendRequest(message, type, true);
    if (ret < 0) {
      Close();
      LOG(WARNING) << "send request to replica " << replica.id()
                   << " failed, trying next replica";
      continue;
    }
    std::string resp_str;
    ret = NetChannel::RecvRawMessageData(&resp_str);
    if (ret < 0) {
      Close();
      LOG(WARNING) << "recv response from replica " << replica.id()
                   << " failed, trying next replica";
      continue;
    }
    if (!response->ParseFromString(resp_str)) {
      LOG(ERROR) << "parse response fail:" << resp_str.size();
      return -2;
    }
    if (IsNotLeaderResponse(*response)) {
      Close();
      LOG(WARNING) << "replica " << replica.id()
                   << " is not leader, trying next replica";
      continue;
    }
    next_replica_index_.store((start_index + offset) % replica_count);
    return 0;
  }
  return -1;
}

}  // namespace resdb
