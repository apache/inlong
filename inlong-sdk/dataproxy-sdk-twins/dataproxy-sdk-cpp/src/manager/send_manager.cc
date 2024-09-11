/**
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

#include "send_manager.h"
#include "../utils/utils.h"
#include "proxy_manager.h"
namespace inlong {
SendManager::SendManager() : send_group_idx_(0) {
  LOG_INFO("SendManager,send group num:"
           << SdkConfig::getInstance()->per_groupid_thread_nums_);
}

SendGroupPtr SendManager::GetSendGroup(const std::string &group_key) {
  std::string send_key= GetSendKey(group_key);
  SendGroupPtr send_group_ptr = DoGetSendGroup(send_key);
  if (send_group_ptr == nullptr) {
    AddSendGroup(send_key);
  }
  return send_group_ptr;
}

bool SendManager::AddSendGroup(const std::string &send_group_key) {
  if (!ProxyManager::GetInstance()->HasProxy(send_group_key)) {
    LOG_ERROR("inlong_group_id is not exist." << send_group_key);
    return false;
  }
  DoAddSendGroup(send_group_key);
  return false;
}

void SendManager::DoAddSendGroup(const std::string &send_group_key) {
  unique_write_lock<read_write_mutex> wtlck(send_group_map_rwmutex_);
  auto send_group_map = send_group_map_.find(send_group_key);
  if (send_group_map != send_group_map_.end()) {
    LOG_WARN("send group has exist." << send_group_key);
    return;
  }
  std::vector<SendGroupPtr> send_group;
  send_group.reserve(SdkConfig::getInstance()->per_groupid_thread_nums_);
  for (int32_t j = 0; j < SdkConfig::getInstance()->per_groupid_thread_nums_;
       j++) {
    send_group.push_back(std::make_shared<SendGroup>(send_group_key));
  }
  send_group_map_[send_group_key] = send_group;
}

SendGroupPtr SendManager::DoGetSendGroup(const std::string &send_group_key) {
  unique_read_lock<read_write_mutex> rdlck(send_group_map_rwmutex_);
  auto send_group_map = send_group_map_.find(send_group_key);
  if (send_group_map == send_group_map_.end()) {
    LOG_ERROR("Fail to get send group, group key:" << send_group_key);
    return nullptr;
  }
  if (send_group_map->second.empty()) {
    return nullptr;
  }
  auto send_group_vec = send_group_map->second;
  send_group_idx_++;
  if (send_group_idx_ >= send_group_vec.size()) {
    send_group_idx_ = 0;
  }
  return send_group_vec[send_group_idx_];
}
std::string SendManager::GetSendKey(const std::string &send_group_key) {
  if (constants::IsolationLevel::kLevelSecond == SdkConfig::getInstance()->isolation_level_) {
    return ProxyManager::GetInstance()->GetClusterID(send_group_key);
  }
  return send_group_key;
}
} // namespace inlong
