/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "recv_manager.h"
#include "../utils/utils.h"
#include "proxy_manager.h"

namespace inlong {
RecvManager::RecvManager(std::shared_ptr<SendManager> send_manager)
    : work_(asio::make_work_guard(io_context_)), send_manager_(send_manager),
      exit_flag_(false) {
  dispatch_interval_ = SdkConfig::getInstance()->dispatch_interval_zip_;

  max_groupid_streamid_num_ =
      std::max(SdkConfig::getInstance()->max_group_id_num_,
               SdkConfig::getInstance()->max_stream_id_num_);
  LOG_INFO("max_groupid_streamid_num " <<max_groupid_streamid_num_);

  check_timer_ = std::make_shared<asio::steady_timer>(io_context_);
  check_timer_->expires_after(std::chrono::milliseconds(10));
  check_timer_->async_wait(
      std::bind(&RecvManager::DispatchData, this, std::placeholders::_1));

  thread_ = std::thread(&RecvManager::Run, this);
};

RecvManager::~RecvManager() {
  LOG_INFO("~RecvManager ");
  if (check_timer_) {
    check_timer_->cancel();
  }
  exit_flag_ = true;
  for (auto it : recv_group_map_) {
    it.second->DispatchMsg(true);
  }
  recv_group_map_.clear();

  io_context_.stop();

  if (thread_.joinable()) {
    thread_.join();
  }
}
void RecvManager::Run() { io_context_.run(); }
RecvGroupPtr RecvManager::GetRecvGroup(const std::string &groupId) {
  std::lock_guard<std::mutex> lck(mutex_);
  std::string group_key = ProxyManager::GetInstance()->GetGroupKey(groupId);
  if (group_key.empty()) {
    return nullptr;
  }
  auto it = recv_group_map_.find(group_key);
  if (it != recv_group_map_.end()) {
    return it->second;
  } else {
    if (recv_group_map_.size() > max_groupid_streamid_num_) {
      return nullptr;
    }

    RecvGroupPtr recv_group =
        std::make_shared<RecvGroup>(group_key, send_manager_);
    recv_group_map_.emplace(group_key, recv_group);
    return recv_group;
  }
}

void RecvManager::DispatchData(std::error_code error) {
  if (error) {
    LOG_WARN("DoDispatchMsg error " << error.message());
    return;
  }
  for (auto it : recv_group_map_) {
    it.second->DispatchMsg(exit_flag_);
  }
  check_timer_->expires_after(std::chrono::milliseconds(dispatch_interval_));
  check_timer_->async_wait(
      std::bind(&RecvManager::DispatchData, this, std::placeholders::_1));
}
} // namespace inlong