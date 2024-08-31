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

#include <queue>
#include "../config/sdk_conf.h"
#include "../utils/send_buffer.h"
#include "../utils/logger.h"
#include "../core/sdk_msg.h"

#ifndef INLONG_MSG_MANAGER_H
#define INLONG_MSG_MANAGER_H
namespace inlong {
class MsgManager {
 private:
  std::queue<SdkMsgPtr> msg_queue_;
  mutable std::mutex mutex_;
  uint32_t queue_limit_;
  bool enable_share_msg_;
  bool exit_= false;
  MsgManager() {
    uint32_t data_capacity_ = std::max(SdkConfig::getInstance()->max_msg_size_, SdkConfig::getInstance()->pack_size_);
    uint32_t buffer_num = SdkConfig::getInstance()->recv_buf_size_ / data_capacity_;
    queue_limit_ = std::min(SdkConfig::getInstance()->max_cache_num_, buffer_num);
    enable_share_msg_ = SdkConfig::getInstance()->enable_share_msg_;
    LOG_INFO("Data capacity:" << data_capacity_ << ", buffer_num: " << buffer_num << ", limit: " << queue_limit_
                              << ", enable share msg: " << enable_share_msg_);
    for (int i = 0; i < queue_limit_; i++) {
      std::shared_ptr<SdkMsg> msg_ptr = std::make_shared<SdkMsg>();
      if (msg_ptr == nullptr) {
        LOG_INFO("Msg ptr is null");
        continue;
      }
      AddMsg(msg_ptr);
    }
  }
  ~MsgManager(){
    std::lock_guard<std::mutex> lck(mutex_);
    exit_ = true;
    LOG_INFO("Msg manager exited");
  }

 public:
  static MsgManager *GetInstance() {
    static MsgManager instance;
    return &instance;
  }
  SdkMsgPtr GetMsg() {
    if (!enable_share_msg_ || exit_) {
      return nullptr;
    }
    std::lock_guard<std::mutex> lck(mutex_);
    if (msg_queue_.empty()) {
      return nullptr;
    }
    SdkMsgPtr buf = msg_queue_.front();
    msg_queue_.pop();
    return buf;
  }
  void AddMsg(const SdkMsgPtr &msg_ptr) {
    if (nullptr == msg_ptr || !enable_share_msg_ || exit_) {
      return;
    }
    std::lock_guard<std::mutex> lck(mutex_);
    if (msg_queue_.size() > queue_limit_) {
      return;
    }
    msg_queue_.emplace(msg_ptr);
  }

  void AddMsg(const std::vector<SdkMsgPtr> &user_msg_vector) {
    if (!enable_share_msg_ || exit_) {
      return;
    }
    std::lock_guard<std::mutex> lck(mutex_);
    for (auto it : user_msg_vector) {
      if (nullptr == it) {
        continue;
      }
      if (msg_queue_.size() > queue_limit_) {
        return;
      }
      msg_queue_.emplace(std::move(it));
    }
  }
};
}  // namespace inlong
#endif  // INLONG_MSG_MANAGER_H
