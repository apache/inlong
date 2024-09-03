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

#ifndef INLONG_BUFFER_MANAGER_H
#define INLONG_BUFFER_MANAGER_H

namespace inlong {
class BufferManager {
 private:
  std::queue<SendBufferPtrT> buffer_queue_;
  mutable std::mutex mutex_;
  uint32_t queue_limit_;
  bool exit_= false;
  BufferManager() {
    uint32_t data_capacity_ = std::max(SdkConfig::getInstance()->max_msg_size_,
                                       SdkConfig::getInstance()->pack_size_);
    uint32_t buffer_num =
        (SdkConfig::getInstance()->recv_buf_size_ / data_capacity_) *
            SdkConfig::getInstance()->instance_num_;
    queue_limit_ =
        std::min(SdkConfig::getInstance()->max_cache_num_, buffer_num);
    LOG_INFO("Data capacity:"
                 << data_capacity_ << ", buffer num: " << buffer_num
                 << ", instance num: " << SdkConfig::getInstance()->instance_num_
                 << ", limit: " << queue_limit_ << " ,max cache num: "
                 << SdkConfig::getInstance()->max_cache_num_);
    for ( uint32_t index = 0; index < queue_limit_; index++) {
      std::shared_ptr<SendBuffer> send_buffer =
          std::make_shared<SendBuffer>(data_capacity_);
      if (send_buffer == nullptr) {
        LOG_INFO("Buffer manager is null");
        continue;
      }
      AddSendBuffer(send_buffer);
    }
  }
  ~BufferManager(){
    std::lock_guard<std::mutex> lck(mutex_);
    exit_ = true;
    LOG_INFO("Buffer manager exited");
  }

 public:
  static BufferManager *GetInstance() {
    static BufferManager instance;
    return &instance;
  }
  SendBufferPtrT GetSendBuffer() {
    if(exit_){
      return nullptr;
    }
    std::lock_guard<std::mutex> lck(mutex_);
    if (buffer_queue_.empty()) {
      return nullptr;
    }
    SendBufferPtrT buf = buffer_queue_.front();
    buffer_queue_.pop();
    return buf;
  }
  void AddSendBuffer(const SendBufferPtrT &send_buffer) {
    if (nullptr == send_buffer || exit_) {
      return;
    }
    send_buffer->releaseBuf();
    std::lock_guard<std::mutex> lck(mutex_);
    if (buffer_queue_.size() > queue_limit_) {
      return;
    }
    buffer_queue_.emplace(send_buffer);
  }
};
} // namespace inlong
#endif // INLONG_BUFFER_MANAGER_H
