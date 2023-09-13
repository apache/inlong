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

#ifndef INLONG_SDK_SEND_BUFFER_H
#define INLONG_SDK_SEND_BUFFER_H

#include <mutex>
#include <string>

#include "atomic.h"
#include "logger.h"
#include "noncopyable.h"
#include "sdk_msg.h"
#include <asio.hpp>
#include <deque>
#include <queue>

namespace inlong {
class Connection;
using SteadyTimerPtr = std::shared_ptr<asio::steady_timer>;
using ConnectionPtr = std::shared_ptr<Connection>;
class SendBuffer : noncopyable {
private:
  uint32_t uniq_id_;
  std::atomic<bool> is_used_;
  std::atomic<bool> is_packed_;
  char *content_;
  uint32_t size_;
  int32_t msg_cnt_;
  uint32_t len_;
  std::string inlong_group_id_;
  std::string inlong_stream_id_;
  AtomicInt already_send_;
  uint64_t first_send_time_;
  uint64_t latest_send_time_;
  std::vector<SdkMsgPtr> user_msg_vector_;

public:
  SendBuffer(uint32_t size)
      : uniq_id_(0), is_used_(false), is_packed_(false), size_(size),
        msg_cnt_(0), len_(0), inlong_group_id_(), inlong_stream_id_(),
        first_send_time_(0), latest_send_time_(0) {
    content_ = new char[size];
    if (content_) {
      memset(content_, 0x0, size);
    }
  }
  ~SendBuffer() {
    if (content_) {
      delete[] content_;
    }
  }

  char *content() { return content_; }
  int32_t msgCnt() const { return msg_cnt_; }
  void setMsgCnt(const int32_t &msg_cnt) { msg_cnt_ = msg_cnt; }
  uint32_t len() { return len_; }
  void setLen(const uint32_t len) { len_ = len; }
  std::string bid() { return inlong_group_id_; }
  std::string tid() { return inlong_stream_id_; }
  void setInlongGroupId(const std::string &inlong_group_id) {
    inlong_group_id_ = inlong_group_id;
  }
  void setStreamId(const std::string &inlong_stream_id) {
    inlong_stream_id_ = inlong_stream_id;
  }

  void setUniqId(const uint32_t &uniq_id) { uniq_id_ = uniq_id; }

  void addUserMsg(SdkMsgPtr msg) { user_msg_vector_.push_back(msg); }
  void doUserCallBack() {
    for (auto it : user_msg_vector_) {
      if (it->cb_) {
        it->cb_(inlong_group_id_.data(), inlong_stream_id_.data(),
                it->msg_.data(), it->msg_.size(), it->user_report_time_,
                it->user_client_ip_.data());
      }
    }
  }

  void releaseBuf() {
    if (!is_used_) {
      return;
    }
    uniq_id_ = 0;
    is_used_ = false;
    is_packed_ = false;
    memset(content_, 0x0, size_);
    msg_cnt_ = 0;
    len_ = 0;
    inlong_group_id_ = "";
    inlong_stream_id_ = "";
    already_send_.getAndSet(0);
    first_send_time_ = 0;
    latest_send_time_ = 0;
    user_msg_vector_.clear();
    AtomicInt fail_create_conn_;
    fail_create_conn_.getAndSet(0);
  }

  void setIsPacked(bool is_packed) { is_packed_ = is_packed; }
};
typedef std::shared_ptr<SendBuffer> SendBufferPtrT;
typedef std::queue<SendBufferPtrT> SendBufferPtrDeque;
} // namespace inlong

#endif // INLONG_SDK_SEND_BUFFER_H