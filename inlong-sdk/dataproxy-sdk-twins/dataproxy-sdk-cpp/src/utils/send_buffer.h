/**
 * Copyright 2023 Tencent Inc.
 * All rights reserved.
 * Author: doleyzi@tencent.com
 **/

#ifndef INLONG_SEND_BUFFER_H
#define INLONG_SEND_BUFFER_H

#include <mutex>
#include <string>
#include <deque>
#include <queue>

#include "asio.hpp"
#include "atomic.h"
#include "logger.h"
#include "noncopyable.h"

#include "../core/sdk_msg.h"
#include "../manager/msg_manager.h"

namespace inlong {
class SendBuffer : noncopyable {
 private:
  char *data_;
  uint32_t data_len_;
  uint32_t msg_cnt_;
  std::string inlong_group_id_;
  std::string inlong_stream_id_;
  std::vector<SdkMsgPtr> user_msg_vector_;

 public:
  SendBuffer(uint32_t size) : msg_cnt_(0), data_len_(0), inlong_group_id_(), inlong_stream_id_() {
    data_ = new char[size];
    if (data_) {
      memset(data_, 0x0, size);
    }
  }
  ~SendBuffer() {
    if (data_) {
      delete[] data_;
    }
  }
  char *GetData() const {
    return data_;
  }
  void SetData(char *data) {
    data_ = data;
  }
  uint32_t GetDataLen() const {
    return data_len_;
  }
  void SetDataLen(uint32_t data_len) {
    data_len_ = data_len;
  }
  uint32_t GetMsgCnt() const {
    return msg_cnt_;
  }
  void SetMsgCnt(uint32_t msg_cnt) {
    msg_cnt_ = msg_cnt;
  }
  const std::string &GetInlongGroupId() const {
    return inlong_group_id_;
  }
  void SetInlongGroupId(const std::string &inlong_group_id) {
    inlong_group_id_ = inlong_group_id;
  }
  const std::string &GetInlongStreamId() const {
    return inlong_stream_id_;
  }
  void SetInlongStreamId(const std::string &inlong_stream_id) {
    inlong_stream_id_ = inlong_stream_id;
  }

  void addUserMsg(const SdkMsgPtr &msg) { user_msg_vector_.push_back(msg); }

  void doUserCallBack() {
    for (auto it : user_msg_vector_) {
      if (it->cb_) {
        it->cb_(it->inlong_group_id_.data(),
                it->inlong_stream_id_.data(),
                it->msg_.data(),
                it->msg_.size(),
                it->report_time_,
                it->client_ip_.data());
      }
    }
  }

  void releaseBuf() {
    msg_cnt_ = 0;
    data_len_ = 0;
    inlong_group_id_ = "";
    inlong_stream_id_ = "";
    for (const auto &it : user_msg_vector_) {
      if (it->cb_) {
        it->clear();
      }
    }
    MsgManager::GetInstance()->AddMsg(user_msg_vector_);
    user_msg_vector_.clear();
    user_msg_vector_.shrink_to_fit();
  }
};
typedef std::shared_ptr<SendBuffer> SendBufferPtrT;
}  // namespace inlong

#endif  // INLONG_SEND_BUFFER_H