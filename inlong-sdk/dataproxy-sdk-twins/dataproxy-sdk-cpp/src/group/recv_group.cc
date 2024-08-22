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

#include "recv_group.h"

#include <functional>
#include "../core/api_code.h"
#include "../manager/buffer_manager.h"
#include "../utils/utils.h"
#include "../manager/msg_manager.h"
#include "../manager/metric_manager.h"

namespace inlong {
const uint32_t DEFAULT_PACK_ATTR = 400;
const uint64_t LOG_SAMPLE = 100;
RecvGroup::RecvGroup(const std::string &group_key, std::shared_ptr<SendManager> send_manager)
    : cur_len_(0),
      group_key_(group_key),
      groupId_num_(0),
      streamId_num_(0),
      msg_type_(SdkConfig::getInstance()->msg_type_),
      data_capacity_(SdkConfig::getInstance()->recv_buf_size_),
      send_manager_(send_manager),
      log_stat_(0),
      send_group_(nullptr),
      max_msg_size_(0),
      uniq_id_(0) {
  data_capacity_ = std::max(SdkConfig::getInstance()->max_msg_size_, SdkConfig::getInstance()->pack_size_);
  data_capacity_ = data_capacity_ + DEFAULT_PACK_ATTR;

  pack_buf_ = new char[data_capacity_];
  memset(pack_buf_, 0x0, data_capacity_);
  data_time_ = Utils::getCurrentMsTime();
  last_pack_time_ = Utils::getCurrentMsTime();
  max_recv_size_ = SdkConfig::getInstance()->recv_buf_size_;
  local_ip_ = SdkConfig::getInstance()->local_ip_;
  group_id_key_ = SdkConfig::getInstance()->extend_report_ ? "bid=" : "groupId=";
  stream_id_key_ = SdkConfig::getInstance()->extend_report_ ? "&tid=" : "&streamId=";
  LOG_INFO("RecvGroup:" << group_key_ << ",data_capacity:" << data_capacity_ << ",max_recv_size:" << max_recv_size_);
}

RecvGroup::~RecvGroup() {
  if (pack_buf_) {
    delete[] pack_buf_;
    pack_buf_ = nullptr;
  }
}

int32_t RecvGroup::SendData(const std::string &msg, const std::string &inlong_group_id_, const std::string &inlong_stream_id_,
                            uint64_t report_time, UserCallBack call_back) {
  std::lock_guard<std::mutex> lck(mutex_);
  if (msg.size() + cur_len_ > max_recv_size_) {
    MetricManager::GetInstance()->AddReceiveBufferFullCount(inlong_group_id_,inlong_stream_id_,1);
    return SdkCode::kRecvBufferFull;
  }

  uint64_t data_time = (report_time == 0) ? data_time_ : report_time;

  std::string data_pack_format_attr =
      "__addcol1__reptime=" + Utils::getFormatTime(data_time) + "&__addcol2__ip=" + local_ip_;
  max_msg_size_ = std::max(max_msg_size_, msg.size());
  auto it = recv_queue_.find(inlong_group_id_ + inlong_stream_id_);
  if (it == recv_queue_.end()) {
    std::queue<SdkMsgPtr> tmp;
    it = recv_queue_.insert(recv_queue_.begin(), std::make_pair(inlong_group_id_ + inlong_stream_id_, tmp));
  }
  SdkMsgPtr msg_ptr = MsgManager::GetInstance()->GetMsg();
  if(nullptr == msg_ptr){
    it->second.emplace(std::make_shared<SdkMsg>(msg, local_ip_, data_time, call_back, data_pack_format_attr, local_ip_, data_time, inlong_group_id_, inlong_stream_id_));
  }else{
    msg_ptr->setMsg(msg);
    msg_ptr->setClientIp(local_ip_);
    msg_ptr->setReportTime(data_time);
    msg_ptr->setCb(call_back);
    msg_ptr->setDataPackFormatAttr(data_pack_format_attr);
    msg_ptr->setUserClientIp(local_ip_);
    msg_ptr->setUserReportTime(data_time);
    msg_ptr->setGroupId(inlong_group_id_);
    msg_ptr->setStreamId(inlong_stream_id_);
    it->second.emplace(msg_ptr);
  }

  cur_len_ = cur_len_ + msg.size() + constants::ATTR_LENGTH;

  return SdkCode::kSuccess;
}

void RecvGroup::DoDispatchMsg() {
  if (!CanDispatch()) {
    return;
  }
  last_pack_time_ = Utils::getCurrentMsTime();
  data_time_ = last_pack_time_;
  while (!fail_queue_.empty()) {
    SendBufferPtrT tmp_ptr = fail_queue_.front();
    if (SdkCode::kSuccess != send_group_->PushData(tmp_ptr)) {
      return;
    }
    fail_queue_.pop();
  }

  {
    std::lock_guard<std::mutex> lck(mutex_);
    recv_queue_.swap(dispatch_queue_);
  }

  for (auto &it : dispatch_queue_) {
    std::vector<SdkMsgPtr> msg_vec;
    uint64_t msg_size = 0;
    while (!it.second.empty()) {
      SdkMsgPtr msg = it.second.front();
      msg_vec.push_back(msg);
      msg_size = msg_size + msg->msg_.size() + constants::ATTR_LENGTH;
      it.second.pop();

      if ((msg_size + max_msg_size_) >= SdkConfig::getInstance()->pack_size_) {
        uint32_t ret = ParseMsg(msg_vec);
        if (SdkCode::kBufferManagerFull == ret) {
          for (const auto &it_msg : msg_vec) {
            it.second.emplace(it_msg);
          }
          return;
        }
        UpdateCurrentMsgLen(msg_size);
        msg_size = 0;

        if (SdkCode::kSuccess != ret) {
          return;
        }
        std::vector<SdkMsgPtr>().swap(msg_vec);
      }
    }
    if (!msg_vec.empty()) {
      uint32_t ret = ParseMsg(msg_vec);
      if (SdkCode::kBufferManagerFull == ret) {
        for (const auto &it_msg : msg_vec) {
          it.second.emplace(it_msg);
        }
        return;
      }
      UpdateCurrentMsgLen(msg_size);

      if (SdkCode::kSuccess != ret) {
        return;
      }
    }
  }
}

bool RecvGroup::PackMsg(std::vector<SdkMsgPtr> &msgs, char *pack_data,uint32_t &out_len) {
  if (pack_data == nullptr) {
    LOG_ERROR("nullptr, failed to allocate memory for buf");
    return false;
  }
  uint32_t idx = 0;
  for (auto &it : msgs) {
    if (msg_type_ >= 5) {
      *(uint32_t *)(&pack_buf_[idx]) = htonl(it->msg_.size());
      idx += sizeof(uint32_t);
    }
    memcpy(&pack_buf_[idx], it->msg_.data(), it->msg_.size());
    idx += static_cast<uint32_t>(it->msg_.size());

    if (SdkConfig::getInstance()->isAttrDataPackFormat()) {
      *(uint32_t *)(&pack_buf_[idx]) = htonl(it->data_pack_format_attr_.size());
      idx += sizeof(uint32_t);

      memcpy(&pack_buf_[idx], it->data_pack_format_attr_.data(),
             it->data_pack_format_attr_.size());
      idx += static_cast<uint32_t>(it->data_pack_format_attr_.size());
    }

    if (msg_type_ == 2 || msg_type_ == 3) {
      pack_buf_[idx] = '\n';
      ++idx;
    }
  }

  uint32_t cnt = 1;
  if (msgs.size()) {
    cnt = msgs.size();
  }

  if (msg_type_ >= constants::kBinPackMethod) {
    char *bodyBegin = pack_data + sizeof(BinaryMsgHead) + sizeof(uint32_t);
    uint32_t body_len = 0;

    std::string snappy_res;
    bool isSnappy = IsZipAndOperate(snappy_res, idx);
    char real_msg_type;

    if (isSnappy) {
      body_len = static_cast<uint32_t>(snappy_res.size());
      memcpy(bodyBegin, snappy_res.data(), body_len);
      // msg_type
      real_msg_type = (msg_type_ | constants::kBinSnappyFlag);
    } else {
      body_len = idx;
      memcpy(bodyBegin, pack_buf_, body_len);
      real_msg_type = msg_type_;
    }
    *(uint32_t *)(&(pack_data[sizeof(BinaryMsgHead)])) = htonl(body_len);

    bodyBegin += body_len;

    uint32_t char_groupId_flag = 0;
    std::string groupId_streamId_char;
    uint16_t groupId_num = 0, streamId_num = 0;
    if (SdkConfig::getInstance()->enableChar() || groupId_num_ == 0 ||
        streamId_num_ == 0) {
      groupId_num = 0;
      streamId_num = 0;
      groupId_streamId_char = group_id_key_ + msgs[0]->inlong_group_id_ + stream_id_key_ + msgs[0]->inlong_stream_id_;
      char_groupId_flag = 0x4;
    } else {
      groupId_num = groupId_num_;
      streamId_num = streamId_num_;
    }
    uint16_t ext_field =
        (SdkConfig::getInstance()->extend_field_ | char_groupId_flag);
    uint32_t data_time = data_time_ / 1000;

    std::string attr;
    if (SdkConfig::getInstance()->enableTraceIP()) {
      if (groupId_streamId_char.empty())
        attr = "node1ip=" + SdkConfig::getInstance()->local_ip_ +
            "&rtime1=" + std::to_string(Utils::getCurrentMsTime());
      else
        attr = groupId_streamId_char +
            "&node1ip=" + SdkConfig::getInstance()->local_ip_ +
            "&rtime1=" + std::to_string(Utils::getCurrentMsTime());
    } else {
      attr = group_id_key_ + msgs[0]->inlong_group_id_ +
          "&streamId=" + msgs[0]->inlong_stream_id_;
    }
    *(uint16_t *)bodyBegin = htons(attr.size());
    bodyBegin += sizeof(uint16_t);
    memcpy(bodyBegin, attr.data(), attr.size());
    bodyBegin += attr.size();

    *(uint16_t *)bodyBegin = htons(constants::kBinaryMagic);

    uint32_t total_len = 25 + body_len + attr.size();

    char *p = pack_data;
    *(uint32_t *)p = htonl(total_len);
    p += 4;
    *p = real_msg_type;
    ++p;
    *(uint16_t *)p = htons(groupId_num);
    p += 2;
    *(uint16_t *)p = htons(streamId_num);
    p += 2;
    *(uint16_t *)p = htons(ext_field);
    p += 2;
    *(uint32_t *)p = htonl(data_time);
    p += 4;
    *(uint16_t *)p = htons(cnt);
    p += 2;
    *(uint32_t *)p = htonl(uniq_id_);

    out_len = total_len + 4;
  } else {
    if (msg_type_ == 3 || msg_type_ == 2) {
      --idx;
    }

    char *bodyBegin = pack_data + sizeof(ProtocolMsgHead) + sizeof(uint32_t);
    uint32_t body_len = 0;
    std::string snappy_res;
    bool isSnappy = IsZipAndOperate(snappy_res, idx);
    if (isSnappy) {
      body_len = static_cast<uint32_t>(snappy_res.size());
      memcpy(bodyBegin, snappy_res.data(), body_len);
    } else {
      body_len = idx;
      memcpy(bodyBegin, pack_buf_, body_len);
    }
    *(uint32_t *)(&(pack_data[sizeof(ProtocolMsgHead)])) = htonl(body_len);
    bodyBegin += body_len;

    // attr
    std::string attr;
    attr = group_id_key_ + msgs[0]->inlong_group_id_ + stream_id_key_ + msgs[0]->inlong_stream_id_;

    attr += "&dt=" + std::to_string(data_time_);
    attr += "&mid=" + std::to_string(uniq_id_);
    if (isSnappy)
      attr += "&cp=snappy";
    attr += "&cnt=" + std::to_string(cnt);
    attr += "&sid=" + std::string(Utils::getSnowflakeId());

    *(uint32_t *)bodyBegin = htonl(attr.size());
    bodyBegin += sizeof(uint32_t);
    memcpy(bodyBegin, attr.data(), attr.size());

    // total_len
    uint32_t total_len = 1 + 4 + body_len + 4 + attr.size();
    *(uint32_t *)pack_data = htonl(total_len);
    // msg_type
    *(&pack_data[4]) = msg_type_;
    out_len = total_len + 4;
  }
  return true;
}

bool RecvGroup::IsZipAndOperate(std::string &res, uint32_t real_cur_len) {
  if (SdkConfig::getInstance()->enable_zip_ &&
      real_cur_len > SdkConfig::getInstance()->min_zip_len_) {
    Utils::zipData(pack_buf_, real_cur_len, res);
    return true;
  } else
    return false;
}

void RecvGroup::DispatchMsg(bool exit) {
  if (cur_len_ <= constants::ATTR_LENGTH) return;
  bool len_enough = cur_len_ > SdkConfig::getInstance()->pack_size_;
  bool time_enough = (Utils::getCurrentMsTime() - last_pack_time_) > SdkConfig::getInstance()->pack_timeout_;
  if (len_enough || time_enough) {
    DoDispatchMsg();
  }
}
std::shared_ptr<SendBuffer> RecvGroup::BuildSendBuf(std::vector<SdkMsgPtr> &msgs) {
  if (msgs.empty()) {
    return nullptr;
  }
  std::shared_ptr<SendBuffer> send_buffer = BufferManager::GetInstance()->GetSendBuffer();
  if (send_buffer == nullptr) {
    return nullptr;
  }

  uint32_t len = 0;
  uint32_t msg_cnt = msgs.size();
  if (++uniq_id_ >= constants::kMaxSnowFlake) {
    uniq_id_ = 0;
  }

  if (!PackMsg(msgs, send_buffer->GetData(), len) || len == 0) {
    LOG_ERROR("failed to write data to send buf from pack queue, sendQueue");
    return nullptr;
  }

  send_buffer->SetDataLen(len);
  send_buffer->SetMsgCnt(msg_cnt);
  send_buffer->SetInlongGroupId(msgs[0]->inlong_group_id_);
  send_buffer->SetInlongStreamId(msgs[0]->inlong_stream_id_);
  for (const auto &it : msgs) {
    if(it->cb_){
      send_buffer->addUserMsg(it);
    }
  }

  return send_buffer;
}

uint32_t RecvGroup::ParseMsg(std::vector<SdkMsgPtr> &msg_vec) {
  if (msg_vec.empty()) {
    return SdkCode::kSuccess;
  }

  std::shared_ptr<SendBuffer> send_buffer = BuildSendBuf(msg_vec);

  if (send_buffer == nullptr) {
    return SdkCode::kBufferManagerFull;
  }

  uint32_t ret = send_group_->PushData(send_buffer);
  if (ret != SdkCode::kSuccess) {
    fail_queue_.push(send_buffer);
  }
  return ret;
}
bool RecvGroup::CanDispatch() {
  if (group_key_.empty()) {
    if (log_stat_++ > LOG_SAMPLE) {
      LOG_ERROR("Group key is empty!");
      log_stat_ = 0;
    }
    return false;
  }
  if (nullptr == send_group_) {
    send_group_ = send_manager_->GetSendGroup(group_key_);
    if (log_stat_++ > LOG_SAMPLE) {
      LOG_ERROR("failed to get send group! group_key:" << group_key_);
      log_stat_ = 0;
    }
    return false;
  }
  if (!send_group_->IsAvailable()) {
    if (log_stat_++ > LOG_SAMPLE) {
      LOG_ERROR("failed to get send group! group_key:" << group_key_ << " is not available!");
      log_stat_ = 0;
    }
    return false;
  }
  if (send_group_->IsFull()) {
    if (log_stat_++ > LOG_SAMPLE) {
      LOG_ERROR("failed to get send group! group_key:" << group_key_ << " is full!");
      log_stat_ = 0;
    }
    return false;
  }
  return true;
}
void RecvGroup::UpdateCurrentMsgLen(uint64_t msg_size) {
  std::lock_guard<std::mutex> lck(mutex_);
  cur_len_ = cur_len_ - msg_size;
}
}  // namespace inlong