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

#ifndef INLONG_SDK_RECV_GROUP_H
#define INLONG_SDK_RECV_GROUP_H

#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "../manager/send_manager.h"
#include "../utils/atomic.h"
#include "../utils/noncopyable.h"
#include "sdk_conf.h"

namespace inlong {
class RecvGroup {
private:
  char *pack_buf_;
  std::queue<SdkMsgPtr> msgs_;
  uint32_t data_capacity_;
  uint32_t cur_len_;
  AtomicInt pack_err_;
  uint64_t data_time_;
//  std::string inlong_group_id_;
//  std::string inlong_stream_id_;
  uint16_t groupId_num_;
  uint16_t streamId_num_;
 // std::string topic_desc_;
  uint32_t msg_type_;
  mutable std::mutex mutex_;

  std::shared_ptr<SendManager> send_manager_;
  uint64_t last_pack_time_;

  uint64_t max_recv_size_;
  std::string group_key_;

  int32_t DoDispatchMsg();
  void AddMsg(const std::string &msg, std::string client_ip,
              int64_t report_time, UserCallBack call_back,const std::string &groupId,
              const std::string &streamId);
  bool IsZipAndOperate(std::string &res, uint32_t real_cur_len);
  inline void ResetPackBuf() { memset(pack_buf_, 0x0, data_capacity_); }

public:
  RecvGroup(const std::string &group_key,std::shared_ptr<SendManager> send_manager);
  ~RecvGroup();

  int32_t SendData(const std::string &msg, const std::string &groupId,
                   const std::string &streamId, const std::string &client_ip,
                   uint64_t report_time, UserCallBack call_back);
  bool PackMsg(std::vector<SdkMsgPtr> &msgs, char *pack_data, uint32_t &out_len,
               uint32_t uniq_id);
  void DispatchMsg(bool exit);

  char *data() const { return pack_buf_; }

  std::shared_ptr<SendBuffer> BuildSendBuf(std::vector<SdkMsgPtr> &msgs);
  void CallbalkToUsr(std::vector<SdkMsgPtr> &msgs);
};
using RecvGroupPtr = std::shared_ptr<RecvGroup>;
} // namespace inlong

#endif // INLONG_SDK_RECV_GROUP_H