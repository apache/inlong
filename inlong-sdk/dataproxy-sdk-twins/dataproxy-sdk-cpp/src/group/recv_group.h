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

#ifndef INLONG_SDK_RECV_GROUP_H
#define INLONG_SDK_RECV_GROUP_H

#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "../config/sdk_conf.h"
#include "../core/sdk_msg.h"
#include "../manager/send_manager.h"
#include "../utils/atomic.h"
#include "../utils/noncopyable.h"

namespace inlong {
class RecvGroup {
 private:
  char* pack_buf_;
  std::queue<SdkMsgPtr> msgs_;
  std::queue<SdkMsgPtr> fail_msgs_;
  uint32_t data_capacity_;
  uint64_t cur_len_;
  AtomicInt pack_err_;
  uint64_t data_time_;
  uint16_t groupId_num_;
  uint16_t streamId_num_;
  uint32_t msg_type_;
  mutable std::mutex mutex_;

  std::shared_ptr<SendManager> send_manager_;
  uint64_t last_pack_time_;

  uint64_t max_recv_size_;

  std::string group_key_;
  uint64_t log_stat_;
  SendGroupPtr send_group_;
  std::string local_ip_;
  uint64_t max_msg_size_;
  uint64_t uniq_id_;

  std::unordered_map<std::string, std::queue<SdkMsgPtr>> recv_queue_;
  std::unordered_map<std::string, std::queue<SdkMsgPtr>> dispatch_queue_;
  std::queue<SendBufferPtrT> fail_queue_;

  std::string group_id_key_;
  std::string stream_id_key_;

  void DoDispatchMsg();
  bool IsZipAndOperate(std::string& res, uint32_t real_cur_len);
  inline void ResetPackBuf() { memset(pack_buf_, 0x0, data_capacity_); }
  uint32_t ParseMsg(std::vector<SdkMsgPtr>& msg_vec);

 public:
  RecvGroup(const std::string& group_key, std::shared_ptr<SendManager> send_manager);
  ~RecvGroup();

  int32_t SendData(const std::string& msg, const std::string& inlong_group_id_, const std::string& inlong_stream_id_, uint64_t report_time,
                   UserCallBack call_back);
  bool PackMsg(std::vector<SdkMsgPtr>& msgs, char* pack_data, uint32_t& out_len);
  void DispatchMsg(bool exit);
  std::shared_ptr<SendBuffer> BuildSendBuf(std::vector<SdkMsgPtr>& msgs);
  bool CanDispatch();
  void UpdateCurrentMsgLen(uint64_t msg_size);
};
using RecvGroupPtr = std::shared_ptr<RecvGroup>;
}  // namespace inlong

#endif  // INLONG_SDK_RECV_GROUP_H