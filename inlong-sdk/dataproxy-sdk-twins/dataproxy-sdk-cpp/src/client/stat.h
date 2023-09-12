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

#ifndef INLONG_SDK_STAT_H
#define INLONG_SDK_STAT_H

#include <cstdint>
class Stat {
private:
  uint64_t send_success_pack_num_;
  uint64_t send_success_msg_num_;

  uint64_t send_failed_pack_num_;
  uint64_t send_failed_msg_num_;

public:
  Stat()
      : send_success_pack_num_(0), send_success_msg_num_(0),
        send_failed_pack_num_(0), send_failed_msg_num_(0) {}

  void AddSendSuccessPackNum(uint64_t num) { send_success_pack_num_ += num; }
  void AddSendSuccessMsgNum(uint64_t num) { send_success_msg_num_ += num; }
  void AddSendFailPackNum(uint64_t num) { send_failed_pack_num_ += num; }
  void AddSendFailMsgNum(uint64_t num) { send_failed_msg_num_ += num; }

  void ResetStat() {
    send_success_pack_num_ = 0;
    send_success_msg_num_ = 0;
    send_failed_pack_num_ = 0;
    send_failed_msg_num_ = 0;
  }
  std::string ToString() {
    std::stringstream stat;
    stat << "success-pack[" << send_success_pack_num_ << "]";
    stat << " success-msg[" << send_success_msg_num_ << "]";
    stat << " failed-pack[" << send_failed_pack_num_ << "]";
    stat << " failed-msg[" << send_failed_msg_num_ << "]";
    return stat.str();
  }
};
#endif // INLONG_SDK_STAT_H
