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

#ifndef INLONG_METRIC_H
#define INLONG_METRIC_H

#include <cstdint>
#include <sstream>
namespace inlong {
class Metric {
 private:
  uint64_t send_success_pack_num_;
  uint64_t send_success_msg_num_;
  uint64_t send_failed_pack_num_;
  uint64_t send_failed_msg_num_;

  uint64_t time_cost_;
  uint64_t time_cost_0t32_;
  uint64_t time_cost_32t128_;
  uint64_t time_cost_128t1024_;
  uint64_t time_cost_1024t65536_;

  uint64_t receive_buffer_full_count_;
  uint64_t too_long_msg_count_;
  uint64_t metadata_fail_count_;

 public:
  Metric()
      : send_success_pack_num_(0),
        send_success_msg_num_(0),
        send_failed_pack_num_(0),
        send_failed_msg_num_(0),
        time_cost_(0),
        time_cost_0t32_(0),
        time_cost_32t128_(0),
        time_cost_128t1024_(0),
        time_cost_1024t65536_(0),
        receive_buffer_full_count_(0),
        too_long_msg_count_(0),
        metadata_fail_count_(0) {}

  void AddSendSuccessPackNum(uint64_t num) { send_success_pack_num_ += num; }
  void AddSendSuccessMsgNum(uint64_t num) { send_success_msg_num_ += num; }
  void AddSendFailPackNum(uint64_t num) { send_failed_pack_num_ += num; }
  void AddSendFailMsgNum(uint64_t num) { send_failed_msg_num_ += num; }
  void AddReceiveBufferFullCount(uint64_t receive_buffer_full_count) {
    receive_buffer_full_count_ += receive_buffer_full_count;
  }
  void AddTooLongMsgCount(uint64_t too_long_msg_count) { too_long_msg_count_ += too_long_msg_count; }
  void AddMetadataFailCount(uint64_t metadata_fail_count) { metadata_fail_count_ += metadata_fail_count; }

  uint64_t GetSendSuccessPackNum() { return send_success_pack_num_; }
  uint64_t GetSendSuccessMsgNum() { return send_success_msg_num_; }
  uint64_t GetSendFailedPackNum() { return send_failed_pack_num_; }
  uint64_t GetSendFailedMsgNum() { return send_failed_msg_num_; }
  uint64_t GetTimeCost() { return time_cost_; }
  uint64_t GetTimeCost0T32() const { return time_cost_0t32_; }
  uint64_t GetTimeCost32T128() const { return time_cost_32t128_; }
  uint64_t GetTimeCost128T1024() const { return time_cost_128t1024_; }
  uint64_t GetTimeCost1024T65536() const { return time_cost_1024t65536_; }
  uint64_t GetReceiveBufferFullCount() const { return receive_buffer_full_count_; }
  uint64_t GetTooLongMsgCount() const { return too_long_msg_count_; }
  uint64_t GetMetadataFailCount() const { return metadata_fail_count_; }

  void AddTimeCost(uint64_t time_cost) {
    time_cost_ += time_cost;
    if (time_cost < 32) {
      time_cost_0t32_++;
      return;
    } else if (time_cost < 128) {
      time_cost_32t128_++;
      return;
    } else if (time_cost < 1024) {
      time_cost_128t1024_++;
      return;
    } else {
      time_cost_1024t65536_++;
    }
  }

  void ResetStat() {
    send_success_pack_num_ = 0;
    send_success_msg_num_ = 0;
    send_failed_pack_num_ = 0;
    send_failed_msg_num_ = 0;
    time_cost_ = 0;
    time_cost_0t32_ = 0;
    time_cost_32t128_ = 0;
    time_cost_128t1024_ = 0;
    time_cost_1024t65536_ = 0;
    receive_buffer_full_count_ = 0;
    too_long_msg_count_ = 0;
    metadata_fail_count_ = 0;
  }

  void Update(Metric stat) {
    send_success_pack_num_ += stat.send_success_pack_num_;
    send_success_msg_num_ += stat.send_success_msg_num_;
    send_failed_pack_num_ += stat.send_failed_pack_num_;
    send_failed_msg_num_ += stat.send_failed_msg_num_;
    time_cost_ += stat.time_cost_;
    time_cost_0t32_ += stat.time_cost_0t32_;
    time_cost_32t128_ += stat.time_cost_32t128_;
    time_cost_128t1024_ += stat.time_cost_128t1024_;
    time_cost_1024t65536_ += stat.time_cost_1024t65536_;
  }

  uint64_t getTransTime() const {
    uint64_t pack_num = send_success_pack_num_ + send_failed_pack_num_ + 1;
    return time_cost_ / pack_num;
  }

  std::string GetSendMetricInfo() const {
    std::stringstream metric;
    metric << "success-pack[" << send_success_pack_num_ << "] ";
    metric << "msg[" << send_success_msg_num_ << "] ";
    metric << "failed-pack[" << send_failed_pack_num_ << "] ";
    metric << "msg[" << send_failed_msg_num_ << "] ";
    metric << "trans[" << getTransTime() << "]";
    return metric.str();
  }
  std::string ToString() const {
    std::stringstream metric;
    metric << "success-pack[" << send_success_pack_num_ << "] ";
    metric << "msg[" << send_success_msg_num_ << "] ";
    metric << "failed-pack[" << send_failed_pack_num_ << "] ";
    metric << "msg[" << send_failed_msg_num_ << "] ";
    metric << "trans[" << getTransTime() << "] ";
    metric << "buffer full[" << receive_buffer_full_count_ << "] ";
    metric << "too long msg[" << too_long_msg_count_ << "] ";
    metric << "metadata fail[" << metadata_fail_count_ << "]";
    return metric.str();
  }
};
}  // namespace inlong

#endif  // INLONG_METRIC_H
