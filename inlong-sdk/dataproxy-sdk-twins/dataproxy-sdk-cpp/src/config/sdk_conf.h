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

#ifndef CAPI_BASE_CLIENT_CONFIG_H_
#define CAPI_BASE_CLIENT_CONFIG_H_

#include <mutex>
#include <rapidjson/document.h>
#include <stdint.h>
#include <string>
#include <vector>
#include <atomic>
#include "../utils/capi_constant.h"

namespace inlong {
class SdkConfig {
private:
  std::string config_path_;
  std::mutex mutex_;
  void InitThreadParam(const rapidjson::Value &doc);
  void InitCacheParam(const rapidjson::Value &doc);
  void InitZipParam(const rapidjson::Value &doc);
  void InitLogParam(const rapidjson::Value &doc);
  void InitManagerParam(const rapidjson::Value &doc);
  void InitTcpParam(const rapidjson::Value &doc);
  void InitAuthParm(const rapidjson::Value &doc);
  void OthersParam(const rapidjson::Value &doc);
  bool GetLocalIPV4Address(std::string& err_info, std::string& localhost);
  SdkConfig():extend_report_(false) { defaultInit(); };

      public:
  // cache parameter
  std::vector<std::string>
      inlong_group_ids_;   // Initialize the inlong groupid collection
  uint32_t recv_buf_size_; // Receive buf size, tid granularity
  uint32_t send_buf_size_; // Send buf size, bid granularity
  uint32_t max_group_id_num_; // Send buf size, bid granularity
  uint32_t max_stream_id_num_; // Send buf size, bid granularity
  uint32_t max_cache_num_;
  uint32_t max_instance_;
  uint32_t instance_num_;
  bool enable_share_msg_;

  // thread parameters
  uint32_t per_groupid_thread_nums_; // Sending thread per groupid
  uint32_t dispatch_interval_zip_;   // Compression thread distribution interval
  uint32_t dispatch_interval_send_;  // sending thread sending interval
  uint32_t load_balance_interval_;
  uint32_t heart_beat_interval_;

  // Packaging parameters
  bool enable_pack_;
  uint32_t pack_size_;    // byte
  uint32_t pack_timeout_; // millisecond
  uint32_t max_msg_size_;

  // compression parameters
  bool enable_zip_;
  uint32_t min_zip_len_; // byte

  // log parameters
  uint32_t log_num_;   // number of log files
  uint32_t log_size_;  // Single log size, in MB
  uint32_t log_level_; // log level,trace(4)>debug(3)>info(2)>warn(1)>error(0)
  std::string log_path_;

  // manager parameters
  std::string manager_url_;
  uint32_t manager_update_interval_; // Automatic update interval, minutes
  uint32_t manager_url_timeout_;     // URL parsing timeout, seconds
  uint64_t max_proxy_num_;
  uint64_t reserve_proxy_num_;
  uint32_t msg_type_;
  uint32_t isolation_level_;

  // Network parameters
  bool enable_tcp_nagle_;
  uint64_t tcp_idle_time_;          // The time when tcpclient did not send data
  uint32_t tcp_detection_interval_; // tcp-client detection interval
  bool enable_balance_;
  bool enable_local_cache_;
  uint32_t retry_times_;
  uint32_t proxy_repeat_times_;

  // auth settings
  bool need_auth_;
  std::string auth_id_;
  std::string auth_key_;

  // Other parameters
  std::string local_ip_;        // local ip
  uint32_t local_port_ = 46801; // local port

  bool enable_setaffinity_;    // cpu bound core
  uint32_t mask_cpu_affinity_; // cpu tied core mask
  uint16_t extend_field_;

  uint32_t buf_size_;

  volatile bool parsed_ = false;
  bool extend_report_;

  void defaultInit();
  static SdkConfig *getInstance();

  bool ParseConfig(const std::string &config_path);
  void ShowClientConfig();

  inline bool enableChar() const { return (((extend_field_)&0x4) >> 2); }
  inline bool enableTraceIP() const { return (((extend_field_)&0x2) >> 1); }
  // data type message datlen|data
  inline bool isNormalDataPackFormat() const {
    return ((5 == msg_type_) || ((msg_type_ >= 7) && (!(extend_field_ & 0x1))));
  }
  //  data&attr message datlen|data|attr_len|attr
  inline bool isAttrDataPackFormat() const {
    return ((6 == msg_type_) || ((msg_type_ >= 7) && (extend_field_ & 0x1)));
  }
};
} // namespace inlong

#endif // CAPI_BASE_CLIENT_CONFIG_H_