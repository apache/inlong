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

namespace sdk {
class SdkConfig {
private:
  static SdkConfig *instance_;
  std::string config_path_;
  std::mutex mutex_;
  void InitThreadParam(const rapidjson::Value &doc);
  void InitCacheParam(const rapidjson::Value &doc);
  void InitZipParam(const rapidjson::Value &doc);
  void InitLogParam(const rapidjson::Value &doc);
  void InitBusParam(const rapidjson::Value &doc);
  void InitTcpParam(const rapidjson::Value &doc);
  void OthersParam(const rapidjson::Value &doc);

public:
  // cache parameter
  std::vector<std::string> group_ids_; // Initialize the inlong groupid collection
  uint32_t recv_buf_size_;        // Receive buf size, tid granularity
  uint32_t send_buf_size_;        // Send buf size, bid granularity

  // thread parameters
  uint32_t per_groupid_thread_nums_;    // Sending thread per groupid
  uint32_t dispatch_interval_zip_;  // Compression thread distribution interval
  uint32_t dispatch_interval_send_; // sending thread sending interval

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
  bool enable_manager_url_from_cluster_;
  std::string manager_cluster_url_;
  uint32_t manager_update_interval_; // Automatic update interval, minutes
  uint32_t manager_url_timeout_;     // URL parsing timeout, seconds
  uint32_t max_tcp_num_;
  uint32_t msg_type_;

  // Network parameters
  bool enable_tcp_nagle_;
  uint64_t tcp_idle_time_;          // The time when tcpclient did not send data
  uint32_t tcp_detection_interval_; // tcp-client detection interval

  // Other parameters
  std::string local_ip_;        // local ip
  uint32_t local_port_ = 46801; // local port

  bool enable_setaffinity_;    // cpu bound core
  uint32_t mask_cpu_affinity_; // cpu tied core mask
  uint16_t extend_field_;

  uint32_t buf_size_;

  volatile bool parsed_ = false;

  void defaultInit();
  static SdkConfig *getInstance();

  SdkConfig() { defaultInit(); }

  bool ParseConfig(const std::string &config_path);
  void ShowClientConfig();

  inline bool enableChar() const {
    return (((extend_field_)&0x4) >> 2);
  }
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
} // namespace busapi

#endif // CAPI_BASE_CLIENT_CONFIG_H_