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

#include "sdk_conf.h"
#include <rapidjson/document.h>

#include "../utils/capi_constant.h"
#include "../utils/logger.h"
#include "../utils/utils.h"

namespace sdk {
SdkConfig *SdkConfig::instance_ = new SdkConfig();
SdkConfig *SdkConfig::getInstance() { return SdkConfig::instance_; }

bool SdkConfig::ParseConfig(const std::string &config_path) {
  // Ensure the data consistency of each sdk instance
  std::lock_guard<std::mutex> lock(mutex_);

  // Guaranteed to only parse the configuration file once
  if (!__sync_bool_compare_and_swap(&parsed_, false, true)) {
    LOG_INFO("ParseConfig has  parsed .");
    return true;
  }

  config_path_ = config_path;

  std::string file_content;
  if (!Utils::readFile(config_path_, file_content)) {
    return false;
  }

  rapidjson::Document root;

  if (root.Parse(file_content.c_str()).HasParseError()) {
    return false;
  }

  if (!root.HasMember("init-param")) {
    return false;
  }
  const rapidjson::Value &doc = root["init-param"];

  InitThreadParam(doc);
  InitCacheParam(doc);
  InitZipParam(doc);
  InitLogParam(doc);
  InitBusParam(doc);
  InitTcpParam(doc);
  OthersParam(doc);

  sdk::initLog4cplus();

  ShowClientConfig();

  return true;
}

void SdkConfig::defaultInit() {
  per_groupid_thread_nums_ = constants::kPerBidThreadNums;
  dispatch_interval_send_ = constants::kDispatchIntervalSend;
  dispatch_interval_zip_ = constants::kDispatchIntervalZip;
  tcp_detection_interval_ = constants::kTcpDetectionInterval;
  tcp_idle_time_ = constants::kTcpIdleTime;

  // cache parameter
  send_buf_size_ = constants::kSendBufSize;
  recv_buf_size_ = constants::kRecvBufSize;
  max_msg_size_ = constants::kExtPackSize;

  // Packaging parameters
  enable_pack_ = constants::kEnablePack;
  pack_size_ = constants::kPackSize;
  pack_timeout_ = constants::kPackTimeout;
  enable_zip_ = constants::kEnableZip;
  min_zip_len_ = constants::kMinZipLen;

  // log parameters
  log_num_ = constants::kLogNum;
  log_size_ = constants::kLogSize;
  log_level_ = constants::kLogLevel;
  log_path_ = constants::kLogPath;

  // manager parameters
  manager_url_ = constants::kBusURL;
  enable_manager_url_from_cluster_ = constants::kEnableBusURLFromCluster;
  manager_cluster_url_ = constants::kBusClusterURL;
  manager_update_interval_ = constants::kBusUpdateInterval;
  manager_url_timeout_ = constants::kBusURLTimeout;
  max_tcp_num_ = constants::kMaxBusNum;

  local_ip_ = constants::kSerIP;
  local_port_ = constants::kSerPort;
  msg_type_ = constants::kMsgType;
  enable_tcp_nagle_ = constants::kEnableTCPNagle;

  enable_setaffinity_ = constants::kEnableSetAffinity;
  mask_cpu_affinity_ = constants::kMaskCPUAffinity;
  extend_field_ = constants::kExtendField;
}

void SdkConfig::InitThreadParam(const rapidjson::Value &doc) {
  if (doc.HasMember("per_groupid_thread_nums") &&
      doc["per_groupid_thread_nums"].IsInt() &&
      doc["per_groupid_thread_nums"].GetInt() > 0) {
    const rapidjson::Value &obj = doc["per_groupid_thread_nums"];
    per_groupid_thread_nums_ = obj.GetInt();
  } else {
    per_groupid_thread_nums_ = constants::kPerBidThreadNums;
  }
  if (doc.HasMember("dispatch_interval_zip") &&
      doc["dispatch_interval_zip"].IsInt() &&
      doc["dispatch_interval_zip"].GetInt() > 0) {
    const rapidjson::Value &obj = doc["dispatch_interval_zip"];
    dispatch_interval_zip_ = obj.GetInt();
  } else {
    dispatch_interval_zip_ = constants::kDispatchIntervalZip;
  }

  if (doc.HasMember("dispatch_interval_send") &&
      doc["dispatch_interval_send"].IsInt() &&
      doc["dispatch_interval_send"].GetInt() > 0) {
    const rapidjson::Value &obj = doc["dispatch_interval_send"];
    dispatch_interval_send_ = obj.GetInt();
  } else {
    dispatch_interval_send_ = constants::kDispatchIntervalSend;
  }
}

void SdkConfig::InitCacheParam(const rapidjson::Value &doc) {
  if (doc.HasMember("recv_buf_size") && doc["recv_buf_size"].IsInt() &&
      doc["recv_buf_size"].GetInt() > 0) {
    const rapidjson::Value &obj = doc["recv_buf_size"];
    recv_buf_size_ = obj.GetInt();
  } else {
    recv_buf_size_ = constants::kRecvBufSize;
  }

  if (doc.HasMember("send_buf_size") && doc["send_buf_size"].IsInt() &&
      doc["send_buf_size"].GetInt() > 0) {
    const rapidjson::Value &obj = doc["send_buf_size"];
    send_buf_size_ = obj.GetInt();
  } else {
    send_buf_size_ = constants::kSendBufSize;
  }
}

void SdkConfig::InitZipParam(const rapidjson::Value &doc) {
  // enable_pack
  if (doc.HasMember("enable_pack") && doc["enable_pack"].IsBool()) {
    const rapidjson::Value &obj = doc["enable_pack"];
    enable_pack_ = obj.GetBool();
  } else {
    enable_pack_ = constants::kEnablePack;
  }
  // pack_size
  if (doc.HasMember("pack_size") && doc["pack_size"].IsInt() &&
      doc["pack_size"].GetInt() > 0) {
    const rapidjson::Value &obj = doc["pack_size"];
    pack_size_ = obj.GetInt();
  } else {
    pack_size_ = constants::kPackSize;
  }
  // pack_timeout
  if (doc.HasMember("pack_timeout") && doc["pack_timeout"].IsInt() &&
      doc["pack_timeout"].GetInt() > 0) {
    const rapidjson::Value &obj = doc["pack_timeout"];
    pack_timeout_ = obj.GetInt();
  } else {
    pack_timeout_ = constants::kPackTimeout;
  }
  // ext_pack_size
  if (doc.HasMember("ext_pack_size") && doc["ext_pack_size"].IsInt() &&
      doc["ext_pack_size"].GetInt() > 0) {
    const rapidjson::Value &obj = doc["ext_pack_size"];
    max_msg_size_ = obj.GetInt();
  } else {
    max_msg_size_ = constants::kExtPackSize;
  }
  // enable_zip
  if (doc.HasMember("enable_zip") && doc["enable_zip"].IsBool()) {
    const rapidjson::Value &obj = doc["enable_zip"];
    enable_zip_ = obj.GetBool();
  } else {
    enable_zip_ = constants::kEnablePack;
  }
  // min_zip_len
  if (doc.HasMember("min_zip_len") && doc["min_zip_len"].IsInt() &&
      doc["min_zip_len"].GetInt() > 0) {
    const rapidjson::Value &obj = doc["min_zip_len"];
    min_zip_len_ = obj.GetInt();
  } else {
    min_zip_len_ = constants::kMinZipLen;
  }
}
void SdkConfig::InitLogParam(const rapidjson::Value &doc) {
  // log_num
  if (doc.HasMember("log_num") && doc["log_num"].IsInt() &&
      doc["log_num"].GetInt() > 0) {
    const rapidjson::Value &obj = doc["log_num"];
    log_num_ = obj.GetInt();
  } else {
    log_num_ = constants::kLogNum;
  }
  // log_size
  if (doc.HasMember("log_size") && doc["log_size"].IsInt() &&
      doc["log_size"].GetInt() > 0) {
    const rapidjson::Value &obj = doc["log_size"];
    log_size_ = obj.GetInt();
  } else {
    log_size_ = constants::kLogSize;
  }
  // log_level
  if (doc.HasMember("log_level") && doc["log_level"].IsInt() &&
      doc["log_level"].GetInt() >= 0 && doc["log_level"].GetInt() <= 4) {
    const rapidjson::Value &obj = doc["log_level"];
    log_level_ = obj.GetInt();
  } else {
    log_level_ = constants::kLogLevel;
  }

  // log_path
  if (doc.HasMember("log_path") && doc["log_path"].IsString()) {
    const rapidjson::Value &obj = doc["log_path"];
    log_path_ = obj.GetString();
  } else {
    log_path_ = constants::kLogPath;
  }
}
void SdkConfig::InitBusParam(const rapidjson::Value &doc) {
  // bus_URL
  if (doc.HasMember("manager_url") && doc["manager_url"].IsString()) {
    const rapidjson::Value &obj = doc["manager_url"];
    manager_url_ = obj.GetString();
  } else {
    manager_url_ = constants::kBusURL;
  }
  // bus_cluster_URL_
  if (doc.HasMember("manager_cluster_url") &&
      doc["manager_cluster_url"].IsString()) {
    const rapidjson::Value &obj = doc["manager_cluster_url"];
    manager_cluster_url_ = obj.GetString();
  } else {
    manager_cluster_url_ = constants::kBusClusterURL;
  }
  // enable_bus_URL_from_cluster
  if (doc.HasMember("enable_manager_url_from_cluster") &&
      doc["enable_manager_url_from_cluster"].IsBool()) {
    const rapidjson::Value &obj = doc["enable_manager_url_from_cluster"];
    enable_manager_url_from_cluster_ = obj.GetBool();
  } else {
    enable_manager_url_from_cluster_ = constants::kEnableBusURLFromCluster;
  }
  // bus_update_interval
  if (doc.HasMember("manager_update_interval") &&
      doc["manager_update_interval"].IsInt() &&
      doc["manager_update_interval"].GetInt() > 0) {
    const rapidjson::Value &obj = doc["manager_update_interval"];
    manager_update_interval_ = obj.GetInt();
  } else {
    manager_update_interval_ = constants::kBusUpdateInterval;
  }
  // bus_URL_timeout
  if (doc.HasMember("manager_url_timeout") &&
      doc["manager_url_timeout"].IsInt() &&
      doc["manager_url_timeout"].GetInt() > 0) {
    const rapidjson::Value &obj = doc["manager_url_timeout"];
    manager_url_timeout_ = obj.GetInt();
  } else {
    manager_url_timeout_ = constants::kBusURLTimeout;
  }
  // msg_type
  if (doc.HasMember("msg_type") && doc["msg_type"].IsInt() &&
      doc["msg_type"].GetInt() > 0 && doc["msg_type"].GetInt() < 9) {
    const rapidjson::Value &obj = doc["msg_type"];
    msg_type_ = obj.GetInt();
  } else {
    msg_type_ = constants::kMsgType;
  }
  // max_active_bus_num
  if (doc.HasMember("max_tcp_num") && doc["max_tcp_num"].IsInt() &&
      doc["max_tcp_num"].GetInt() > 0) {
    const rapidjson::Value &obj = doc["max_tcp_num"];
    max_tcp_num_ = obj.GetInt();
  } else {
    max_tcp_num_ = constants::kMaxBusNum;
  }

  if (doc.HasMember("group_ids") && doc["group_ids"].IsString()) {
    const rapidjson::Value &obj = doc["group_ids"];
    std::string bids_str = obj.GetString();
    int32_t size = Utils::splitOperate(bids_str, group_ids_, ",");
  } else {
  }
}
void SdkConfig::InitTcpParam(const rapidjson::Value &doc) {
  if (doc.HasMember("tcp_detection_interval") &&
      doc["tcp_detection_interval"].IsInt() &&
      doc["tcp_detection_interval"].GetInt() > 0) {
    const rapidjson::Value &obj = doc["tcp_detection_interval"];
    tcp_detection_interval_ = obj.GetInt();
  } else {
    tcp_detection_interval_ = constants::kTcpDetectionInterval;
  }
  // enable_TCP_nagle
  if (doc.HasMember("enable_tcp_nagle") && doc["enable_tcp_nagle"].IsBool()) {
    const rapidjson::Value &obj = doc["enable_tcp_nagle"];
    enable_tcp_nagle_ = obj.GetBool();
  } else {
    enable_tcp_nagle_ = constants::kEnableTCPNagle;
  }

  // tcp_idle_time
  if (doc.HasMember("tcp_idle_time") && doc["tcp_idle_time"].IsInt() &&
      doc["tcp_idle_time"].GetInt() > 0) {
    const rapidjson::Value &obj = doc["tcp_idle_time"];
    tcp_idle_time_ = obj.GetInt();
  } else {
    tcp_idle_time_ = constants::kTcpIdleTime;
  }
}

void SdkConfig::OthersParam(const rapidjson::Value &doc) {
  // ser_ip
  if (doc.HasMember("ser_ip") && doc["ser_ip"].IsString()) {
    const rapidjson::Value &obj = doc["ser_ip"];
    local_ip_ = obj.GetString();
  } else {
    local_ip_ = constants::kSerIP;
  }
  // ser_port
  if (doc.HasMember("ser_port") && doc["ser_port"].IsInt() &&
      doc["ser_port"].GetInt() > 0) {
    const rapidjson::Value &obj = doc["ser_port"];
    local_port_ = obj.GetInt();
  } else {
    local_port_ = constants::kSerPort;
  }

  // enable_setaffinity
  if (doc.HasMember("enable_setaffinity") &&
      doc["enable_setaffinity"].IsBool()) {
    const rapidjson::Value &obj = doc["enable_setaffinity"];
    enable_setaffinity_ = obj.GetBool();
  } else {
    enable_setaffinity_ = constants::kEnableSetAffinity;
  }
  // mask_cpu_affinity
  if (doc.HasMember("mask_cpuaffinity") && doc["mask_cpuaffinity"].IsInt() &&
      doc["mask_cpuaffinity"].GetInt() > 0) {
    const rapidjson::Value &obj = doc["mask_cpuaffinity"];
    mask_cpu_affinity_ = obj.GetInt();
  } else {
    mask_cpu_affinity_ = constants::kMaskCPUAffinity;
  }

  // extend_field
  if (doc.HasMember("extend_field") && doc["extend_field"].IsInt() &&
      doc["extend_field"].GetInt() > 0) {
    const rapidjson::Value &obj = doc["extend_field"];
    extend_field_ = obj.GetInt();
  } else {
    extend_field_ = constants::kExtendField;
  }
}

void SdkConfig::ShowClientConfig() {
  LOG_INFO("per_groupid_thread_nums: " << per_groupid_thread_nums_);
  LOG_INFO("group_ids: " << Utils::getVectorStr(group_ids_).c_str());
  LOG_INFO("buffer_num_per_bid: " << send_buf_size_);
  LOG_INFO("local_ip: " << local_ip_.c_str());
  LOG_INFO("local_port: " << local_port_);
  LOG_INFO("enable_pack: " << enable_pack_ ? "true" : "false");
  LOG_INFO("pack_size: " << pack_size_);
  LOG_INFO("pack_timeout: " << pack_timeout_);
  LOG_INFO("ext_pack_size: " << max_msg_size_);
  LOG_INFO("enable_zip: " << enable_zip_ ? "true" : "false");
  LOG_INFO("min_zip_len: " << min_zip_len_);
  LOG_INFO("log_num: " << log_num_);
  LOG_INFO("log_size: " << log_size_);
  LOG_INFO("log_level: " << log_level_);
  LOG_INFO("log_path: " << log_path_.c_str());
  LOG_INFO("manager_url: " << manager_url_.c_str());
  LOG_INFO("manager_cluster_url: " << manager_cluster_url_.c_str());
  LOG_INFO(
      "enable_manager_url_from_cluster: " << enable_manager_url_from_cluster_
          ? "true"
          : "false");
  LOG_INFO("manager_update_interval:  minutes" << manager_update_interval_);
  LOG_INFO("manager_url_timeout: " << manager_url_timeout_);
  LOG_INFO("max_tcp_num: " << max_tcp_num_);
  LOG_INFO("msg_type: " << msg_type_);
  LOG_INFO("enable_tcp_nagle: " << enable_tcp_nagle_ ? "true" : "false");
  LOG_INFO("enable_setaffinity: " << enable_setaffinity_ ? "true" : "false");
  LOG_INFO("mask_cpuaffinity: " << mask_cpu_affinity_);
  LOG_INFO("extend_field: " << extend_field_);
  LOG_INFO("tcp_idle_time: " << tcp_idle_time_);
  LOG_INFO("tcp_detection_interval_: " << tcp_detection_interval_);
  LOG_INFO("tcp_idle_time_: " << tcp_idle_time_);
  LOG_INFO("send_buf_size_: " << send_buf_size_);
  LOG_INFO("recv_buf_size_: " << recv_buf_size_);
}
} // namespace sdk
