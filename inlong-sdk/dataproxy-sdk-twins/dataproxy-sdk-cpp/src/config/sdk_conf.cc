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
#include <net/if.h>
#include <arpa/inet.h>
#include <sys/ioctl.h>
#include <rapidjson/document.h>

#include "../utils/capi_constant.h"
#include "../utils/logger.h"
#include "../utils/utils.h"

namespace inlong {
SdkConfig *SdkConfig::getInstance() {
  static SdkConfig instance;
  return &instance;
}

bool SdkConfig::ParseConfig(const std::string &config_path) {
  // Ensure the data consistency of each sdk instance
  std::lock_guard<std::mutex> lock(mutex_);

  // Guaranteed to only parse the configuration file once
  if (!__sync_bool_compare_and_swap(&parsed_, false, true)) {
    LOG_INFO("ParseConfig has  parsed .");
    if (++instance_num_ > max_instance_) {
      return false;
    }
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
  InitManagerParam(doc);
  InitTcpParam(doc);

  std::string err, local_ip;
  if (GetLocalIPV4Address(err, local_ip)) {
    local_ip_ = local_ip;
  } else {
    local_ip_ = constants::kSerIP;
  }

  OthersParam(doc);
  InitAuthParm(doc);

  inlong::initLog4cplus();

  ShowClientConfig();

  return true;
}

void SdkConfig::defaultInit() {
  per_groupid_thread_nums_ = constants::kPerGroupidThreadNums;
  dispatch_interval_send_ = constants::kDispatchIntervalSend;
  dispatch_interval_zip_ = constants::kDispatchIntervalZip;
  tcp_detection_interval_ = constants::kTcpDetectionInterval;
  tcp_idle_time_ = constants::kTcpIdleTime;
  load_balance_interval_ = constants::kLoadBalanceInterval;
  heart_beat_interval_ = constants::kHeartBeatInterval;
  enable_balance_ = constants::kEnableBalance;
  isolation_level_ = constants::IsolationLevel::kLevelSecond;

  // cache parameter
  send_buf_size_ = constants::kSendBufSize;
  recv_buf_size_ = constants::kRecvBufSize;
  max_msg_size_ = constants::kExtPackSize;
  max_group_id_num_ = constants::kMaxGroupIdNum;
  max_stream_id_num_ = constants::kMaxStreamIdNum;

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
  manager_url_ = constants::kManagerURL;
  manager_update_interval_ = constants::kManagerUpdateInterval;
  manager_url_timeout_ = constants::kManagerTimeout;
  max_proxy_num_ = constants::kMaxProxyNum;
  reserve_proxy_num_ = constants::kReserveProxyNum;
  enable_local_cache_ = constants::kEnableLocalCache;

  local_ip_ = constants::kSerIP;
  local_port_ = constants::kSerPort;
  msg_type_ = constants::kMsgType;
  enable_tcp_nagle_ = constants::kEnableTCPNagle;

  enable_setaffinity_ = constants::kEnableSetAffinity;
  mask_cpu_affinity_ = constants::kMaskCPUAffinity;
  extend_field_ = constants::kExtendField;

  need_auth_ = constants::kNeedAuth;
  max_instance_ = constants::kMaxInstance;
  instance_num_ = 1;
  enable_share_msg_ = constants::kEnableShareMsg;
}

void SdkConfig::InitThreadParam(const rapidjson::Value &doc) {
  if (doc.HasMember("per_groupid_thread_nums") &&
      doc["per_groupid_thread_nums"].IsInt() &&
      doc["per_groupid_thread_nums"].GetInt() > 0) {
    const rapidjson::Value &obj = doc["per_groupid_thread_nums"];
    per_groupid_thread_nums_ = obj.GetInt();
  } else {
    per_groupid_thread_nums_ = constants::kPerGroupidThreadNums;
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

  if (doc.HasMember("load_balance_interval") &&
      doc["load_balance_interval"].IsInt() &&
      doc["load_balance_interval"].GetInt() > 0) {
    const rapidjson::Value &obj = doc["load_balance_interval"];
    load_balance_interval_ = obj.GetInt();
  } else {
    load_balance_interval_ = constants::kLoadBalanceInterval;
  }

  if (doc.HasMember("heart_beat_interval") &&
      doc["heart_beat_interval"].IsInt() &&
      doc["heart_beat_interval"].GetInt() > 0) {
    const rapidjson::Value &obj = doc["heart_beat_interval"];
    heart_beat_interval_ = obj.GetInt();
  } else {
    heart_beat_interval_ = constants::kHeartBeatInterval;
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

  if (doc.HasMember("max_group_id_num") && doc["max_group_id_num"].IsInt() &&
      doc["max_group_id_num"].GetInt() > 0) {
    const rapidjson::Value &obj = doc["max_group_id_num"];
    max_group_id_num_ = obj.GetInt();
  } else {
    max_group_id_num_ = constants::kMaxGroupIdNum;
  }

  if (doc.HasMember("max_stream_id_num") && doc["max_stream_id_num"].IsInt() &&
      doc["max_stream_id_num"].GetInt() > 0) {
    const rapidjson::Value &obj = doc["max_stream_id_num"];
    max_stream_id_num_ = obj.GetInt();
  } else {
    max_stream_id_num_ = constants::kMaxGroupIdNum;
  }

  // max_cache_num
  if (doc.HasMember("max_cache_num") && doc["max_cache_num"].IsInt() && doc["max_cache_num"].GetInt() >= 0) {
    const rapidjson::Value &obj = doc["max_cache_num"];
    max_cache_num_ = obj.GetInt();
  } else {
    max_cache_num_ = constants::kMaxCacheNum;
  }

  if (doc.HasMember("enable_share_msg") && doc["enable_share_msg"].IsBool()) {
    const rapidjson::Value &obj = doc["enable_share_msg"];
    enable_share_msg_ = obj.GetBool();
  } else {
    enable_share_msg_ = constants::kEnableShareMsg;
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
void SdkConfig::InitManagerParam(const rapidjson::Value &doc) {
  // manager url
  if (doc.HasMember("manager_url") && doc["manager_url"].IsString()) {
    const rapidjson::Value &obj = doc["manager_url"];
    manager_url_ = obj.GetString();
  } else {
    manager_url_ = constants::kManagerURL;
  }

  // manager update interval
  if (doc.HasMember("manager_update_interval") &&
      doc["manager_update_interval"].IsInt() &&
      doc["manager_update_interval"].GetInt() > 0) {
    const rapidjson::Value &obj = doc["manager_update_interval"];
    manager_update_interval_ = obj.GetInt();
  } else {
    manager_update_interval_ = constants::kManagerUpdateInterval;
  }
  // manager timeout
  if (doc.HasMember("manager_url_timeout") &&
      doc["manager_url_timeout"].IsInt() &&
      doc["manager_url_timeout"].GetInt() > 0) {
    const rapidjson::Value &obj = doc["manager_url_timeout"];
    manager_url_timeout_ = obj.GetInt();
  } else {
    manager_url_timeout_ = constants::kManagerTimeout;
  }
  // msg type
  if (doc.HasMember("msg_type") && doc["msg_type"].IsInt() &&
      doc["msg_type"].GetInt() > 0 && doc["msg_type"].GetInt() < 9) {
    const rapidjson::Value &obj = doc["msg_type"];
    msg_type_ = obj.GetInt();
  } else {
    msg_type_ = constants::kMsgType;
  }
  // max proxy num
  if (doc.HasMember("max_proxy_num") && doc["max_proxy_num"].IsInt() &&
      doc["max_proxy_num"].GetInt() > 0) {
    const rapidjson::Value &obj = doc["max_proxy_num"];
    max_proxy_num_ = obj.GetInt();
  } else {
    max_proxy_num_ = constants::kMaxProxyNum;
  }

  if (doc.HasMember("inlong_group_ids") && doc["inlong_group_ids"].IsString()) {
    const rapidjson::Value &obj = doc["inlong_group_ids"];
    std::string inlong_group_ids_str = obj.GetString();
    Utils::splitOperate(inlong_group_ids_str, inlong_group_ids_, ",");
  }

  // enable local cache
  if (doc.HasMember("enable_local_cache") && doc["enable_local_cache"].IsBool()) {
    const rapidjson::Value &obj = doc["enable_local_cache"];
    enable_local_cache_ = obj.GetBool();
  } else {
    enable_local_cache_ = constants::kEnableLocalCache;
  }

  // isolation level
  if (doc.HasMember("isolation_level") && doc["isolation_level"].IsInt() && doc["isolation_level"].GetInt() > 0) {
    const rapidjson::Value &obj = doc["isolation_level"];
    isolation_level_ = obj.GetInt();
  } else {
    isolation_level_ = constants::IsolationLevel::kLevelSecond;
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

  // enable balance
  if (doc.HasMember("enable_balance") && doc["enable_balance"].IsBool()) {
    const rapidjson::Value &obj = doc["enable_balance"];
    enable_balance_ = obj.GetBool();
  } else {
    enable_balance_ = constants::kEnableBalance;
  }

  if (doc.HasMember("retry_times") && doc["retry_times"].IsInt() && doc["retry_times"].GetInt() > 0) {
    const rapidjson::Value &obj = doc["retry_times"];
    retry_times_ = obj.GetInt();
  } else {
    retry_times_ = constants::kRetryTimes;
  }
  if (doc.HasMember("proxy_repeat_times") && doc["proxy_repeat_times"].IsInt() && doc["proxy_repeat_times"].GetInt() >= 0) {
    const rapidjson::Value &obj = doc["proxy_repeat_times"];
    proxy_repeat_times_ = obj.GetInt();
  } else {
    proxy_repeat_times_ = constants::kProxyRepeatTimes;
  }
}
void SdkConfig::InitAuthParm(const rapidjson::Value &doc) {
  // auth settings
  if (doc.HasMember("need_auth") && doc["need_auth"].IsBool() &&
      doc["need_auth"].GetBool()) {
    if (!doc.HasMember("auth_id") || !doc.HasMember("auth_key") ||
        !doc["auth_id"].IsString() || !doc["auth_key"].IsString() ||
        doc["auth_id"].IsNull() || doc["auth_key"].IsNull()) {
      LOG_ERROR("need_auth, but auth_id or auth_key is empty or not string "
                "type, check config!");
      return;
    }
    need_auth_ = true;
    auth_id_ = doc["auth_id"].GetString();
    auth_key_ = doc["auth_key"].GetString();
    LOG_INFO("need_auth, auth_id:%s, auth_key:%s" << auth_id_.c_str()
                                                  << auth_key_.c_str());
  } else {
    need_auth_ = constants::kNeedAuth;
    LOG_INFO("need_auth is not expect, then use default:%s" << need_auth_
             ? "true"
             : "false");
  }

}
void SdkConfig::OthersParam(const rapidjson::Value &doc) {
  // ser_ip
  if (doc.HasMember("ser_ip") && doc["ser_ip"].IsString()) {
    const rapidjson::Value &obj = doc["ser_ip"];
    local_ip_ = obj.GetString();
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

  // instance num
  if (doc.HasMember("max_instance") && doc["max_instance"].IsInt() && doc["max_instance"].GetInt() > 0) {
    const rapidjson::Value &obj = doc["max_instance"];
    max_instance_ = obj.GetInt();
  } else {
    max_instance_ = constants::kMaxInstance;
  }

  if (doc.HasMember("extend_report") && doc["extend_report"].IsBool()) {
    const rapidjson::Value &obj = doc["extend_report"];
    extend_report_ = obj.GetBool();
  } else {
    extend_report_ = constants::kExtendReport;
  }
}

bool SdkConfig::GetLocalIPV4Address(std::string &err_info, std::string &localhost) {
  int32_t sockfd;
  int32_t ip_num = 0;
  char buf[1024] = {0};
  struct ifreq *ifreq;
  struct ifreq if_flag;
  struct ifconf ifconf;

  ifconf.ifc_len = sizeof(buf);
  ifconf.ifc_buf = buf;
  if ((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
    err_info = "Open the local socket(AF_INET, SOCK_DGRAM) failure!";
    return false;
  }

  ioctl(sockfd, SIOCGIFCONF, &ifconf);
  ifreq = (struct ifreq *) buf;
  ip_num = ifconf.ifc_len / sizeof(struct ifreq);
  for (int32_t i = 0; i < ip_num; i++, ifreq++) {
    if (ifreq->ifr_flags != AF_INET) {
      continue;
    }
    if (0 == strncmp(&ifreq->ifr_name[0], "lo", sizeof("lo"))) {
      continue;
    }
    memcpy(&if_flag.ifr_name[0], &ifreq->ifr_name[0], sizeof(ifreq->ifr_name));
    if ((ioctl(sockfd, SIOCGIFFLAGS, (char *) &if_flag)) < 0) {
      continue;
    }
    if ((if_flag.ifr_flags & IFF_LOOPBACK)
        || !(if_flag.ifr_flags & IFF_UP)) {
      continue;
    }

    if (!strncmp(inet_ntoa(((struct sockaddr_in *) &(ifreq->ifr_addr))->sin_addr),
                 "127.0.0.1", 7)) {
      continue;
    }
    localhost = inet_ntoa(((struct sockaddr_in *) &(ifreq->ifr_addr))->sin_addr);
    close(sockfd);
    err_info = "Ok";
    return true;
  }
  close(sockfd);
  err_info = "Not found the localHost in local OS";
  return false;
}

void SdkConfig::ShowClientConfig() {
  LOG_INFO("per_groupid_thread_nums: " << per_groupid_thread_nums_);
  LOG_INFO("group_ids: " << Utils::getVectorStr(inlong_group_ids_).c_str());
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
  LOG_INFO("manager_update_interval:  minutes" << manager_update_interval_);
  LOG_INFO("manager_url_timeout: " << manager_url_timeout_);
  LOG_INFO("max_tcp_num: " << max_proxy_num_);
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
  LOG_INFO("need_auth: " << need_auth_ ? "true" : "false");
  LOG_INFO("auth_id: " << auth_id_.c_str());
  LOG_INFO("auth_key: " << auth_key_.c_str());
  LOG_INFO("max_group_id_num: " << max_group_id_num_);
  LOG_INFO("max_stream_id_num: " << max_stream_id_num_);
  LOG_INFO("isolation_level: " << isolation_level_);
  LOG_INFO("max_instance: " << max_instance_);
  LOG_INFO("max_cache_num: " << max_cache_num_);
}

} // namespace inlong
