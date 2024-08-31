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

#include "api_imp.h"
#include <signal.h>
#include <iostream>
#include "../manager/proxy_manager.h"
#include "../manager/metric_manager.h"
#include "../utils/capi_constant.h"
#include "../utils/logger.h"
#include "../utils/utils.h"
#include "../core/api_code.h"

namespace inlong {
int32_t ApiImp::InitApi(const char *config_file_path) {
  if (config_file_path == nullptr) {
    return SdkCode::kErrorInit;
  }
  if (!__sync_bool_compare_and_swap(&inited_, false, true)) {
    return SdkCode::kMultiInit;
  }

  if (!SdkConfig::getInstance()->ParseConfig(config_file_path)) {
    return SdkCode::kErrorInit;
  }

  max_msg_length_ = SdkConfig::getInstance()->max_msg_size_ - constants::ATTR_LENGTH;
  local_ip_ = SdkConfig::getInstance()->local_ip_;

  return DoInit();
}

int32_t ApiImp::Send(const char *inlong_group_id, const char *inlong_stream_id, const char *msg, int32_t msg_len,
                     UserCallBack call_back) {
  if (inited_ == false || exit_flag_) {
    return SdkCode::kSendBeforeInit;
  }
  int32_t code=ValidateParams(inlong_group_id, inlong_stream_id, msg, msg_len);
  if(code !=SdkCode::kSuccess){
    return code;
  }

  return this->SendBase(inlong_group_id, inlong_stream_id, {msg, msg_len}, call_back);
}
int32_t ApiImp::Send(const char *inlong_group_id, const char *inlong_stream_id, const char *msg, int32_t msg_len,
                     int64_t data_time, UserCallBack call_back) {
  if (inited_ == false || exit_flag_) {
    return SdkCode::kSendBeforeInit;
  }
  int32_t code=ValidateParams(inlong_group_id, inlong_stream_id, msg, msg_len);
  if(code !=SdkCode::kSuccess){
    return code;
  }

  return this->SendBase(inlong_group_id, inlong_stream_id, {msg, msg_len}, call_back, data_time);
}

int32_t ApiImp::ValidateParams(const char *inlong_group_id, const char *inlong_stream_id, const char *msg, int32_t msg_len){
  if (msg_len > max_msg_length_) {
    MetricManager::GetInstance()->AddTooLongMsgCount(inlong_group_id, inlong_stream_id, 1);
    return SdkCode::kMsgTooLong;
  }
  if (inlong_group_id == nullptr || inlong_stream_id == nullptr || msg == nullptr || msg_len <= 0) {
    return SdkCode::kInvalidInput;
  }
  return SdkCode::kSuccess;
}

int32_t ApiImp::SendBase(const std::string& inlong_group_id, const std::string& inlong_stream_id, const std::string& msg,
                         UserCallBack call_back, int64_t report_time) {
  int32_t check_ret = CheckData(inlong_group_id, inlong_stream_id, msg);
  if (check_ret != SdkCode::kSuccess) {
    return check_ret;
  }

  ProxyManager::GetInstance()->CheckGroupIdConf(inlong_group_id, true);

  auto recv_group = recv_manager_->GetRecvGroup(inlong_group_id);
  if (recv_group == nullptr) {
    LOG_ERROR("fail to get pack queue, group id:" << inlong_group_id << ",getStreamId:" << inlong_stream_id);
    return SdkCode::kFailGetRevGroup;
  }

  return recv_group->SendData(msg, inlong_group_id, inlong_stream_id, report_time, call_back);
}

int32_t ApiImp::CloseApi(int32_t max_waitms) {
  exit_flag_ = true;
  std::this_thread::sleep_for(std::chrono::milliseconds(max_waitms));
  return SdkCode::kSuccess;
}

int32_t ApiImp::DoInit() {
  LOG_INFO("tdbus sdk cpp start Init, version:" << constants::kVersion);

  signal(SIGPIPE, SIG_IGN);

  LOG_INFO("tdbus_sdk_cpp Init complete!");

  ProxyManager::GetInstance()->Init();
  MetricManager::GetInstance()->Init();

  for (int i = 0; i < SdkConfig::getInstance()->inlong_group_ids_.size(); i++) {
    LOG_INFO("Do init:" << SdkConfig::getInstance()->inlong_group_ids_[i]);
    ProxyManager::GetInstance()->CheckGroupIdConf(SdkConfig::getInstance()->inlong_group_ids_[i], false);
  }

  return InitManager();
}

int32_t ApiImp::CheckData(const std::string& inlong_group_id, const std::string& inlong_stream_id, const std::string& msg) {
  if (!init_succeed_) {
    return SdkCode::kSendAfterClose;
  }

  if (msg.empty() || inlong_group_id.empty() || inlong_stream_id.empty()) {
    LOG_ERROR("invalid input, group id:" << inlong_group_id << " stream id:" << inlong_stream_id << "msg" << msg);
    return SdkCode::kInvalidInput;
  }

  if (msg.size() > SdkConfig::getInstance()->max_msg_size_) {
    MetricManager::GetInstance()->AddTooLongMsgCount(inlong_group_id, inlong_stream_id, 1);
    LOG_ERROR("msg DataLen is too long, cur msg_len" << msg.size() << " ext_pack_size"
                                                     << SdkConfig::getInstance()->max_msg_size_);
    return SdkCode::kMsgTooLong;
  }

  return SdkCode::kSuccess;
}
int32_t ApiImp::InitManager() {
  send_manager_ = std::make_shared<SendManager>();
  if (!send_manager_) {
    LOG_ERROR("fail to Init global buffer pools");
    return SdkCode::kErrorInit;
  }

  recv_manager_ = std::make_shared<RecvManager>(send_manager_);
  if (!recv_manager_) {
    return SdkCode::kErrorInit;
  }
  init_succeed_ = true;
  return SdkCode::kSuccess;
}
int32_t ApiImp::AddInLongGroupId(const std::vector<std::string> &group_ids) {
  if (!inited_) {
    return SdkCode::kSendBeforeInit;
  }
  for (auto group_id : group_ids) {
    ProxyManager::GetInstance()->CheckGroupIdConf(group_id, false);
  }
  return SdkCode::kSuccess;
}
ApiImp::ApiImp() = default;
ApiImp::~ApiImp() = default;
}  // namespace inlong