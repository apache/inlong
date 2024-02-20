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
#include "../manager/proxy_manager.h"
#include "../utils/capi_constant.h"
#include "../utils/logger.h"
#include "../utils/utils.h"

#include "api_code.h"
#include <iostream>
#include <signal.h>

namespace inlong {
int32_t ApiImp::InitApi(const char *config_file_path) {
  if (!__sync_bool_compare_and_swap(&inited_, false, true)) {
    return SdkCode::kMultiInit;
  }

  user_exit_flag_.getAndSet(0);

  if (!SdkConfig::getInstance()->ParseConfig(config_file_path)) {
    LOG_ERROR("ParseConfig error ");
    return SdkCode::kErrorInit;
  }
  max_msg_length_ = std::min(SdkConfig::getInstance()->max_msg_size_,
                             SdkConfig::getInstance()->pack_size_) - constants::ATTR_LENGTH;
  local_ip_ = SdkConfig::getInstance()->local_ip_;

  return DoInit();
}

int32_t ApiImp::Send(const char *inlong_group_id, const char *inlong_stream_id,
                     const char *msg, int32_t msg_len, UserCallBack call_back) {
  if (msg_len > max_msg_length_) {
    return SdkCode::kMsgTooLong;
  }
  if (inlong_group_id == nullptr || inlong_stream_id == nullptr ||
      msg == nullptr || msg_len <= 0) {
    return SdkCode::kInvalidInput;
  }

  if (inited_ == false) {
    return SdkCode::kSendBeforeInit;
  }

  int64_t msg_time = Utils::getCurrentMsTime();
  return this->SendBase(inlong_group_id, inlong_stream_id, local_ip_, msg_time,
                        {msg, msg_len}, call_back);
}

int32_t ApiImp::SendBase(const std::string inlong_group_id,
                         const std::string inlong_stream_id,
                         const std::string client_ip, int64_t report_time,
                         const std::string msg, UserCallBack call_back) {
  int32_t check_ret = CheckData(inlong_group_id, inlong_stream_id, msg);
  if (check_ret != SdkCode::kSuccess) {
    return check_ret;
  }

  ProxyManager::GetInstance()->CheckBidConf(inlong_group_id, true);

  auto recv_group =
      recv_manager_->GetRecvGroup(inlong_group_id);
  if (recv_group == nullptr) {
    LOG_ERROR("fail to get recv group, inlong_group_id:"
              << inlong_group_id << " inlong_stream_id:" << inlong_stream_id);
    return SdkCode::kFailGetRevGroup;
  }

  return recv_group->SendData(msg, inlong_group_id, inlong_stream_id, client_ip,
                              report_time, call_back);
}

int32_t ApiImp::CloseApi(int32_t max_waitms) {
  if (!__sync_bool_compare_and_swap(&init_flag_, false, true)) {
    LOG_ERROR("sdk has been closed! .");
    return SdkCode::kMultiExits;
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(max_waitms));
  return SdkCode::kSuccess;
}

int32_t ApiImp::DoInit() {
  LOG_INFO(
      "inlong dataproxy sdk cpp start Init, version:" << constants::kVersion);

  signal(SIGPIPE, SIG_IGN);

  LOG_INFO("inlong dataproxy cpp sdk Init complete!");

  ProxyManager::GetInstance()->Init();
  ProxyManager::GetInstance()->ReadLocalCache();

  for (int i = 0; i < SdkConfig::getInstance()->inlong_group_ids_.size(); i++) {
    LOG_INFO("DoInit CheckConf inlong_group_id:"
             << SdkConfig::getInstance()->inlong_group_ids_[i]);
    ProxyManager::GetInstance()->CheckBidConf(
        SdkConfig::getInstance()->inlong_group_ids_[i], false);
  }

  return InitManager();
}

int32_t ApiImp::CheckData(const std::string inlong_group_id,
                          const std::string inlong_stream_id,
                          const std::string msg) {
  if (init_succeed_ == 0 || user_exit_flag_.get() == 1) {
    LOG_ERROR("capi has been closed, Init first and then send");
    return SdkCode::kSendAfterClose;
  }

  if (msg.empty() || inlong_group_id.empty() || inlong_stream_id.empty()) {
    LOG_ERROR("invalid input, inlong_group_id"
              << inlong_group_id << " inlong_stream_id" << inlong_stream_id);
    return SdkCode::kInvalidInput;
  }

  if (msg.size() > SdkConfig::getInstance()->max_msg_size_) {
    LOG_ERROR("msg len is too long, cur msg_len"
              << msg.size() << " ext_pack_size"
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
    LOG_ERROR("fail to Init global packqueue");
    return SdkCode::kErrorInit;
  }
  init_succeed_ = true;
  return SdkCode::kSuccess;
}
int32_t
ApiImp::AddInLongGroupId(const std::vector<std::string> &inlong_group_ids) {
  if (inited_ == false) {
    return SdkCode::kSendBeforeInit;
  }
  for (auto inlong_group_id : inlong_group_ids) {
    ProxyManager::GetInstance()->CheckBidConf(inlong_group_id, false);
  }
}
ApiImp::ApiImp() = default;
ApiImp::~ApiImp() = default;
} // namespace inlong