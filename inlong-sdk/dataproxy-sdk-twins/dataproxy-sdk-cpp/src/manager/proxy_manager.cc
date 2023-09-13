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

#include "proxy_manager.h"
#include "../utils/capi_constant.h"
#include "../utils/logger.h"
#include "../utils/utils.h"
#include "api_code.h"
#include <fstream>
#include <rapidjson/document.h>

namespace inlong {
ProxyManager *ProxyManager::instance_ = new ProxyManager();
ProxyManager::~ProxyManager() {
  if (update_conf_thread_.joinable()) {
    update_conf_thread_.join();
  }

  exit_flag_ = true;
  std::unique_lock<std::mutex> con_lck(cond_mutex_);
  update_flag_ = true;
  con_lck.unlock();
  cond_.notify_one();
}
void ProxyManager::Init() {
  timeout_ = SdkConfig::getInstance()->manager_url_timeout_;
  if (__sync_bool_compare_and_swap(&inited_, false, true)) {
    update_conf_thread_ = std::thread(&ProxyManager::Update, this);
  }
}

void ProxyManager::Update() {
  while (true) {
    std::unique_lock<std::mutex> con_lck(cond_mutex_);
    if (cond_.wait_for(con_lck,
                       std::chrono::minutes(
                           SdkConfig::getInstance()->manager_update_interval_),
                       [this]() { return update_flag_; })) {
      if (exit_flag_)
        break;
      update_flag_ = false;
      con_lck.unlock();
      DoUpdate();
    } else {
      DoUpdate();
    }
  }
  LOG_INFO("proxylist DoUpdate thread exit");
}
void ProxyManager::DoUpdate() {
  update_mutex_.try_lock();
  LOG_INFO("start ProxyManager DoUpdate.");

  if (groupid_2_cluster_map_.empty()) {
    LOG_INFO("empty groupid, no need to DoUpdate proxy list");
    update_mutex_.unlock();
    return;
  }

  {
    unique_read_lock<read_write_mutex> rdlck(groupid_2_cluster_rwmutex_);
    for (auto &groupid2cluster : groupid_2_cluster_map_) {
      std::string url;
      if (SdkConfig::getInstance()->enable_manager_url_from_cluster_)
        url = SdkConfig::getInstance()->manager_cluster_url_;
      else {
        url = SdkConfig::getInstance()->manager_url_ + "/" +
              groupid2cluster.first;
      }
      std::string post_data = "ip=" + SdkConfig::getInstance()->local_ip_ +
                              "&version=" + constants::kVersion +
                              "&protocolType=" + constants::kProtocolType;
      LOG_WARN("get inlong_group_id:" << groupid2cluster.first.c_str()
                                      << "proxy cfg url " << url.c_str()
                                      << "post_data:" << post_data.c_str());

      std::string meta_data;
      int32_t ret;
      std::string urlByDNS;
      for (int i = 0; i < constants::kMaxRequestTDMTimes; i++) {
        HttpRequest request = {url,
                               timeout_,
                               SdkConfig::getInstance()->need_auth_,
                               SdkConfig::getInstance()->auth_id_,
                               SdkConfig::getInstance()->auth_key_,
                               post_data};
        ret = Utils::requestUrl(meta_data, &request);
        if (!ret) {
          break;
        } // request success
      }
      ProxyInfoVec proxyInfoVec;
      ret = ParseAndGet(groupid2cluster.first, meta_data, proxyInfoVec);
      if (ret != SdkCode::kSuccess) {
        LOG_ERROR("failed to parse groupid:%s json proxy list "
                  << groupid2cluster.first.c_str());
        continue;
      }
      if (!proxyInfoVec.empty()) {
        unique_write_lock<read_write_mutex> wtlck(groupid_2_proxy_map_rwmutex_);
        groupid_2_proxy_map_[groupid2cluster.first] = proxyInfoVec;
        LOG_INFO("groupid:" << groupid2cluster.first << " success update "
                            << proxyInfoVec.size() << " proxy-ip.");
      }
    }
  }
  update_mutex_.unlock();
  LOG_INFO("finish ProxyManager DoUpdate.");
}

int32_t ProxyManager::ParseAndGet(const std::string &inlong_group_id,
                                  const std::string &meta_data,
                                  ProxyInfoVec &proxy_info_vec) {
  rapidjson::Document doc;
  if (doc.Parse(meta_data.c_str()).HasParseError()) {
    LOG_ERROR("failed to parse meta_data, error" << doc.GetParseError() << ":"
                                                 << doc.GetErrorOffset());
    return SdkCode::kErrorParseJson;
  }

  if (!(doc.HasMember("success") && doc["success"].IsBool() &&
        doc["success"].GetBool())) {
    LOG_ERROR("failed to get proxy_list of inlong_group_id:%s, success: not "
              "exist or false"
              << inlong_group_id.c_str());
    return SdkCode::kErrorParseJson;
  }
  // check data valid
  if (!doc.HasMember("data") || doc["data"].IsNull()) {
    LOG_ERROR("failed to get proxy_list of inlong_group_id:%s, data: not exist "
              "or null"
              << inlong_group_id.c_str());
    return SdkCode::kErrorParseJson;
  }

  // check nodelist valid
  const rapidjson::Value &clusterInfo = doc["data"];
  if (!clusterInfo.HasMember("nodeList") || clusterInfo["nodeList"].IsNull()) {
    LOG_ERROR("invalid nodeList of inlong_group_id:%s, not exist or null"
              << inlong_group_id.c_str());
    return SdkCode::kErrorParseJson;
  }

  // check nodeList isn't empty
  const rapidjson::Value &nodeList = clusterInfo["nodeList"];
  if (nodeList.GetArray().Size() == 0) {
    LOG_ERROR("empty nodeList of inlong_group_id:%s"
              << inlong_group_id.c_str());
    return SdkCode::kErrorParseJson;
  }
  // check clusterId
  if (!clusterInfo.HasMember("clusterId") ||
      !clusterInfo["clusterId"].IsInt() ||
      clusterInfo["clusterId"].GetInt() < 0) {
    LOG_ERROR("clusterId of inlong_group_id:%s is not found or not a integer"
              << inlong_group_id.c_str());
    return SdkCode::kErrorParseJson;
  }

  // proxy list
  for (auto &proxy : nodeList.GetArray()) {
    std::string ip;
    std::string id;
    int32_t port;
    if (proxy.HasMember("ip") && !proxy["ip"].IsNull())
      ip = proxy["ip"].GetString();
    else {
      LOG_ERROR("this ip info is null");
      continue;
    }
    if (proxy.HasMember("port") && !proxy["port"].IsNull()) {
      if (proxy["port"].IsString())
        port = std::stoi(proxy["port"].GetString());
      else if (proxy["port"].IsInt())
        port = proxy["port"].GetInt();
    }

    else {
      LOG_ERROR("this ip info is null or negative");
      continue;
    }
    if (proxy.HasMember("id") && !proxy["id"].IsNull()) {
      if (proxy["id"].IsString())
        id = proxy["id"].GetString();
      else if (proxy["id"].IsInt())
        id = proxy["id"].GetInt();
    } else {
      LOG_WARN("there is no id info of inlong_group_id");
      continue;
    }
    proxy_info_vec.emplace_back(id, ip, port);
  }

  return SdkCode::kSuccess;
}

int32_t ProxyManager::GetProxy(const std::string &groupid,
                               ProxyInfoVec &proxy_info_vec) {
  unique_read_lock<read_write_mutex> rdlck(groupid_2_proxy_map_rwmutex_);
  auto it = groupid_2_proxy_map_.find(groupid);
  if (it == groupid_2_proxy_map_.end()) {
    LOG_ERROR("GetProxyByGroupid  failed . Groupid " << groupid);
    return SdkCode::kFailGetProxyConf;
  }
  proxy_info_vec = it->second;
  return SdkCode::kSuccess;
}

int32_t ProxyManager::CheckBidConf(const std::string &inlong_group_id,
                                   bool is_inited) {
  {
    unique_read_lock<read_write_mutex> rdlck(groupid_2_cluster_rwmutex_);
    auto it = groupid_2_cluster_map_.find(inlong_group_id);
    if (it != groupid_2_cluster_map_.end()) {
      return SdkCode::kSuccess;
    }
  }

  {
    unique_write_lock<read_write_mutex> wtlck(groupid_2_cluster_rwmutex_);
    groupid_2_cluster_map_.emplace(inlong_group_id, -1);
  }

  LOG_INFO("CheckProxyConf groupid:" << inlong_group_id
                                     << ",isInited :" << is_inited);
  if (is_inited) {
    std::unique_lock<std::mutex> con_lck(cond_mutex_);
    update_flag_ = true;
    con_lck.unlock();
    cond_.notify_one();
  } else {
    DoUpdate();
  }
  return SdkCode::kSuccess;
}

bool ProxyManager::IsExist(const std::string &inlong_group_id) {
  unique_read_lock<read_write_mutex> rdlck(groupid_2_proxy_map_rwmutex_);
  auto it = groupid_2_proxy_map_.find(inlong_group_id);
  if (it == groupid_2_proxy_map_.end()) {
    return false;
  }
  return true;
}
} // namespace inlong
