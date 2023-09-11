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
  timeout_ = SdkConfig::getInstance()->bus_URL_timeout_;
  if (__sync_bool_compare_and_swap(&inited_, false, true)) {
    update_conf_thread_ = std::thread(&ProxyManager::Update, this);
  }
}

void ProxyManager::Update() {
  while (true) {
    std::unique_lock<std::mutex> con_lck(cond_mutex_);
    if (cond_.wait_for(con_lck,
                       std::chrono::minutes(
                           SdkConfig::getInstance()->bus_update_interval_),
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
    LOG_INFO("empty groupid, no need to DoUpdate buslist");
    update_mutex_.unlock();
    return;
  }

  {
    unique_read_lock<read_write_mutex> rdlck(bid_2_cluster_rwmutex_);
    for (auto &groupid2cluster : groupid_2_cluster_map_) {
      std::string url;
      if (SdkConfig::getInstance().enable_proxy_URL_from_cluster_)
        url = SdkConfig::getInstance().proxy_cluster_URL_;
      else {
        url = SdkConfig::getInstance().proxy_URL_ + "/" + groupid2cluster.first;
      }
      std::string post_data = "ip=" + SdkConfig::getInstance().ser_ip_ +
                              "&version=" + constants::kTDBusCAPIVersion +
                              "&protocolType=" + constants::kProtocolType;
      LOG_WARN("get inlong_group_id:%s proxy cfg url:%s, post_data:%s",
               groupid2cluster.first.c_str(), url.c_str(), post_data.c_str());

      std::string meta_data;
      int32_t ret;
      std::string urlByDNS;
      for (int i = 0; i < constants::kMaxRequestTDMTimes; i++) {
        ret = Utils::requestUrl(url, urlByDNS, meta_data, timeout_);
        if (!ret) {
          break;
        }
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

int32_t ProxyManager::ParseAndGet(const std::string &groupid,
                                  const std::string &meta_data,
                                  ProxyInfoVec &proxy_info_vec) {
  rapidjson::Document doc;
  if (doc.Parse(meta_data.c_str()).HasParseError()) {
    LOG_ERROR("failed to parse meta_data, error" << doc.GetParseError() << ":"
                                                 << doc.GetErrorOffset());
    return SdkCode::kErrorParseJson;
  }

  if (doc.HasMember("size") && doc["size"].IsInt() && !doc["size"].IsNull() &&
      doc["size"].GetInt() > 0) {
    const rapidjson::Value &obj = doc["size"];
  } else {
    LOG_ERROR("can't find groupid:%s buslist from meta_data"
              << groupid.c_str());
    return SdkCode::kErrorParseJson;
  }

  if (doc.HasMember("cluster_id") && doc["cluster_id"].IsInt() &&
      !doc["cluster_id"].IsNull()) {
    const rapidjson::Value &obj = doc["cluster_id"];
  } else {
    LOG_ERROR("cluster_id of groupid:%s is not found or not a integer"
              << groupid.c_str());
    return SdkCode::kErrorParseJson;
  }

  if (doc.HasMember("address") && !doc["address"].IsNull()) // v2版本
  {
    const rapidjson::Value &hostlist = doc["address"];
    for (auto &info : hostlist.GetArray()) {
      std::string id, ip;
      int32_t port;
      if (info.HasMember("host") && !info["host"].IsNull())
        ip = info["host"].GetString();
      else {
        LOG_ERROR("this host info is null");
        continue;
      }
      if (info.HasMember("port") && !info["port"].IsNull()) {
        if (info["port"].IsString())
          port = std::stoi(info["port"].GetString());
        if (info["port"].IsInt())
          port = info["port"].GetInt();
      }

      else {
        LOG_ERROR("this port info is null or negative");
        continue;
      }
      if (info.HasMember("id") && !info["id"].IsNull()) {
        if (info["id"].IsString())
          id = info["id"].GetString();
        if (info["id"].IsInt())
          id = std::to_string(info["id"].GetInt());
      } else {
        LOG_ERROR("there is no id info of groupid");
        continue;
      }
      proxy_info_vec.emplace_back(id, ip, port);
    }
  } else {
    LOG_ERROR("there is no any host info of groupid:%s" << groupid.c_str());
    return SdkCode::kErrorParseJson;
  }
  return SdkCode::kSuccess;
}

int32_t ProxyManager::GetBusByBid(const std::string &groupid,
                                  BusInfoVec &proxy_info_vec) {
  unique_read_lock<read_write_mutex> rdlck(groupid_2_proxy_map_rwmutex_);
  auto it = groupid_2_proxy_map_.find(groupid);
  if (it == groupid_2_proxy_map_.end()) {
    LOG_ERROR("GetProxyByGroupid  failed . Groupid " << groupid);
    return SdkCode::kFailGetBusConf;
  }
  proxy_info_vec = it->second;
  return SdkCode::kSuccess;
}

int32_t ProxyManager::CheckBidConf(const std::string &groupid, bool is_inited) {
  {
    unique_read_lock<read_write_mutex> rdlck(groupid_2_cluster_rwmutex_);
    auto it = groupid_2_cluster_map_.find(groupid);
    if (it != groupid_2_cluster_map_.end()) {
      return SdkCode::kSuccess;
    }
  }

  {
    unique_write_lock<read_write_mutex> wtlck(groupid_2_cluster_rwmutex_);
    groupid_2_cluster_map_.emplace(groupid, -1);
  }

  LOG_INFO("CheckProxyConf groupid:" << groupid << ",isInited :" << is_inited);
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

bool ProxyManager::IsBusExist(const std::string &groupid) {
  unique_read_lock<read_write_mutex> rdlck(groupid_2_proxy_map_rwmutex_);
  auto it = groupid_2_proxy_map_.find(groupid);
  if (it == groupid_2_proxy_map_.end()) {
    return false;
  }
  return true;
}
} // namespace inlong
