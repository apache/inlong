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

#include "api_code.h"
#include <fstream>
#include <curl/curl.h>

#include "../config/ini_help.h"
#include "../utils/capi_constant.h"
#include "../utils/logger.h"
#include "../utils/utils.h"
#include "../utils/parse_json.h"

namespace inlong {
const uint64_t MINUTE = 60000;
ProxyManager::~ProxyManager() {
  LOG_INFO("~ProxyManager");
  exit_flag_ = true;
  std::unique_lock<std::mutex> con_lck(cond_mutex_);
  update_flag_ = true;
  con_lck.unlock();
  cond_.notify_one();

  if (update_conf_thread_.joinable()) {
    update_conf_thread_.join();
  }

  curl_global_cleanup();
}
void ProxyManager::Init() {
  timeout_ = SdkConfig::getInstance()->manager_url_timeout_;
  last_update_time_ = Utils::getCurrentMsTime();
  if (__sync_bool_compare_and_swap(&inited_, false, true)) {
    curl_global_init(CURL_GLOBAL_ALL);
    ReadLocalCache();
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
  LOG_INFO("start ProxyManager DoUpdate.");
  if (!update_mutex_.try_lock()) {
    LOG_INFO("DoUpdate try_lock. " << getpid());
    return;
  }

  std::srand(unsigned(std::time(nullptr)));

  if (groupid_2_cluster_id_map_.empty()) {
    LOG_INFO("empty groupid, no need to DoUpdate proxy list");
    update_mutex_.unlock();
    return;
  }

  int retry = constants::MAX_RETRY;
  do {
    std::unordered_map<std::string, std::string> bid_2_cluster_id =
        BuildGroupId2ClusterId();

    UpdateProxy(bid_2_cluster_id);

    UpdateGroupid2ClusterIdMap();

    UpdateClusterId2ProxyMap();
    uint64_t id_count = GetGroupIdCount();
    if (bid_2_cluster_id.size() == id_count) {
      break;
    }
    LOG_INFO("retry DoUpdate!. update size:" << bid_2_cluster_id.size()
                                             << " != latest size:" << id_count);
  } while (retry--);

  if (SdkConfig::getInstance()->enable_local_cache_) {
    WriteLocalCache();
  }

  last_update_time_ = Utils::getCurrentMsTime();

  update_mutex_.unlock();
  LOG_INFO("finish ProxyManager DoUpdate.");
}

int32_t ProxyManager::GetProxy(const std::string &key,
                               ProxyInfoVec &proxy_info_vec) {
  if (constants::IsolationLevel::kLevelOne ==
      SdkConfig::getInstance()->isolation_level_) {
    return GetProxyByGroupid(key, proxy_info_vec);
  }
  return GetProxyByClusterId(key, proxy_info_vec);
}

int32_t ProxyManager::CheckGroupIdConf(const std::string &inlong_group_id,
                                       bool is_inited) {
  {
    unique_read_lock<read_write_mutex> rdlck(groupid_2_cluster_id_rwmutex_);
    auto it = groupid_2_cluster_id_map_.find(inlong_group_id);
    if (it != groupid_2_cluster_id_map_.end()) {
      return SdkCode::kSuccess;
    }
  }

  {
    unique_write_lock<read_write_mutex> wtlck(groupid_2_cluster_id_rwmutex_);
    groupid_2_cluster_id_map_.emplace(inlong_group_id, "");
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

bool ProxyManager::HasProxy(const std::string &group_key) {
  if (constants::IsolationLevel::kLevelOne ==
      SdkConfig::getInstance()->isolation_level_) {
    return CheckGroupid(group_key);
  }
  return CheckClusterId(group_key);
}

int32_t ProxyManager::GetProxyByGroupid(const std::string &inlong_group_id,
                                        ProxyInfoVec &proxy_info_vec) {
  unique_read_lock<read_write_mutex> rdlck(groupid_2_proxy_map_rwmutex_);
  auto it = groupid_2_proxy_map_.find(inlong_group_id);
  if (it == groupid_2_proxy_map_.end()) {
    LOG_ERROR("GetProxy failed! inlong_group_id: " << inlong_group_id);
    return SdkCode::kFailGetConn;
  }
  proxy_info_vec = it->second;
  return SdkCode::kSuccess;
}
int32_t ProxyManager::GetProxyByClusterId(const std::string &cluster_id,
                                          ProxyInfoVec &proxy_info_vec) {
  unique_read_lock<read_write_mutex> rdlck(clusterid_2_proxy_map_rwmutex_);
  auto it = cluster_id_2_proxy_map_.find(cluster_id);
  if (it == cluster_id_2_proxy_map_.end()) {
    LOG_ERROR("GetProxy failed! cluster_id:" << cluster_id);
    return SdkCode::kFailGetConn;
  }
  proxy_info_vec = it->second;
  return SdkCode::kSuccess;
}
std::string ProxyManager::GetGroupKey(const std::string &groupid) {
  if (constants::IsolationLevel::kLevelThird ==
      SdkConfig::getInstance()->isolation_level_) {
    return GetClusterID(groupid);
  }
  return groupid;
}
bool ProxyManager::CheckGroupid(const std::string &groupid) {
  unique_read_lock<read_write_mutex> rdlck(groupid_2_proxy_map_rwmutex_);
  auto it = groupid_2_proxy_map_.find(groupid);
  if (it == groupid_2_proxy_map_.end()) {
    return false;
  }
  return true;
}
bool ProxyManager::CheckClusterId(const std::string &cluster_id) {
  unique_read_lock<read_write_mutex> rdlck(clusterid_2_proxy_map_rwmutex_);
  auto it = cluster_id_2_proxy_map_.find(cluster_id);
  if (it == cluster_id_2_proxy_map_.end()) {
    return false;
  }
  return true;
}
void ProxyManager::UpdateClusterId2ProxyMap() {
  unique_read_lock<read_write_mutex> rdlck(groupid_2_cluster_id_rwmutex_);
  for (const auto &it : groupid_2_cluster_id_update_map_) {
    ProxyInfoVec proxy_info_vec;
    if (GetProxyByGroupid(it.first, proxy_info_vec) == SdkCode::kSuccess) {
      unique_write_lock<read_write_mutex> wtlck(clusterid_2_proxy_map_rwmutex_);
      cluster_id_2_proxy_map_[std::to_string(it.second)] = proxy_info_vec;
    }
  }
}
void ProxyManager::UpdateGroupid2ClusterIdMap() {
  unique_write_lock<read_write_mutex> wtlck(groupid_2_cluster_id_rwmutex_);
  for (const auto &it : groupid_2_cluster_id_update_map_) {
    groupid_2_cluster_id_map_[it.first] = std::to_string(it.second);
    LOG_INFO("UpdateGroup2ClusterIdMap groupid:" << it.first << " ,cluster id:"
                                                 << it.second);
  }
}

void ProxyManager::BuildLocalCache(std::ofstream &file, int32_t groupid_index,
                                   const std::string &groupid,
                                   const std::string &meta_data) {
  file << "[groupid" << groupid_index << "]" << std::endl;
  file << "groupid=" << groupid << std::endl;
  file << "proxy_cfg=" << meta_data << std::endl;
}

void ProxyManager::ReadLocalCache() {
  try {
    IniFile ini = IniFile();
    if (ini.load(constants::kCacheFile)) {
      LOG_INFO("there is no bus list cache file");
      return;
    }
    int32_t groupid_count = 0;
    if (ini.getInt("main", "groupid_count", &groupid_count)) {
      LOG_WARN("failed to parse .proxy list.ini file");
      return;
    }
    for (int32_t i = 0; i < groupid_count; i++) {
      std::string groupid_list = "groupid" + std::to_string(i);
      std::string groupid, proxy;
      if (ini.getString(groupid_list, "groupid", &groupid)) {
        LOG_WARN("failed to get from cache file." << groupid);
        continue;
      }
      if (ini.getString(groupid_list, "proxy_cfg", &proxy)) {
        LOG_WARN("failed to get cache proxy list" << groupid);
        continue;
      }
      LOG_INFO("read cache file, id:" << groupid << ", local config:" << proxy);
      cache_proxy_info_[groupid] = proxy;
    }
  } catch (...) {
    LOG_ERROR("ReadLocalCache error!");
  }
}

void ProxyManager::WriteLocalCache() {
  int32_t groupid_count = 0;
  try {
    std::ofstream outfile;
    outfile.open(constants::kCacheTmpFile, std::ios::out | std::ios::trunc);

    for (auto &it : cache_proxy_info_) {
      BuildLocalCache(outfile, groupid_count, it.first, it.second);
      groupid_count++;
    }
    if (outfile) {
      if (groupid_count) {
        outfile << "[main]" << std::endl;
        outfile << "groupid_count=" << groupid_count << std::endl;
      }
      outfile.close();
    }
    if (groupid_count) {
      rename(constants::kCacheTmpFile, constants::kCacheFile);
    }
  } catch (...) {
    LOG_ERROR("WriteLocalCache error!");
  }
  LOG_INFO("WriteLocalCache getGroupId number:" << groupid_count);
}

std::string ProxyManager::RecoverFromLocalCache(const std::string &groupid) {
  std::string meta_data;
  auto it = cache_proxy_info_.find(groupid);
  if (it != cache_proxy_info_.end()) {
    meta_data = it->second;
  }
  LOG_INFO("RecoverFromLocalCache:" << groupid << ",local cache:" << meta_data);
  return meta_data;
}
std::string ProxyManager::GetClusterID(const std::string &groupid) {
  unique_read_lock<read_write_mutex> rdlck(groupid_2_cluster_id_rwmutex_);
  auto it = groupid_2_cluster_id_map_.find(groupid);
  if (it == groupid_2_cluster_id_map_.end()) {
    return "";
  }
  return it->second;
}

void ProxyManager::UpdateProxy(
    std::unordered_map<std::string, std::string> &group_id_2_cluster_id) {
  for (auto &groupid2cluster : group_id_2_cluster_id) {
    if (SkipUpdate(groupid2cluster.first)) {
      LOG_WARN("SkipUpdate group_id:" << groupid2cluster.first);
      continue;
    }
    std::string url = SdkConfig::getInstance()->manager_url_ + "/" + groupid2cluster.first;
    if (SdkConfig::getInstance()->extend_report_) {
      url = SdkConfig::getInstance()->manager_url_ + "?bid=" + groupid2cluster.first + "&net_tag=all&ip=" +
          SdkConfig::getInstance()->local_ip_;
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

    if (ret != SdkCode::kSuccess) {
      if (groupid_2_proxy_map_.find(groupid2cluster.first) !=
          groupid_2_proxy_map_.end()) {
        LOG_WARN("failed to request from manager, use previous "
                     << groupid2cluster.first);
        continue;
      }
      if (!SdkConfig::getInstance()->enable_local_cache_) {
        LOG_WARN("failed to request from manager, forbid local cache!");
        continue;
      }
      meta_data = RecoverFromLocalCache(groupid2cluster.first);
      if (meta_data.empty()) {
        LOG_WARN("local cache is empty!");
        continue;
      }
    }

    ProxyInfoVec proxyInfoVec;
    if (SdkConfig::getInstance()->extend_report_) {
      ret = ParseJson::ParseProxyInfo(groupid2cluster.first, meta_data, groupid_2_cluster_id_update_map_, proxyInfoVec);
    } else {
      ret = ParseJson::ParseProxyInfo(groupid2cluster.first, meta_data, proxyInfoVec, groupid_2_cluster_id_update_map_);
    }

    if (ret != SdkCode::kSuccess) {
      LOG_ERROR("Failed to parse json: " << meta_data);
      continue;
    }
    if (!proxyInfoVec.empty()) {
      unique_write_lock<read_write_mutex> wtlck(groupid_2_proxy_map_rwmutex_);
      groupid_2_proxy_map_[groupid2cluster.first] = proxyInfoVec;
      cache_proxy_info_[groupid2cluster.first] = meta_data;
      LOG_INFO("groupid:" << groupid2cluster.first << " success update "
                          << proxyInfoVec.size() << " proxy-ip.");
    }
  }
}
std::unordered_map<std::string, std::string>
ProxyManager::BuildGroupId2ClusterId() {
  std::unordered_map<std::string, std::string> bid_2_cluster_id_map_tmp;
  unique_read_lock<read_write_mutex> rdlck(groupid_2_cluster_id_rwmutex_);
  for (auto &bid2cluster : groupid_2_cluster_id_map_) {
    bid_2_cluster_id_map_tmp.insert(bid2cluster);
  }
  return bid_2_cluster_id_map_tmp;
}

uint64_t ProxyManager::GetGroupIdCount() {
  unique_read_lock<read_write_mutex> rdlck(groupid_2_cluster_id_rwmutex_);
  return groupid_2_cluster_id_map_.size();
}

bool ProxyManager::SkipUpdate(const std::string &group_id) {
  uint64_t current_time = Utils::getCurrentMsTime();
  uint64_t diff = current_time - last_update_time_;
  uint64_t threshold =
      SdkConfig::getInstance()->manager_update_interval_ * MINUTE;
  bool ret = CheckGroupid(group_id);
  if (diff < threshold && ret) {
    return true;
  }
  return false;
}
} // namespace inlong
