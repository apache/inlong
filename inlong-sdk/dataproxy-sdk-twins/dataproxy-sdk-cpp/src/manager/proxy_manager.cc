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
#include "../config/ini_help.h"
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

  std::srand(unsigned(std::time(nullptr)));

  if (groupid_2_cluster_id_map_.empty()) {
    LOG_INFO("empty groupid, no need to DoUpdate proxy list");
    update_mutex_.unlock();
    return;
  }

  {
    unique_read_lock<read_write_mutex> rdlck(groupid_2_cluster_id_rwmutex_);
    for (auto &groupid2cluster : groupid_2_cluster_id_map_) {
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

      if (ret != SdkCode::kSuccess) {
        if (groupid_2_proxy_map_.find(groupid2cluster.first) != groupid_2_proxy_map_.end()) {
          LOG_WARN("failed to request from manager, use previous " << groupid2cluster.first);
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
      ret = ParseAndGet(groupid2cluster.first, meta_data, proxyInfoVec);
      if (ret != SdkCode::kSuccess) {
        LOG_ERROR("failed to parse groupid:%s json proxy list "
                  << groupid2cluster.first.c_str());
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

  UpdateGroupid2ClusterIdMap();

  UpdateClusterId2ProxyMap();

  if (SdkConfig::getInstance()->enable_local_cache_) {
    WriteLocalCache();
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
  groupid_2_cluster_id_update_map_[inlong_group_id] =
      clusterInfo["clusterId"].GetInt();

  // check load
  int32_t load = 0;
  if (clusterInfo.HasMember("load") && clusterInfo["load"].IsInt() &&
      !clusterInfo["load"].IsNull()) {
    const rapidjson::Value &obj = clusterInfo["load"];
    load = obj.GetInt();
  } else {
    load = 0;
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
    proxy_info_vec.emplace_back(id, ip, port, load);
  }

  return SdkCode::kSuccess;
}

int32_t ProxyManager::GetProxy(const std::string &key,
                               ProxyInfoVec &proxy_info_vec) {
  if (SdkConfig::getInstance()->enable_isolation_) {
    return GetProxyByGroupid(key, proxy_info_vec);
  } else {
    return GetProxyByClusterId(key, proxy_info_vec);
  }
}

int32_t ProxyManager::CheckBidConf(const std::string &inlong_group_id,
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

bool ProxyManager::HasProxy(const std::string &inlong_group_id) {
  if (SdkConfig::getInstance()->enable_isolation_) {
    return CheckGroupid(inlong_group_id);
  } else {
    return CheckClusterId(inlong_group_id);
  }
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
  if (SdkConfig::getInstance()->enable_isolation_) {
    return groupid;
  }
  unique_read_lock<read_write_mutex> rdlck(groupid_2_cluster_id_rwmutex_);
  auto it = groupid_2_cluster_id_map_.find(groupid);
  if (it == groupid_2_cluster_id_map_.end()) {
    return "";
  }
  return it->second;
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
  if (SdkConfig::getInstance()->enable_isolation_) {
    return;
  }
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

void ProxyManager::BuildLocalCache(std::ofstream &file, int32_t bid_index, const std::string &bid,
                                     const std::string &meta_data) {
  file << "[bid" << bid_index << "]" << std::endl;
  file << "bid=" << bid << std::endl;
  file << "bus_cfg=" << meta_data << std::endl;
}

void ProxyManager::ReadLocalCache() {
  try {
    IniFile ini = IniFile();
    if (ini.load(constants::kCacheFile)) {
      LOG_INFO("there is no bus list cache file");
      return;
    }
    int32_t bid_count = 0;
    if (ini.getInt("main", "bid_count", &bid_count)) {
      LOG_WARN("failed to parse .bus list.ini file");
      return;
    }
    for (int32_t i = 0; i < bid_count; i++) {
      std::string bidlist = "bid" + std::to_string(i);
      std::string bid, bus;
      if (ini.getString(bidlist, "bid", &bid)) {
        LOG_WARN("failed to get from cache file." << bid);
        continue;
      }
      if (ini.getString(bidlist, "bus_cfg", &bus)) {
        LOG_WARN("failed to get cache bus list" << bid);
        continue;
      }
      LOG_INFO("read cache file, bid:" << bid << ", local config:" << bus);
      cache_proxy_info_[bid] = bus;
    }
  }catch (...){
    LOG_ERROR("ReadLocalCache error!");
  }
}

void ProxyManager::WriteLocalCache() {
  int32_t bid_count = 0;
  try {
    std::ofstream outfile;
    outfile.open(constants::kCacheTmpFile, std::ios::out | std::ios::trunc);

    for (auto &it : cache_proxy_info_) {
      BuildLocalCache(outfile, bid_count, it.first, it.second);
      bid_count++;
    }
    if (outfile) {
      if (bid_count) {
        outfile << "[main]" << std::endl;
        outfile << "bid_count=" << bid_count << std::endl;
      }
      outfile.close();
    }
    if (bid_count) {
      rename(constants::kCacheTmpFile, constants::kCacheFile);
    }
  } catch (...) {
    LOG_ERROR("WriteLocalCache error!");
  }
  LOG_INFO("WriteLocalCache bid number:" << bid_count);
}

std::string ProxyManager::RecoverFromLocalCache(const std::string &bid) {
  std::string meta_data;
  auto it = cache_proxy_info_.find(bid);
  if (it != cache_proxy_info_.end()) {
    meta_data = it->second;
  }
  LOG_INFO("RecoverFromLocalCache:" << bid << ",local cache:" << meta_data);
  return meta_data;
}

} // namespace inlong
