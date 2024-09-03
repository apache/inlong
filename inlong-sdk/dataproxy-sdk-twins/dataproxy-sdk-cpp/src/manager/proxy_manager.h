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

#ifndef INLONG_SDK_PROXY_MANAGER_H
#define INLONG_SDK_PROXY_MANAGER_H

#include "../config//proxy_info.h"
#include "../utils/read_write_mutex.h"
#include <thread>
#include <unordered_map>
#include <vector>

namespace inlong {
using GroupId2ClusterIdMap = std::unordered_map<std::string, int32_t>;
class ProxyManager {
private:
  uint32_t timeout_;
  read_write_mutex groupid_2_cluster_id_rwmutex_;
  read_write_mutex groupid_2_proxy_map_rwmutex_;
  read_write_mutex clusterid_2_proxy_map_rwmutex_;

  std::unordered_map<std::string, std::string> groupid_2_cluster_id_map_;
  std::unordered_map<std::string, int32_t> groupid_2_cluster_id_update_map_;

  std::unordered_map<std::string, ProxyInfoVec> groupid_2_proxy_map_;
  std::unordered_map<std::string, ProxyInfoVec>
      cluster_id_2_proxy_map_; //<cluster_id,busList>

  bool update_flag_;
  std::mutex cond_mutex_;
  std::mutex update_mutex_;

  std::condition_variable cond_;
  bool exit_flag_;
  std::thread update_conf_thread_;
  volatile bool inited_ = false;
  std::unordered_map<std::string, std::string> cache_proxy_info_;
  uint64_t last_update_time_;

  int32_t ParseAndGet(const std::string &key, const std::string &meta_data,
                      ProxyInfoVec &proxy_info_vec,GroupId2ClusterIdMap &group_id_2_cluster_id);

public:
  ProxyManager(){};
  ~ProxyManager();
  static ProxyManager *GetInstance() {
    static ProxyManager instance;
    return &instance;
  }
  int32_t CheckGroupIdConf(const std::string &inlong_group_id, bool is_inited);
  void Update();
  void DoUpdate();
  void Init();
  int32_t GetProxy(const std::string &groupid, ProxyInfoVec &proxy_info_vec);
  int32_t GetProxyByGroupid(const std::string &inlong_group_id,
                            ProxyInfoVec &proxy_info_vec);
  int32_t GetProxyByClusterId(const std::string &cluster_id,
                              ProxyInfoVec &proxy_info_vec);
  std::string GetGroupKey(const std::string &groupid);
  bool HasProxy(const std::string &group_key);
  bool CheckGroupid(const std::string &groupid);
  bool CheckClusterId(const std::string &cluster_id);
  void UpdateClusterId2ProxyMap();
  void UpdateGroupid2ClusterIdMap();
  void BuildLocalCache(std::ofstream &file, int32_t groupid_index,
                       const std::string &groupid,
                       const std::string &meta_data);
  void ReadLocalCache();
  void WriteLocalCache();
  std::string RecoverFromLocalCache(const std::string &groupid);
  std::string GetClusterID(const std::string &groupid);
  void UpdateProxy(
      std::unordered_map<std::string, std::string> &group_id_2_cluster_id);
  std::unordered_map<std::string, std::string> BuildGroupId2ClusterId();
  uint64_t GetGroupIdCount();
  bool SkipUpdate(const std::string &group_id);
};
} // namespace inlong

#endif // INLONG_SDK_PROXY_MANAGER_H
