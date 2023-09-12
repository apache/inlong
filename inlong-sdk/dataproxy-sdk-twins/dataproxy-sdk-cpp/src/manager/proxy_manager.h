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
class ProxyManager {
private:
  static ProxyManager *instance_;
  int32_t timeout_;
  read_write_mutex groupid_2_cluster_rwmutex_;
  read_write_mutex groupid_2_proxy_map_rwmutex_;

  std::unordered_map<std::string, int32_t> groupid_2_cluster_map_;
  std::unordered_map<std::string, ProxyInfoVec> groupid_2_proxy_map_;
  bool update_flag_;
  std::mutex cond_mutex_;
  std::mutex update_mutex_;

  std::condition_variable cond_;
  bool exit_flag_;
  std::thread update_conf_thread_;
  volatile bool inited_ = false;

  int32_t ParseAndGet(const std::string &groupid, const std::string &meta_data,
                      ProxyInfoVec &bus_info_vec);

public:
  ProxyManager(){};
  ~ProxyManager();
  static ProxyManager *GetInstance() { return instance_; }
  int32_t CheckBidConf(const std::string &groupid, bool is_inited);
  void Update();
  void DoUpdate();
  void Init();
  int32_t GetProxy(const std::string &groupid, ProxyInfoVec &proxy_info_vec);
  bool IsBusExist(const std::string &groupid);
};
} // namespace inlong

#endif // INLONG_SDK_PROXY_MANAGER_H
