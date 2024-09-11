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

#include <queue>
#include <thread>
#include <unordered_map>

#include "../config/sdk_conf.h"
#include "../metric/environment.h"
#include "../metric/metric.h"
#include "../utils/logger.h"

#ifndef INLONG_METRIC_MANAGER_H
#define INLONG_METRIC_MANAGER_H
namespace inlong {
using MetricMap = std::unordered_map<std::string, Metric>;
static const char kStatJoiner = ' ';
class MetricManager {
 private:
  mutable std::mutex mutex_;
  MetricMap stat_map_;
  std::thread update_thread_;
  volatile bool inited_ = false;
  bool running_ = true;
  Environment environment_;
  std::string coreParma_;

 public:
  static MetricManager *GetInstance() {
    static MetricManager instance;
    return &instance;
  }
  void Init();
  void InitEnvironment();
  void PrintMetric();
  void Run();
  void UpdateMetric(const std::string &stat_key, Metric &stat) {
    if(!running_){
      return;
    }
    std::lock_guard<std::mutex> lck(mutex_);
    stat_map_[stat_key].Update(stat);
  }

  void AddReceiveBufferFullCount(const std::string &inlong_group_id, const std::string &inlong_stream_id,uint64_t count) {
    if(!running_){
      return;
    }
    std::lock_guard<std::mutex> lck(mutex_);
    std::string stat_key= BuildStatKey(inlong_group_id,inlong_stream_id);
    stat_map_[stat_key].AddReceiveBufferFullCount(count);
  }

  void AddTooLongMsgCount(const std::string &inlong_group_id, const std::string &inlong_stream_id,uint64_t count) {
    if (!running_) {
      return;
    }
    std::lock_guard<std::mutex> lck(mutex_);
    std::string stat_key= BuildStatKey(inlong_group_id,inlong_stream_id);
    stat_map_[stat_key].AddTooLongMsgCount(count);
  }

  void AddMetadataFailCount(const std::string &inlong_group_id, const std::string &inlong_stream_id,uint64_t count) {
    if (!running_) {
      return;
    }
    std::lock_guard<std::mutex> lck(mutex_);
    std::string stat_key= BuildStatKey(inlong_group_id,inlong_stream_id);
    stat_map_[stat_key].AddMetadataFailCount(count);
  }

  void Reset();

  std::string BuildStatKey(const std::string &inlong_group_id, const std::string &inlong_stream_id) {
    return inlong_group_id + kStatJoiner + inlong_stream_id;
  }

  ~MetricManager() {
    running_ = false;
    if (update_thread_.joinable()) {
      update_thread_.join();
    }
    LOG_INFO("Metric manager exited");
  }
};
}  // namespace inlong
#endif  // INLONG_METRIC_MANAGER_H
