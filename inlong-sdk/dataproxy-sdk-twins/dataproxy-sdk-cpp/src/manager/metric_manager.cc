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

#include "metric_manager.h"

#include <rapidjson/document.h>
#include <sys/prctl.h>
#include <unistd.h>

#include "../utils/logger.h"
#include "../utils/utils.h"
#include "../utils/capi_constant.h"

namespace inlong {
void MetricManager::Init() {
  if (__sync_bool_compare_and_swap(&inited_, false, true)) {
    update_thread_ = std::thread(&MetricManager::Run, this);
  }
  InitEnvironment();
}
void MetricManager::InitEnvironment() {
  environment_.setType("cpp");
  environment_.setVersion(constants::kVersion);
  environment_.setPid(getpid());
  environment_.setIp(SdkConfig::getInstance()->local_ip_);
  environment_.SetExtendReport(SdkConfig::getInstance()->extend_report_);
}
void MetricManager::Run() {
  prctl(PR_SET_NAME, "metric-manager");
  while (running_) {
    LOG_INFO("Start report metric");
    PrintMetric();
    std::this_thread::sleep_for(std::chrono::minutes(constants::kMetricIntervalMinutes));
  }
}
void MetricManager::PrintMetric() {
  std::unordered_map<std::string, Metric> stat_map;
  {
    std::lock_guard<std::mutex> lck(mutex_);
    stat_map.swap(stat_map_);
  }

  LOG_INFO("[MetricManager] Environment info: " << environment_.ToString());

  for (auto it : stat_map) {
    LOG_INFO("[MetricManager] Metric info: " << it.first << " " << it.second.ToString());
  }
}
}  // namespace inlong
