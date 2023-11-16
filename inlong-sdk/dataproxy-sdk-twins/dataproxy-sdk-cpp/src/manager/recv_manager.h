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

#ifndef INLONG_RECV_MANAGER_H
#define INLONG_RECV_MANAGER_H

#include "../group/recv_group.h"
#include "../utils/noncopyable.h"
#include "send_manager.h"
#include <thread>
#include <unordered_map>

namespace inlong {
using SteadyTimerPtr = std::shared_ptr<asio::steady_timer>;
using IOContext = asio::io_context;
using io_context_work =
    asio::executor_work_guard<asio::io_context::executor_type>;

class RecvManager : noncopyable {
private:
  IOContext io_context_;
  io_context_work work_;
  std::thread thread_;

  std::unordered_map<std::string, RecvGroupPtr> recv_group_map_;
  mutable std::mutex mutex_;
  bool exit_flag_;
  std::shared_ptr<SendManager> send_manager_;
  SteadyTimerPtr check_timer_;

  uint32_t dispatch_interval_;

  uint64_t max_groupid_streamid_num_;

  void Run();

public:
  RecvManager(std::shared_ptr<SendManager> send_manager);
  ~RecvManager();
  void DispatchData(std::error_code error);
  RecvGroupPtr GetRecvGroup(const std::string &bid);
};
} // namespace inlong

#endif // INLONG_RECV_MANAGER_H
