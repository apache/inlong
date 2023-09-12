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

#ifndef INLONG_SDK_SEND_GROUP_H
#define INLONG_SDK_SEND_GROUP_H

#include "../config/proxy_info.h"
#include "../utils/send_buffer.h"
#include "../client/tcp_client.h"
#include <queue>
namespace inlong {
const int kTimerMiSeconds = 10;
const int kTimerMinute = 60000;

using SteadyTimerPtr = std::shared_ptr<asio::steady_timer>;
using IOContext = asio::io_context;
using io_context_work =
    asio::executor_work_guard<asio::io_context::executor_type>;

class SendGroup : noncopyable {
private:
  IOContext io_context_;
  io_context_work work_;
  std::thread thread_;
  void Run();

public:
  std::shared_ptr<std::vector<TcpClientTPtrT>> tcp_clients_;
  std::shared_ptr<std::vector<TcpClientTPtrT>> tcp_clients_old_;

  ProxyInfoVec current_proxy_vec_;
  TcpClientTPtrVecItT current_client_;
  std::queue<SendBufferPtrT> send_buf_list_;

  SteadyTimerPtr send_timer_;
  SteadyTimerPtr update_conf_timer_;

  read_write_mutex send_group_mutex_;
  read_write_mutex remote_proxy_list_mutex_;
  std::mutex mutex_;
  std::string group_id_;

  std::uint32_t send_idx_;
  uint32_t max_send_queue_num_;

  SendGroup(std::string group_id);
  ~SendGroup();

  void PreDispatchData(std::error_code error);
  void DispatchData(std::error_code error);
  bool IsFull();
  uint32_t PushData(SendBufferPtrT send_buffer_ptr);
  SendBufferPtrT PopData();
  uint32_t GetQueueSize();
  void UpdateConf(std::error_code error);
  bool IsConfChanged(ProxyInfoVec &current_proxy_vec,
                     ProxyInfoVec &new_proxy_vec);
  bool IsAvailable();
  uint32_t dispatch_interval_;

  void ClearOldTcpClients();
};
using SendGroupPtr = std::shared_ptr<SendGroup>;
} // namespace inlong

#endif // INLONG_SDK_SEND_GROUP_H
