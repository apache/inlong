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

#ifndef INLONG_SDK_SEND_GROUP_H
#define INLONG_SDK_SEND_GROUP_H

#include <queue>
#include <unordered_map>

#include "../config/proxy_info.h"
#include "../utils/send_buffer.h"
#include "../client/tcp_client.h"

namespace inlong {
const int kTimerMiSeconds = 10;
const int kTimerMinute = 60000;

using SteadyTimerPtr = std::shared_ptr<asio::steady_timer>;
using IOContext = asio::io_context;
using io_context_work = asio::executor_work_guard<asio::io_context::executor_type>;

class SendGroup : noncopyable {
 private:
  IOContext io_context_;
  io_context_work work_;  // 保持io_context.run()在无任何任务时不退出
  std::thread thread_;
  void Run();
  uint64_t max_proxy_num_;

 public:
  std::shared_ptr<std::vector<TcpClientTPtrT>> work_clients_;
  std::shared_ptr<std::vector<TcpClientTPtrT>> work_clients_old_;
  std::vector<TcpClientTPtrT> reserve_clients_;

  ProxyInfoVec current_proxy_vec_;
  std::queue<SendBufferPtrT> send_proxy_list_;

  SteadyTimerPtr send_timer_;
  SteadyTimerPtr update_conf_timer_;
  SteadyTimerPtr load_balance_timer_;

  read_write_mutex send_group_mutex_;
  read_write_mutex work_clients_mutex_;
  read_write_mutex reserve_clients_mutex_;
  std::mutex mutex_;
  std::string send_group_key_;

  std::uint32_t send_idx_;
  uint32_t max_send_queue_num_;

  uint32_t dispatch_interval_;
  uint32_t heart_beat_interval_;
  uint32_t load_balance_interval_;
  uint32_t dispatch_stat_;
  volatile bool need_balance_;
  volatile int32_t load_threshold_;

  SendGroup(std::string send_group_key);
  ~SendGroup();

  void PreDispatchData(std::error_code error);
  void DispatchData(std::error_code error);
  bool IsFull();
  uint32_t PushData(const SendBufferPtrT &send_buffer_ptr);
  SendBufferPtrT PopData();
  uint32_t GetQueueSize();
  void UpdateConf(std::error_code error);
  bool IsConfChanged(ProxyInfoVec &current_proxy_vec, ProxyInfoVec &new_proxy_vec);
  bool IsAvailable();
  void ClearOldTcpClients();

  // balance
  bool NeedDoLoadBalance();
  void LoadBalance(std::error_code error);
  void DoLoadBalance();
  void HeartBeat();
  ProxyInfo GetRandomProxy(const std::string &ip = "", uint32_t port = 0);
  TcpClientTPtrT GetReserveClient();
  TcpClientTPtrT GetMaxLoadClient();
  void InitReserveClient();
  static bool UpSort(const TcpClientTPtrT &begin, const TcpClientTPtrT &end);
  bool ExistInWorkClient(const std::string &ip, uint32_t port);
};
using SendGroupPtr = std::shared_ptr<SendGroup>;
}  // namespace inlong

#endif  // INLONG_SDK_SEND_GROUP_H
