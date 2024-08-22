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

#ifndef INLONG_TCP_CLIENT_H
#define INLONG_TCP_CLIENT_H

#include <queue>
#include <unordered_map>

#include "../metric/metric.h"
#include "../protocol/msg_protocol.h"
#include "../utils/block_memory.h"
#include "../utils/capi_constant.h"
#include "../utils/read_write_mutex.h"
#include "../utils/send_buffer.h"

namespace inlong {
enum ClientStatus {
  kUndefined = 0,
  kConnecting = 1,
  kWriting = 2,
  kFree = 3,
  kConnectFailed = 4,
  kWaiting = 5,
  kStopped = 6,
  kClientResponse = 7,
  kHeartBeat = 8
};

enum {
  kConnectTimeout = 1000 * 20,
};
using IOContext = asio::io_context;
using TcpSocketPtr = std::shared_ptr<asio::ip::tcp::socket>;
using SteadyTimerPtr = std::shared_ptr<asio::steady_timer>;
class TcpClient {
 private:
  TcpSocketPtr socket_;
  SteadyTimerPtr wait_timer_;
  SteadyTimerPtr keep_alive_timer_;
  ClientStatus status_;
  std::string ip_;

 public:
  const std::string &getIp() const;

 private:
  uint32_t port_;

 public:
  uint32_t getPort() const;

 private:
  std::string client_info_;

 public:
  const std::string &getClientInfo() const;

 private:
  std::shared_ptr<SendBuffer> sendBuffer_;
  asio::ip::tcp::endpoint endpoint_;
  BlockMemoryPtrT recv_buf_;
  uint64_t tcp_idle_time_;
  uint32_t tcp_detection_interval_;
  uint64_t last_update_time_;
  Metric stat_;
  bool exit_;
  BinaryHB bin_hb_ = {0};
  std::vector<int32_t> proxy_loads_;
  uint32_t heart_beat_index_;
  bool wait_heart_beat_;
  bool reset_client_;
  volatile bool only_heart_heat_;
  bool need_retry_;
  uint32_t retry_times_;
  std::unordered_map <std::string ,Metric> stat_map_;

 public:
  TcpClient(IOContext &io_context, std::string ip, uint32_t port);
  ~TcpClient();
  void AsyncConnect();
  void DoAsyncConnect(asio::error_code error);
  void OnConnected(asio::error_code error);
  void BeginWrite();
  void OnWroten(const asio::error_code error, std::size_t bytes_transferred);
  void OnReturn(asio::error_code error, std::size_t len);
  void OnBody(asio::error_code error, size_t bytesTransferred);
  void DoClose();
  void HandleFail();
  bool isFree() { return (status_ == kFree); }
  void write(SendBufferPtrT sendBuffer, bool retry = false);
  void DetectStatus(const asio::error_code error);
  void HeartBeat(bool only_heart_heat = false);
  void SetHeartBeatStatus();
  void ParseHeartBeat(size_t total_length);
  void ParseGenericResponse();
  void UpdateClient(const std::string ip, const uint32_t port);
  void RestClient();
  int32_t GetAvgLoad();
  void ResetRetry();
  void ResetSendBuffer();
  void UpdateMetric();
};
typedef std::shared_ptr<TcpClient> TcpClientTPtrT;
typedef std::vector<TcpClientTPtrT> TcpClientTPtrVecT;
typedef TcpClientTPtrVecT::iterator TcpClientTPtrVecItT;
}  // namespace inlong

#endif  // INLONG_TCP_CLIENT_H
