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

#include "tcp_client.h"

#include <utility>

#include "../manager/buffer_manager.h"
#include "../manager/metric_manager.h"
#include "../utils/utils.h"

namespace inlong {
#define CLIENT_INFO client_info_ << "[" << status_ << "]"
TcpClient::TcpClient(IOContext &io_context, std::string ip, uint32_t port)
    : socket_(std::make_shared<asio::ip::tcp::socket>(io_context)),
      wait_timer_(std::make_shared<asio::steady_timer>(io_context)),
      keep_alive_timer_(std::make_shared<asio::steady_timer>(io_context)),
      ip_(ip),
      port_(port),
      endpoint_(asio::ip::address::from_string(ip), port),
      status_(kUndefined),
      recv_buf_(new BlockMemory()),
      exit_(false),
      proxy_loads_(30),
      wait_heart_beat_(false),
      reset_client_(false),
      heart_beat_index_(0),
      only_heart_heat_(false),
      need_retry_(false),
      retry_times_(0) {
  client_info_ = " [" + ip_ + ":" + std::to_string(port_) + "]";

  tcp_detection_interval_ = SdkConfig::getInstance()->tcp_detection_interval_;
  tcp_idle_time_ = SdkConfig::getInstance()->tcp_idle_time_;
  last_update_time_ = Utils::getCurrentMsTime();

  keep_alive_timer_->expires_after(std::chrono::milliseconds(tcp_detection_interval_));
  keep_alive_timer_->async_wait(std::bind(&TcpClient::DetectStatus, this, std::placeholders::_1));

  LOG_INFO("TcpClient At remote info .status:" << status_ << client_info_);
  AsyncConnect();
}
TcpClient::~TcpClient() {
  status_ = kStopped;
  exit_ = true;
  try {
    asio::error_code ignored_ec;
    if (socket_) {
      socket_->cancel(ignored_ec);
      if (socket_->is_open()) {
        socket_->close(ignored_ec);
      }
    }
    if (wait_timer_) {
      wait_timer_->cancel(ignored_ec);
    }
    if (keep_alive_timer_) {
      keep_alive_timer_->cancel(ignored_ec);
    }
  } catch (std::exception &e) {
    LOG_ERROR("~TcpClient exception." << e.what() << CLIENT_INFO);
  }
}
void TcpClient::DoClose() {
  status_ = kStopped;
  exit_ = true;
  LOG_INFO("Closed client." << CLIENT_INFO);
}

void TcpClient::AsyncConnect() {
  if (kStopped == status_ || exit_) {
    return;
  }
  last_update_time_ = Utils::getCurrentMsTime();
  try {
    if (socket_->is_open()) {
      asio::error_code error;
      socket_->close(error);
      if (asio::error::operation_aborted == error) {
        return;
      }
    }
    status_ = kConnecting;
    LOG_INFO("Began to connect." << CLIENT_INFO);
  } catch (std::exception &e) {
    LOG_ERROR("AsyncConnect exception." << e.what() << CLIENT_INFO);
  }

  socket_->async_connect(endpoint_, std::bind(&TcpClient::OnConnected, this, std::placeholders::_1));
}

void TcpClient::DoAsyncConnect(asio::error_code error) {
  if (kStopped == status_ || exit_) {
    return;
  }
  if (error) {
    if (asio::error::operation_aborted == error) {
      return;
    }
  }
  AsyncConnect();
}

void TcpClient::OnConnected(asio::error_code error) {
  if (kStopped == status_ || exit_) {
    return;
  }
  if (!error) {
    socket_->set_option(asio::ip::tcp::no_delay(true));
    asio::socket_base::keep_alive option(true);
    socket_->set_option(option);
    LOG_INFO("Client has connected." << CLIENT_INFO);
    if (need_retry_) {
      LOG_WARN("Client has connected retry! times:" << retry_times_ << CLIENT_INFO);
      write(sendBuffer_, true);
      return;
    }
    status_ = kFree;
    return;
  }
  if (asio::error::operation_aborted == error) {
    return;
  }
  status_ = kConnectFailed;
  LOG_ERROR("Connect has error:" << error.message() << CLIENT_INFO);
  if (need_retry_) {
    ResetSendBuffer();
  }
  wait_timer_->expires_after(std::chrono::milliseconds(kConnectTimeout));
  wait_timer_->async_wait(std::bind(&TcpClient::DoAsyncConnect, this, std::placeholders::_1));
}

void TcpClient::write(SendBufferPtrT sendBuffer, bool retry) {
  if (kStopped == status_ || exit_) {
    LOG_ERROR("Stop.At." << CLIENT_INFO);
    return;
  }
  if (status_ != kFree && !retry) {
    LOG_WARN("Not free ." << CLIENT_INFO);
    return;
  }
  sendBuffer_ = std::move(sendBuffer);
  BeginWrite();
}

void TcpClient::BeginWrite() {
  if (sendBuffer_ == nullptr) {
    status_ = kFree;
    return;
  }
  last_update_time_ = Utils::getCurrentMsTime();
  status_ = kWriting;
  asio::async_write(*socket_, asio::buffer(sendBuffer_->GetData(), sendBuffer_->GetDataLen()),
                    std::bind(&TcpClient::OnWroten, this, std::placeholders::_1, std::placeholders::_2));
}
void TcpClient::OnWroten(const asio::error_code error, std::size_t bytes_transferred) {
  if (kStopped == status_ || exit_) {
    return;
  }
  if (error) {
    if (asio::error::operation_aborted == error) {
      return;
    }
    LOG_ERROR("Write error:" << error.message() << CLIENT_INFO);
    status_ = kWriting;
    HandleFail();
    return;
  }

  if (0 == bytes_transferred) {
    LOG_ERROR("Transferred 0 bytes." << CLIENT_INFO);
    status_ = kWaiting;
    HandleFail();
    return;
  }

  status_ = kClientResponse;
  asio::async_read(*socket_, asio::buffer(recv_buf_->m_data, sizeof(uint32_t)),
                   std::bind(&TcpClient::OnReturn, this, std::placeholders::_1, std::placeholders::_2));
}
void TcpClient::OnReturn(asio::error_code error, std::size_t len) {
  if (kStopped == status_ || exit_) {
    return;
  }
  if (error) {
    if (asio::error::operation_aborted == error) {
      return;
    }
    LOG_ERROR("OnReturn error:" << error.message() << CLIENT_INFO);
    status_ = kWaiting;
    std::fill(proxy_loads_.begin(), proxy_loads_.end(), 0);
    HandleFail();
    return;
  }
  if (len != sizeof(uint32_t)) {
    status_ = kWaiting;
    HandleFail();
    return;
  }
  size_t resp_len = ntohl(*reinterpret_cast<const uint32_t *>(recv_buf_->m_data));

  if (resp_len > recv_buf_->m_max_size) {
    status_ = kWaiting;
    HandleFail();
    return;
  }
  asio::async_read(*socket_, asio::buffer(recv_buf_->m_data + sizeof(uint32_t), resp_len),
                   std::bind(&TcpClient::OnBody, this, std::placeholders::_1, std::placeholders::_2));
}

void TcpClient::OnBody(asio::error_code error, size_t bytesTransferred) {
  if (kStopped == status_ || exit_) {
    return;
  }

  if (error) {
    if (asio::error::operation_aborted == error) {
      return;
    }
    LOG_ERROR("OnBody error:" << error.message() << CLIENT_INFO);
    status_ = kWaiting;
    HandleFail();
    return;
  }
  uint32_t parse_index = sizeof(uint32_t);
  uint8_t msg_type = *reinterpret_cast<const uint8_t *>(recv_buf_->m_data + parse_index);

  switch (msg_type) {
    case 8:
      ParseHeartBeat(bytesTransferred);
      break;
    default:
      ParseGenericResponse();
      break;
  }

  ResetRetry();

  if (wait_heart_beat_) {
    HeartBeat();
    wait_heart_beat_ = false;
    return;
  }

  if (reset_client_) {
    RestClient();
    reset_client_ = false;
  }

  status_ = kFree;
}

void TcpClient::HandleFail() {
  if (kStopped == status_ || exit_) {
    return;
  }

  status_ = kConnecting;
  ResetSendBuffer();

  int retry_interval = std::min(retry_times_ * constants::kRetryIntervalMs, constants::kMaxRetryIntervalMs);

  wait_timer_->expires_after(std::chrono::milliseconds(retry_interval));
  wait_timer_->async_wait(std::bind(&TcpClient::DoAsyncConnect, this, std::placeholders::_1));
}

void TcpClient::DetectStatus(const asio::error_code error) {
  if (kStopped == status_ || exit_) {
    return;
  }
  if (error) {
    return;
  }
  if (!only_heart_heat_) {
    UpdateMetric();
  }

  if ((Utils::getCurrentMsTime() - last_update_time_) > tcp_idle_time_ && status_ != kConnecting) {
    std::fill(proxy_loads_.begin(), proxy_loads_.end(), 0);
    LOG_INFO("Reconnect because it has idle " << tcp_idle_time_ << " ms."
                                              << "last send time:" << last_update_time_ << CLIENT_INFO);
    AsyncConnect();
  }

  keep_alive_timer_->expires_after(std::chrono::milliseconds(tcp_detection_interval_));
  keep_alive_timer_->async_wait(std::bind(&TcpClient::DetectStatus, this, std::placeholders::_1));
}

void TcpClient::UpdateMetric() {
  Metric stat;
  for (auto &it : stat_map_) {
    MetricManager::GetInstance()->UpdateMetric(it.first, it.second);
    stat.Update(it.second);
    it.second.ResetStat();
  }
  LOG_INFO(stat.GetSendMetricInfo() << CLIENT_INFO);
}

void TcpClient::HeartBeat(bool only_heart_heat) {
  if (kStopped == status_ || exit_) {
    return;
  }
  only_heart_heat_ = only_heart_heat;
  status_ = kHeartBeat;
  last_update_time_ = Utils::getCurrentMsTime();

  bin_hb_.total_len = htonl(sizeof(BinaryHB) - 4);
  bin_hb_.msg_type = 8;
  bin_hb_.data_time = htonl(static_cast<uint32_t>(Utils::getCurrentMsTime() / 1000));
  bin_hb_.body_ver = 1;
  bin_hb_.body_len = 0;
  bin_hb_.attr_len = 0;
  bin_hb_.magic = htons(constants::kBinaryMagic);
  char *hb = (char *)&bin_hb_;
  uint32_t hb_len = sizeof(bin_hb_);

  asio::async_write(*socket_, asio::buffer(hb, hb_len),
                    std::bind(&TcpClient::OnWroten, this, std::placeholders::_1, std::placeholders::_2));
}

void TcpClient::ParseHeartBeat(size_t total_length) {
  //  | total length(4) | msg type(1) | data time(4) | body version(1) | body length (4) | body | attr length(2) | attr
  //  | magic (2) |
  //  skip total length
  uint32_t parse_index = sizeof(uint32_t);
  //  skip msg type
  parse_index += sizeof(uint8_t);
  //  skip data time
  //  uint32_t data_time = ntohl(*reinterpret_cast<const uint32_t *>(recv_buf_->m_data + parse_index));
  parse_index += sizeof(uint32_t);

  // 3、parse body version
  uint32_t body_version = *reinterpret_cast<const uint8_t *>(recv_buf_->m_data + parse_index);
  parse_index += sizeof(uint8_t);

  // 4、parse body length
  uint32_t body_length = ntohl(*reinterpret_cast<const uint32_t *>(recv_buf_->m_data + parse_index));
  parse_index += sizeof(uint32_t);

  // 5 parse load
  int16_t load = ntohs(*reinterpret_cast<const int16_t *>(recv_buf_->m_data + parse_index));
  parse_index += sizeof(int16_t);

  // 7 parse attr length
  uint16_t attr_length = ntohs(*reinterpret_cast<const uint16_t *>(recv_buf_->m_data + parse_index));
  parse_index += sizeof(uint16_t);

  // 8 skip attr
  parse_index += attr_length;

  // 9 parse magic
  uint16_t magic = ntohs(*reinterpret_cast<const uint16_t *>(recv_buf_->m_data + parse_index));
  parse_index += sizeof(uint16_t);

  if (magic != constants::kBinaryMagic) {
    LOG_ERROR("Failed to ParseMsg heartbeat ack! error magic " << magic << " !=" << constants::kBinaryMagic
                                                               << CLIENT_INFO);
    return;
  }

  if (total_length + 4 != parse_index) {
    LOG_ERROR("Failed to ParseMsg heartbeat ack! total_length " << total_length << " +4 !=" << parse_index
                                                                << CLIENT_INFO);
    return;
  }
  if (heart_beat_index_ > constants::MAX_STAT) {
    heart_beat_index_ = 0;
  }
  if (body_version == 1 && body_length == 2) {
    proxy_loads_[heart_beat_index_++ % 30] = load;
  } else {
    proxy_loads_[heart_beat_index_++ % 30] = 0;
  }
  LOG_INFO("current load is " << load << CLIENT_INFO);
}

void TcpClient::ParseGenericResponse() {
  if (sendBuffer_ != nullptr) {
    std::string stat_key = sendBuffer_->GetInlongGroupId() + kStatJoiner + sendBuffer_->GetInlongStreamId();
    stat_map_[stat_key].AddSendSuccessMsgNum(sendBuffer_->GetMsgCnt());
    stat_map_[stat_key].AddSendSuccessPackNum(1);
    stat_map_[stat_key].AddTimeCost(Utils::getCurrentMsTime() - last_update_time_);

    BufferManager::GetInstance()->AddSendBuffer(sendBuffer_);
  }
}

int32_t TcpClient::GetAvgLoad() {
  int32_t numerator = 0;
  int32_t denominator = 0;
  for (int i = 0; i < proxy_loads_.size(); i++) {
    if (proxy_loads_[i] > 0) {
      numerator += proxy_loads_[i] * constants::kWeight[i];
      denominator += constants::kWeight[i];
    }
  }
  int32_t avg_load = 0;
  if (0 == denominator) {
    return avg_load;
  }
  avg_load = numerator / denominator;
  LOG_INFO("average load is " << avg_load << CLIENT_INFO);
  return avg_load;
}

void TcpClient::SetHeartBeatStatus() { wait_heart_beat_ = true; }

void TcpClient::UpdateClient(const std::string ip, const uint32_t port) {
  LOG_INFO("UpdateClient[" << only_heart_heat_ << "][" << ip << ":" << port << "] replace" << CLIENT_INFO);
  ip_ = ip;
  port_ = port;
  reset_client_ = true;
}
void TcpClient::RestClient() {
  std::fill(proxy_loads_.begin(), proxy_loads_.end(), 0);
  asio::ip::tcp::endpoint endpoint(asio::ip::address::from_string(ip_), port_);
  endpoint_ = endpoint;
  client_info_ = " [" + ip_ + ":" + std::to_string(port_) + "]";

  LOG_INFO("RestClient[" << only_heart_heat_ << "]" << CLIENT_INFO);

  AsyncConnect();
}
const std::string &TcpClient::getIp() const { return ip_; }
const std::string &TcpClient::getClientInfo() const { return client_info_; }
uint32_t TcpClient::getPort() const { return port_; }
void TcpClient::ResetRetry() {
  need_retry_ = false;
  retry_times_ = 0;
}
void TcpClient::ResetSendBuffer() {
  if (sendBuffer_ == nullptr) {
    return;
  }
  retry_times_++;
  if (retry_times_ > SdkConfig::getInstance()->retry_times_) {
    std::string stat_key = sendBuffer_->GetInlongGroupId() + kStatJoiner + sendBuffer_->GetInlongStreamId();
    stat_map_[stat_key].AddSendFailMsgNum(sendBuffer_->GetMsgCnt());
    stat_map_[stat_key].AddSendFailPackNum(1);
    stat_map_[stat_key].AddTimeCost(Utils::getCurrentMsTime() - last_update_time_);

    sendBuffer_->doUserCallBack();

    BufferManager::GetInstance()->AddSendBuffer(sendBuffer_);

    LOG_INFO("resend to proxy fail! retry times:" << retry_times_ << CLIENT_INFO);
    ResetRetry();
  } else {
    need_retry_ = true;
  }
}
}  // namespace inlong
