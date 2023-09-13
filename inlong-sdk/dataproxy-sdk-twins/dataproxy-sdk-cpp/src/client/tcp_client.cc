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

#include "tcp_client.h"
#include "../utils/utils.h"
#include "api_code.h"
#include <utility>

namespace inlong {
#define CLIENT_INFO client_info_ << "[" << status_ << "]"
TcpClient::TcpClient(IOContext &io_context, std::string ip, uint32_t port)
    : socket_(std::make_shared<asio::ip::tcp::socket>(io_context)),
      wait_timer_(std::make_shared<asio::steady_timer>(io_context)),
      keep_alive_timer_(std::make_shared<asio::steady_timer>(io_context)),
      ip_(ip), port_(port), endpoint_(asio::ip::address::from_string(ip), port),
      status_(kUndefined), recv_buf_(new BlockMemory()), exit_(false) {
  ;
  client_info_ = " [" + ip_ + ":" + std::to_string(port_) + "]";

  tcp_detection_interval_ = SdkConfig::getInstance()->tcp_detection_interval_;
  tcp_idle_time_ = SdkConfig::getInstance()->tcp_idle_time_;
  last_update_time_ = Utils::getCurrentMsTime();

  keep_alive_timer_->expires_after(
      std::chrono::milliseconds(tcp_detection_interval_));
  keep_alive_timer_->async_wait(
      std::bind(&TcpClient::DetectStatus, this, std::placeholders::_1));

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
  LOG_INFO("closed client." << CLIENT_INFO);
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
        // operation aborted
        return;
      }
    }
    status_ = kConnecting;
    LOG_INFO("began to connect." << CLIENT_INFO);
  } catch (std::exception &e) {
    LOG_ERROR("AsyncConnect exception." << e.what() << CLIENT_INFO);
  }

  socket_->async_connect(endpoint_, std::bind(&TcpClient::OnConnected, this,
                                              std::placeholders::_1));
}

void TcpClient::DoAsyncConnect(asio::error_code error) {
  if (kStopped == status_ || exit_) {
    return;
  }
  if (error) {
    if (asio::error::operation_aborted == error) {
      // operation aborted必
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
    LOG_INFO("client has connected." << CLIENT_INFO);
    status_ = kFree;
    return;
  }
  if (asio::error::operation_aborted == error) {
    // operation aborted
    return;
  }
  status_ = kConnectFailed;
  LOG_ERROR("connect has error:" << error.message() << CLIENT_INFO);
  wait_timer_->expires_after(std::chrono::milliseconds(kConnectTimeout));
  wait_timer_->async_wait(
      std::bind(&TcpClient::DoAsyncConnect, this, std::placeholders::_1));
}

void TcpClient::write(SendBufferPtrT sendBuffer) {
  if (kStopped == status_ || exit_) {
    LOG_ERROR("Stop.At." << CLIENT_INFO);
    return;
  }
  if (status_ != kFree) {
    LOG_WARN("Not free ." << CLIENT_INFO);
    return;
  }
  sendBuffer_ = sendBuffer;
  BeginWrite();
}

void TcpClient::BeginWrite() {
  if (sendBuffer_ == nullptr) {
    status_ = kFree;
    return;
  }
  last_update_time_ = Utils::getCurrentMsTime();
  status_ = kWriting;
  asio::async_write(*socket_,
                    asio::buffer(sendBuffer_->content(), sendBuffer_->len()),
                    std::bind(&TcpClient::OnWroten, this, std::placeholders::_1,
                              std::placeholders::_2));
}
void TcpClient::OnWroten(const asio::error_code error,
                         std::size_t bytes_transferred) {
  if (kStopped == status_ || exit_) {
    return;
  }
  if (error) {
    if (asio::error::operation_aborted == error) {
      // operation aborted
      return;
    }
    LOG_ERROR("write error:" << error.message() << CLIENT_INFO);
    status_ = kWriting;
    HandleFail();
    return;
  }

  if (0 == bytes_transferred) {
    LOG_ERROR("transferred 0 bytes." << CLIENT_INFO);
    status_ = kWaiting;
    HandleFail();
    return;
  }

  status_ = CLIENT_RESPONSE;
  asio::async_read(*socket_, asio::buffer(recv_buf_->m_data, sizeof(uint32_t)),
                   std::bind(&TcpClient::OnReturn, this, std::placeholders::_1,
                             std::placeholders::_2));
}
void TcpClient::OnReturn(asio::error_code error, std::size_t len) {
  if (kStopped == status_ || exit_) {
    return;
  }
  if (error) {
    if (asio::error::operation_aborted == error) {
      // operation aborted
      return;
    }
    LOG_ERROR("OnReturn error:" << error.message() << CLIENT_INFO);
    status_ = kWaiting;
    HandleFail();
    return;
  }
  if (len != sizeof(uint32_t)) {
    status_ = kWaiting;
    HandleFail();
    return;
  }
  size_t resp_len =
      ntohl(*reinterpret_cast<const uint32_t *>(recv_buf_->m_data));

  if (resp_len > recv_buf_->m_max_size) {
    status_ = kWaiting;
    HandleFail();
    return;
  }
  asio::async_read(*socket_, asio::buffer(recv_buf_->m_data, resp_len),
                   std::bind(&TcpClient::OnBody, this, std::placeholders::_1,
                             std::placeholders::_2));
}

void TcpClient::OnBody(asio::error_code error, size_t bytesTransferred) {
  if (kStopped == status_ || exit_) {
    return;
  }

  if (error) {
    if (asio::error::operation_aborted == error) {
      // operation aborted
      return;
    }
    LOG_ERROR("OnBody error:" << error.message() << CLIENT_INFO);
    status_ = kWaiting;
    HandleFail();
    return;
  }

  if (sendBuffer_ != nullptr) {
    stat_.AddSendSuccessMsgNum(sendBuffer_->msgCnt());
    stat_.AddSendSuccessPackNum(1);

    sendBuffer_->releaseBuf();
  }

  status_ = kFree;
}

void TcpClient::HandleFail() {
  if (kStopped == status_ || exit_) {
    return;
  }

  status_ = kConnecting;
  if (sendBuffer_ != nullptr) {
    stat_.AddSendFailMsgNum(sendBuffer_->msgCnt());
    stat_.AddSendFailPackNum(1);

    sendBuffer_->doUserCallBack();
    sendBuffer_->releaseBuf();
  }

  AsyncConnect();
}

void TcpClient::DetectStatus(const asio::error_code error) {
  if (kStopped == status_ || exit_) {
    return;
  }
  if (error) {
    return;
  }

  LOG_INFO(stat_.ToString() << CLIENT_INFO);
  stat_.ResetStat();

  if ((Utils::getCurrentMsTime() - last_update_time_) > tcp_idle_time_ &&
      status_ != kConnecting) {
    LOG_INFO("reconnect because it has idle "
             << tcp_idle_time_ << " ms."
             << "last send time:" << last_update_time_ << CLIENT_INFO);
    AsyncConnect();
  }

  keep_alive_timer_->expires_after(
      std::chrono::milliseconds(tcp_detection_interval_));
  keep_alive_timer_->async_wait(
      std::bind(&TcpClient::DetectStatus, this, std::placeholders::_1));
}

} // namespace inlong
