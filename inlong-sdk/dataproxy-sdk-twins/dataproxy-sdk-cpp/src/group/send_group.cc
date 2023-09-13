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

#include "send_group.h"
#include "api_code.h"
#include "proxy_manager.h"
#include <algorithm>
#include <random>

namespace inlong {
const int kDefaultQueueSize = 20;
SendGroup::SendGroup(std::string group_id)
    : work_(asio::make_work_guard(io_context_)), group_id_(group_id),
      send_idx_(0) {
  max_send_queue_num_ = SdkConfig::getInstance()->send_buf_size_ /
                        SdkConfig::getInstance()->pack_size_;
  if (max_send_queue_num_ <= 0) {
    max_send_queue_num_ = kDefaultQueueSize;
  }
  LOG_INFO("SendGroup  max_send_queue_num " << max_send_queue_num_);
  dispatch_interval_ = SdkConfig::getInstance()->dispatch_interval_send_;
  tcp_clients_old_ = nullptr;
  tcp_clients_ = std::make_shared<std::vector<TcpClientTPtrT>>();
  tcp_clients_->reserve(SdkConfig::getInstance()->max_proxy_num_);

  send_timer_ = std::make_shared<asio::steady_timer>(io_context_);
  send_timer_->expires_after(std::chrono::milliseconds(dispatch_interval_));
  send_timer_->async_wait(
      std::bind(&SendGroup::PreDispatchData, this, std::placeholders::_1));

  update_conf_timer_ = std::make_shared<asio::steady_timer>(io_context_);
  update_conf_timer_->expires_after(std::chrono::milliseconds(1));
  update_conf_timer_->async_wait(
      std::bind(&SendGroup::UpdateConf, this, std::placeholders::_1));

  current_proxy_vec_.reserve(SdkConfig::getInstance()->max_proxy_num_);
  thread_ = std::thread(&SendGroup::Run, this);
}
SendGroup::~SendGroup() {
  LOG_INFO("~SendGroup ");
  send_timer_->cancel();
  update_conf_timer_->cancel();
  io_context_.stop();
  if (thread_.joinable()) {
    thread_.join();
  }
}
void SendGroup::Run() { io_context_.run(); }
void SendGroup::PreDispatchData(std::error_code error) {
  if (error) {
    return;
  }
  send_timer_->expires_after(std::chrono::milliseconds(dispatch_interval_));
  send_timer_->async_wait(
      std::bind(&SendGroup::DispatchData, this, std::placeholders::_1));
}

void SendGroup::DispatchData(std::error_code error) {
  if (error) {
    return;
  }
  try {
    unique_read_lock<read_write_mutex> rdlck(remote_proxy_list_mutex_);
    if (tcp_clients_ != nullptr) {
      if (send_idx_ >= tcp_clients_->size()) {
        send_idx_ = 0;
      }

      while (send_idx_ < tcp_clients_->size()) {
        if ((*tcp_clients_)[send_idx_]->isFree()) {
          SendBufferPtrT send_buf = PopData();
          if (send_buf == nullptr) {
            break;
          }
          (*tcp_clients_)[send_idx_]->write(send_buf);
        }
        send_idx_++;
      }
    }
  } catch (std::exception &e) {
    LOG_ERROR("Exception " << e.what());
  }
  send_timer_->expires_after(std::chrono::milliseconds(dispatch_interval_));
  send_timer_->async_wait(
      std::bind(&SendGroup::DispatchData, this, std::placeholders::_1));
}

bool SendGroup::IsFull() { return GetQueueSize() > max_send_queue_num_; }

uint32_t SendGroup::PushData(SendBufferPtrT send_buffer_ptr) {
  if (IsFull()) {
    return SdkCode::kSendBufferFull;
  }
  std::lock_guard<std::mutex> lock(mutex_);
  send_buf_list_.push(send_buffer_ptr);
  return SdkCode::kSuccess;
}

void SendGroup::UpdateConf(std::error_code error) {
  if (error) {
    return;
  }
  LOG_INFO("start UpdateConf.");

  ClearOldTcpClients();

  ProxyInfoVec new_proxy_info;
  if (ProxyManager::GetInstance()->GetProxy(group_id_, new_proxy_info) !=
          kSuccess ||
      new_proxy_info.empty()) {
    update_conf_timer_->expires_after(std::chrono::milliseconds(kTimerMinute));
    update_conf_timer_->async_wait(
        std::bind(&SendGroup::UpdateConf, this, std::placeholders::_1));
    return;
  }

  if (!IsConfChanged(current_proxy_vec_, new_proxy_info)) {
    LOG_INFO("Don`t need UpdateConf. current proxy size("
             << current_proxy_vec_.size() << ")=proxy size("
             << new_proxy_info.size() << ")");
    update_conf_timer_->expires_after(std::chrono::milliseconds(kTimerMinute));
    update_conf_timer_->async_wait(
        std::bind(&SendGroup::UpdateConf, this, std::placeholders::_1));
    return;
  }

  uint32_t proxy_num = SdkConfig::getInstance()->max_proxy_num_;
  if (proxy_num > new_proxy_info.size()) {
    proxy_num = new_proxy_info.size();
  }

  std::shared_ptr<std::vector<TcpClientTPtrT>> tcp_clients_tmp =
      std::make_shared<std::vector<TcpClientTPtrT>>();
  if (tcp_clients_tmp == nullptr) {
    LOG_INFO("tcp_clients_tmp is  nullptr");
    update_conf_timer_->expires_after(std::chrono::milliseconds(kTimerMinute));
    update_conf_timer_->async_wait(
        std::bind(&SendGroup::UpdateConf, this, std::placeholders::_1));
    return;
  }

  std::random_shuffle(new_proxy_info.begin(), new_proxy_info.end());

  tcp_clients_tmp->reserve(proxy_num);
  for (int i = 0; i < proxy_num; i++) {
    ProxyInfo proxy_tmp = new_proxy_info[i];
    TcpClientTPtrT tcpClientTPtrT = std::make_shared<TcpClient>(
        io_context_, proxy_tmp.ip(), proxy_tmp.port());
    tcp_clients_tmp->push_back(tcpClientTPtrT);
    LOG_INFO("new proxy info.[" << proxy_tmp.ip() << ":" << proxy_tmp.port()
                                << "]");
  }

  {
    LOG_INFO("do change tcp clients.");
    unique_write_lock<read_write_mutex> wtlck(remote_proxy_list_mutex_);
    tcp_clients_old_ = tcp_clients_;
    tcp_clients_ = tcp_clients_tmp;
  }

  if (tcp_clients_old_ != nullptr) {
    for (int j = 0; j < tcp_clients_old_->size(); j++) {
      (*tcp_clients_old_)[j]->DoClose();
    }
  }

  current_proxy_vec_ = new_proxy_info;

  update_conf_timer_->expires_after(std::chrono::milliseconds(kTimerMinute));
  update_conf_timer_->async_wait(
      std::bind(&SendGroup::UpdateConf, this, std::placeholders::_1));

  LOG_INFO("Finished UpdateConf.");
}

SendBufferPtrT SendGroup::PopData() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (send_buf_list_.empty()) {
    return nullptr;
  }
  SendBufferPtrT send_buf = send_buf_list_.front();
  send_buf_list_.pop();
  return send_buf;
}

uint32_t SendGroup::GetQueueSize() {
  std::lock_guard<std::mutex> lock(mutex_);
  return send_buf_list_.size();
}

bool SendGroup::IsConfChanged(ProxyInfoVec &current_proxy_vec,
                              ProxyInfoVec &new_proxy_vec) {
  if (new_proxy_vec.empty())
    return false;
  if (current_proxy_vec.size() != new_proxy_vec.size()) {
    return true;
  }

  for (auto &current_bu : current_proxy_vec) {
    for (int i = 0; i < new_proxy_vec.size(); i++) {
      if ((current_bu.ip() == new_proxy_vec[i].ip()) &&
          (current_bu.port() == new_proxy_vec[i].port()))
        break;
      if (i == (new_proxy_vec.size() - 1)) {
        if ((current_bu.ip() != new_proxy_vec[i].ip() ||
             current_bu.port() == new_proxy_vec[i].port())) {
          LOG_INFO("current proxy ip." << current_bu.ip() << ":"
                                       << current_bu.port()
                                       << " can`t find in proxy.");
          return true;
        }
      }
    }
  }
  return false;
}

bool SendGroup::IsAvailable() {
  unique_read_lock<read_write_mutex> rdlck(remote_proxy_list_mutex_);
  if (tcp_clients_ == nullptr) {
    return false;
  }
  if (tcp_clients_->empty()) {
    return false;
  }
  return true;
}
void SendGroup::ClearOldTcpClients() {
  if (tcp_clients_old_ != nullptr) {
    LOG_INFO("ClearOldTcpClients." << tcp_clients_old_->size());
    tcp_clients_old_->clear();
    tcp_clients_old_.reset();
  }
}
} // namespace inlong
