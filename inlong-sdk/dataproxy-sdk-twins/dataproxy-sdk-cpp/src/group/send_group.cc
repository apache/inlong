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

#include "send_group.h"
#include <sys/prctl.h>
#include <algorithm>
#include <random>
#include "../core/api_code.h"
#include "../manager/proxy_manager.h"

namespace inlong {
const uint32_t kDefaultQueueSize = 200;
SendGroup::SendGroup(std::string send_group_key)
    : work_(asio::make_work_guard(io_context_)),
      send_group_key_(send_group_key),
      send_idx_(0),
      dispatch_stat_(0),
      load_threshold_(0),
      max_proxy_num_(0) {
  max_send_queue_num_ = SdkConfig::getInstance()->send_buf_size_ / SdkConfig::getInstance()->pack_size_;
  if (max_send_queue_num_ <= kDefaultQueueSize) {
    max_send_queue_num_ = kDefaultQueueSize;
  }
  max_send_queue_num_ = std::max(max_send_queue_num_, kDefaultQueueSize);
  LOG_INFO("SendGroup:" << send_group_key_ << ",max send queue num:" << max_send_queue_num_);
  dispatch_interval_ = SdkConfig::getInstance()->dispatch_interval_send_;
  load_balance_interval_ = SdkConfig::getInstance()->load_balance_interval_;
  heart_beat_interval_ = SdkConfig::getInstance()->heart_beat_interval_ / dispatch_interval_;
  need_balance_ = SdkConfig::getInstance()->enable_balance_;

  work_clients_old_ = nullptr;
  work_clients_ = std::make_shared<std::vector<TcpClientTPtrT>>();
  work_clients_->reserve(SdkConfig::getInstance()->max_proxy_num_);

  send_timer_ = std::make_shared<asio::steady_timer>(io_context_);
  send_timer_->expires_after(std::chrono::milliseconds(dispatch_interval_));
  send_timer_->async_wait(std::bind(&SendGroup::PreDispatchData, this, std::placeholders::_1));

  update_conf_timer_ = std::make_shared<asio::steady_timer>(io_context_);
  update_conf_timer_->expires_after(std::chrono::milliseconds(1));
  update_conf_timer_->async_wait(std::bind(&SendGroup::UpdateConf, this, std::placeholders::_1));

  if (SdkConfig::getInstance()->enable_balance_) {
    load_balance_timer_ = std::make_shared<asio::steady_timer>(io_context_);
    load_balance_timer_->expires_after(std::chrono::milliseconds(load_balance_interval_));
    load_balance_timer_->async_wait(std::bind(&SendGroup::LoadBalance, this, std::placeholders::_1));
  }

  current_proxy_vec_.reserve(SdkConfig::getInstance()->max_proxy_num_);
  thread_ = std::thread(&SendGroup::Run, this);
}
SendGroup::~SendGroup() {
  LOG_INFO("~SendGroup ");
  if (send_timer_) {
    send_timer_->cancel();
  }
  if (update_conf_timer_) {
    update_conf_timer_->cancel();
  }
  if (load_balance_timer_) {
    update_conf_timer_->cancel();
  }

  io_context_.stop();
  if (thread_.joinable()) {
    thread_.join();
  }
}
void SendGroup::Run() {
  prctl(PR_SET_NAME, "send-group");
  io_context_.run();
}
void SendGroup::PreDispatchData(std::error_code error) {
  if (error) {
    return;
  }
  send_timer_->expires_after(std::chrono::milliseconds(dispatch_interval_));
  send_timer_->async_wait(std::bind(&SendGroup::DispatchData, this, std::placeholders::_1));
}

void SendGroup::DispatchData(std::error_code error) {
  if (error) {
    return;
  }
  try {
    unique_read_lock<read_write_mutex> rdlck(work_clients_mutex_);
    if (work_clients_ != nullptr) {
      if (send_idx_ >= work_clients_->size()) {
        send_idx_ = 0;
      }
      while (send_idx_ < work_clients_->size()) {
        if ((*work_clients_)[send_idx_]->isFree()) {
          SendBufferPtrT send_buf = PopData();
          if (send_buf == nullptr) {
            break;
          }
          (*work_clients_)[send_idx_]->write(send_buf);
        }
        send_idx_++;
      }
    }
  } catch (std::exception &e) {
    LOG_ERROR("Exception " << e.what());
  }

  if (need_balance_ && dispatch_stat_++ > heart_beat_interval_) {
    HeartBeat();
    dispatch_stat_ = 0;
  }

  send_timer_->expires_after(std::chrono::milliseconds(dispatch_interval_));
  send_timer_->async_wait(std::bind(&SendGroup::DispatchData, this, std::placeholders::_1));
}
void SendGroup::HeartBeat() {
  unique_read_lock<read_write_mutex> rdlck(work_clients_mutex_);
  if (work_clients_ == nullptr) {
    return;
  }
  for (int idx = 0; idx < work_clients_->size(); idx++) {
    if ((*work_clients_)[idx]->isFree()) {
      (*work_clients_)[idx]->HeartBeat();
    } else {
      (*work_clients_)[idx]->SetHeartBeatStatus();
    }
  }

  for (int idx = 0; idx < reserve_clients_.size(); idx++) {
    reserve_clients_[idx]->HeartBeat(true);
  }
}

bool SendGroup::IsFull() { return GetQueueSize() > max_send_queue_num_; }

uint32_t SendGroup::PushData(const SendBufferPtrT &send_buffer_ptr) {
  if (IsFull()) {
    return SdkCode::kSendBufferFull;
  }
  std::lock_guard<std::mutex> lock(mutex_);
  send_proxy_list_.push(send_buffer_ptr);
  return SdkCode::kSuccess;
}

void SendGroup::UpdateConf(std::error_code error) {
  if (error) {
    return;
  }
  LOG_INFO("start UpdateConf.");

  ClearOldTcpClients();

  ProxyInfoVec new_proxy_info;
  if (ProxyManager::GetInstance()->GetProxy(send_group_key_, new_proxy_info) != kSuccess || new_proxy_info.empty()) {
    update_conf_timer_->expires_after(std::chrono::milliseconds(kTimerMinute));
    update_conf_timer_->async_wait(std::bind(&SendGroup::UpdateConf, this, std::placeholders::_1));
    return;
  }
  if (new_proxy_info.empty()) {
    LOG_INFO("New proxy is empty when update config!");
    return;
  }

  load_threshold_ = new_proxy_info[0].GetLoad() > constants::kDefaultLoadThreshold
                    ? constants::kDefaultLoadThreshold
                    : std::max((new_proxy_info[0].GetLoad()), 0);

  if (!IsConfChanged(current_proxy_vec_, new_proxy_info)) {
    LOG_INFO("Don`t need UpdateConf. current proxy size(" << current_proxy_vec_.size() << ")=proxy size("
                                                        << new_proxy_info.size() << ")");
    update_conf_timer_->expires_after(std::chrono::milliseconds(kTimerMinute));
    update_conf_timer_->async_wait(std::bind(&SendGroup::UpdateConf, this, std::placeholders::_1));
    return;
  }

  max_proxy_num_ = std::min(SdkConfig::getInstance()->max_proxy_num_, new_proxy_info.size());

  std::shared_ptr<std::vector<TcpClientTPtrT>> tcp_clients_tmp = std::make_shared<std::vector<TcpClientTPtrT>>();
  if (tcp_clients_tmp == nullptr) {
    LOG_INFO("Tcp clients tmp is nullptr");
    update_conf_timer_->expires_after(std::chrono::milliseconds(kTimerMinute));
    update_conf_timer_->async_wait(std::bind(&SendGroup::UpdateConf, this, std::placeholders::_1));
    return;
  }

  std::random_shuffle(new_proxy_info.begin(), new_proxy_info.end());

  tcp_clients_tmp->reserve(max_proxy_num_);
  for (int i = 0; i < max_proxy_num_; i++) {
    ProxyInfo tmp_proxy = new_proxy_info[i];
    for (int repeat_time = 0; repeat_time < SdkConfig::getInstance()->proxy_repeat_times_; repeat_time++) {
      TcpClientTPtrT tcpClientTPtrT = std::make_shared<TcpClient>(io_context_, tmp_proxy.ip(), tmp_proxy.port());
      tcp_clients_tmp->push_back(tcpClientTPtrT);
      LOG_INFO("New proxy info.[" << tmp_proxy.ip() << ":" << tmp_proxy.port() << "]");
    }
  }

  {
    LOG_INFO("Do change tcp clients.");
    unique_write_lock<read_write_mutex> wtlck(work_clients_mutex_);
    work_clients_old_ = work_clients_;
    work_clients_ = tcp_clients_tmp;
  }

  if (work_clients_old_ != nullptr) {
    for (int j = 0; j < work_clients_old_->size(); j++) {
      (*work_clients_old_)[j]->DoClose();
    }
  }

  current_proxy_vec_ = new_proxy_info;

  InitReserveClient();

  update_conf_timer_->expires_after(std::chrono::milliseconds(kTimerMinute));
  update_conf_timer_->async_wait(std::bind(&SendGroup::UpdateConf, this, std::placeholders::_1));

  LOG_INFO("Finished update send group config.");
}

SendBufferPtrT SendGroup::PopData() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (send_proxy_list_.empty()) {
    return nullptr;
  }
  SendBufferPtrT send_buf = send_proxy_list_.front();
  send_proxy_list_.pop();
  return send_buf;
}

uint32_t SendGroup::GetQueueSize() {
  std::lock_guard<std::mutex> lock(mutex_);
  return send_proxy_list_.size();
}

bool SendGroup::IsConfChanged(ProxyInfoVec &current_proxy_vec, ProxyInfoVec &new_proxy_vec) {
  if (new_proxy_vec.empty()) return false;
  if (current_proxy_vec.size() != new_proxy_vec.size()) {
    return true;
  }

  for (auto &current_bu : current_proxy_vec) {
    for (int i = 0; i < new_proxy_vec.size(); i++) {
      if ((current_bu.ip() == new_proxy_vec[i].ip()) && (current_bu.port() == new_proxy_vec[i].port())) break;
      if (i == (new_proxy_vec.size() - 1)) {
        if ((current_bu.ip() != new_proxy_vec[i].ip() || current_bu.port() == new_proxy_vec[i].port())) {
          LOG_INFO("current proxy ip." << current_bu.ip() << ":" << current_bu.port() << " can`t find in proxy.");
          return true;
        }
      }
    }
  }
  return false;
}

bool SendGroup::IsAvailable() {
  unique_read_lock<read_write_mutex> rdlck(work_clients_mutex_);
  if (work_clients_ == nullptr) {
    return false;
  }
  if (work_clients_->empty()) {
    return false;
  }
  return true;
}
void SendGroup::ClearOldTcpClients() {
  if (work_clients_old_ != nullptr) {
    LOG_INFO("ClearOldTcpClients." << work_clients_old_->size());
    work_clients_old_->clear();
    work_clients_old_.reset();
  }
}

void SendGroup::LoadBalance(std::error_code error) {
  if (error) {
    return;
  }

  if (NeedDoLoadBalance()) {
    DoLoadBalance();
  }
  uint64_t interval = load_balance_interval_ + rand() % 120 * 1000;
  LOG_INFO("LoadBalance interval:" << interval);
  load_balance_timer_->expires_after(std::chrono::milliseconds(interval));
  load_balance_timer_->async_wait(std::bind(&SendGroup::LoadBalance, this, std::placeholders::_1));
}

void SendGroup::DoLoadBalance() {
  if (reserve_clients_.empty()) {
    return;
  }

  TcpClientTPtrT work_client = GetMaxLoadClient();
  TcpClientTPtrT reserve_client = GetReserveClient();
  if (reserve_client == nullptr || work_client == nullptr) {
    LOG_ERROR("client nullptr");
    return;
  }

  if ((work_client->GetAvgLoad() - reserve_client->GetAvgLoad()) > load_threshold_) {
    LOG_INFO("DoLoadBalance " << reserve_client->getClientInfo() << "replace" << work_client->getClientInfo()
                              << ",load[work " << work_client->GetAvgLoad() << "][reserve "
                              << reserve_client->GetAvgLoad() << "][threshold " << load_threshold_ << "]");
    std::string ip = work_client->getIp();
    uint32_t port = work_client->getPort();
    work_client->UpdateClient(reserve_client->getIp(), reserve_client->getPort());

    ProxyInfo proxy = GetRandomProxy(ip, port);
    if (!proxy.ip().empty()) {
      reserve_client->UpdateClient(proxy.ip(), proxy.port());
    }
  }

  need_balance_ = true;
}
bool SendGroup::NeedDoLoadBalance() {
  unique_read_lock<read_write_mutex> rdlck(work_clients_mutex_);
  if (load_threshold_ <= 0 || work_clients_->size() == current_proxy_vec_.size()) {
    LOG_INFO("Don`t need DoLoadBalance [load_threshold]:" << load_threshold_
                                                          << ",[tcp_client size]:" << work_clients_->size()
                                                          << ",[current_proxy_vec size]:" << current_proxy_vec_.size());
    need_balance_ = false;
    return false;
  }
  need_balance_ = true;
  return true;
}
void SendGroup::InitReserveClient() {
  if (max_proxy_num_ >= current_proxy_vec_.size()) {
    return;
  }
  uint64_t max_reserve_num = current_proxy_vec_.size() - max_proxy_num_;
  uint64_t reserve_num = std::min(SdkConfig::getInstance()->reserve_proxy_num_, max_reserve_num);
  if (reserve_num <= 0) {
    return;
  }

  unique_write_lock<read_write_mutex> wtlck(reserve_clients_mutex_);
  reserve_clients_.clear();

  for (uint64_t i = current_proxy_vec_.size() - reserve_num; i < current_proxy_vec_.size(); i++) {
    ProxyInfo tmp_proxy = current_proxy_vec_[i];
    TcpClientTPtrT tcpClientTPtrT = std::make_shared<TcpClient>(io_context_, tmp_proxy.ip(), tmp_proxy.port());
    reserve_clients_.push_back(tcpClientTPtrT);
  }
  LOG_INFO("InitReserveClient reserve_clients size:" << reserve_clients_.size());
}
bool SendGroup::UpSort(const TcpClientTPtrT &begin, const TcpClientTPtrT &end) {
  if (begin && end) {
    return begin->GetAvgLoad() < end->GetAvgLoad();
  }
  return false;
}

TcpClientTPtrT SendGroup::GetMaxLoadClient() {
  unique_read_lock<read_write_mutex> rdlck(work_clients_mutex_);
  uint32_t client_index = 0;
  int32_t max_load = (*work_clients_)[0]->GetAvgLoad();
  for (int index = 1; index < work_clients_->size(); index++) {
    int32_t proxy_load = (*work_clients_)[index]->GetAvgLoad();
    if (proxy_load > max_load) {
      max_load = proxy_load;
      client_index = index;
    }
  }
  return (*work_clients_)[client_index];
}

ProxyInfo SendGroup::GetRandomProxy(const std::string &ip, uint32_t port) {
  ProxyInfo proxy_info;
  for (auto &it : current_proxy_vec_) {
    if (it.ip() == ip && it.port() == port) {
      continue;
    }
    bool exist = false;
    for (int index = 0; index < reserve_clients_.size(); index++) {
      if (it.ip() == reserve_clients_[index]->getIp() && it.port() == reserve_clients_[index]->getPort()) {
        exist = true;
        break;
      }
    }
    if (exist) {
      continue;
    }
    if (ExistInWorkClient(it.ip(), it.port())) {
      continue;
    }
    proxy_info.setIp(it.ip());
    proxy_info.setPort(it.port());
    return proxy_info;
  }
  return proxy_info;
}

TcpClientTPtrT SendGroup::GetReserveClient() {
  std::sort(reserve_clients_.begin(), reserve_clients_.end(), UpSort);
  unique_read_lock<read_write_mutex> rdlck(work_clients_mutex_);
  ProxyInfo proxy_info;
  for (auto &it : reserve_clients_) {
    if (it->GetAvgLoad() <= 0) {
      continue;
    }
    bool exist = false;
    for (int index = 0; index < work_clients_->size(); index++) {
      if (it->getIp() == (*work_clients_)[index]->getIp() && it->getPort() == (*work_clients_)[index]->getPort()) {
        exist = true;
        break;
      }
    }
    if (exist) {
      continue;
    }
    return it;
  }
  return nullptr;
}

bool SendGroup::ExistInWorkClient(const std::string &ip, uint32_t port) {
  unique_read_lock<read_write_mutex> rdlck(work_clients_mutex_);
  for (int index = 0; index < work_clients_->size(); index++) {
    if (ip == (*work_clients_)[index]->getIp() && port == (*work_clients_)[index]->getPort()) {
      return true;
    }
  }
  return false;
}
}  // namespace inlong
