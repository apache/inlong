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

#include "tubemq/client_service.h"

#include <sstream>

#include "tubemq/const_config.h"
#include "tubemq/logger.h"
#include "tubemq/utils.h"



namespace tubemq {

using std::lock_guard;
using std::stringstream;


BaseClient::BaseClient(bool is_producer) {
  this->is_producer_ = is_producer;
}

BaseClient::~BaseClient() {
  // no code
}

/*
TubeMQService::TubeMQService() {
  service_status_.Set(0);
  client_index_base_.Set(0);
}

TubeMQService::~TubeMQService() {
  string err_info;
  Stop(err_info);
}
*/

bool TubeMQService::Start(string& err_info, string conf_file) {
  // check configure file
  bool result = false;
  Fileini fileini;
  string sector = "TubeMQ";  

  result = Utils::ValidConfigFile(err_info, conf_file);
  if (!result) {
    return result;
  }
  result = fileini.Loadini(err_info, conf_file);
  if (!result) {
    return result;
  }
  result = Utils::GetLocalIPV4Address(err_info, local_host_);
  if (!result) {
    return result;
  }
  if (!service_status_.CompareAndSet(0,1)) {
    err_info = "TubeMQ Service has startted or Stopped!";
    return false;
  }
  iniLogger(fileini, sector);
  service_status_.Set(2);
  err_info = "Ok!";
  return true;
}

bool TubeMQService::Stop(string& err_info) {
  if (service_status_.CompareAndSet(2, -1)) {
    shutDownClinets();
    timer_executor_.Close();
    network_executor_.Close();
  }
  err_info = "OK!";
  return true;
}

bool TubeMQService::IsRunning() {
  return (service_status_.Get() == 2);
}

void TubeMQService::iniLogger(const Fileini& fileini, const string& sector) {
  string err_info;
  int32_t log_num = 10;
  int32_t log_size = 10;
  int32_t log_level = 4;
  string log_path = "../log/";
  fileini.GetValue(err_info, sector, "log_num", log_num, 10);
  fileini.GetValue(err_info, sector, "log_size", log_size, 100);
  fileini.GetValue(err_info, sector, "log_path", log_path, "../log/");
  fileini.GetValue(err_info, sector, "log_level", log_level, 4);
  log_level = TUBEMQ_MID(log_level, 0, 4);
  GetLogger().Init(log_path, Logger::Level(log_level), log_size, log_num);
}


int32_t TubeMQService::GetClientObjCnt() {
  lock_guard<mutex> lck(mutex_);
  return clients_map_.size();
}


bool TubeMQService::AddClientObj(string& err_info, BaseClient* client_obj) {
  if (service_status_.Get() != 0) {
    err_info = "Service not startted!";
    return false;
  }
  int32_t client_index = client_index_base_.IncrementAndGet();
  lock_guard<mutex> lck(mutex_);
  client_obj->SetClientIndex(client_index);
  this->clients_map_[client_index] = client_obj;
  err_info = "Ok";
  return true;
}

BaseClient* TubeMQService::GetClientObj(int32_t client_index) const {
  BaseClient* client_obj = NULL;
  map<int32_t, BaseClient*>::const_iterator it;
  
  lock_guard<mutex> lck(mutex_);
  it = clients_map_.find(client_index);
  if (it != clients_map_.end()) {
    client_obj = it->second;
  }
  return client_obj;
}

BaseClient* TubeMQService::RmvClientObj(int32_t client_index) {
  BaseClient* client_obj = NULL;
  map<int32_t, BaseClient*>::iterator it;
  
  lock_guard<mutex> lck(mutex_);
  it = clients_map_.find(client_index);
  if (it != clients_map_.end()) {
    client_obj = it->second;
    clients_map_.erase(client_index);
  }
  return client_obj;
}

void TubeMQService::shutDownClinets() const {
  map<int32_t, BaseClient*>::const_iterator it;
  lock_guard<mutex> lck(mutex_);
  for (it = clients_map_.begin(); it != clients_map_.end(); it++) {
    it->second->ShutDown();
  }
}


}  // namespace tubemq
