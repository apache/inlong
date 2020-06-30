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

#include <sstream> 
#include "client_config.h"
#include "utils.h"


namespace tubemq {


BaseConfig::BaseConfig() {
  this->master_addrinfo_ = "";
  this->auth_enable_ = false;
  this->auth_usrname_ = "";
  this->auth_usrpassword_ = "";
  this->tls_enabled_ = false;
  this->tls_trust_store_path_ = "";
  this->tls_trust_store_password_ = "";
  this->rpc_read_timeout_sec_ = config::kRpcTimoutDef;
  this->heartbeat_period_sec_ = config::kHeartBeatPeriodDef;
  this->max_heartbeat_retry_times_ = config::kHeartBeatFailRetryTimesDef;
  this->heartbeat_period_afterfail_sec_ = config::kHeartBeatSleepPeriodDef;
}

BaseConfig::~BaseConfig() {

}

BaseConfig& BaseConfig::operator=(const BaseConfig& target) {
  if(this != &target) {
    this->master_addrinfo_ = target.master_addrinfo_;
    this->auth_enable_    = target.auth_enable_;
    this->auth_usrname_   = target.auth_usrname_;
    this->auth_usrpassword_ = target.auth_usrpassword_;
    this->tls_enabled_      = target.tls_enabled_;
    this->tls_trust_store_path_      = target.tls_trust_store_path_;
    this->tls_trust_store_password_  = target.tls_trust_store_password_;
    this->rpc_read_timeout_sec_      = target.rpc_read_timeout_sec_;
    this->heartbeat_period_sec_     = target.heartbeat_period_sec_;
    this->max_heartbeat_retry_times_ = target.max_heartbeat_retry_times_;
    this->heartbeat_period_afterfail_sec_ = target.heartbeat_period_afterfail_sec_;
  }
  return *this;
}

bool BaseConfig::SetMasterAddrInfo(string& err_info, const string& master_addrinfo) {
  // check parameter masterAddrInfo
  string trimed_master_addr_info = Utils::trim(master_addrinfo);
  if(trimed_master_addr_info.empty()) {
    err_info = "Illegal parameter: master_addrinfo is blank!";
    return false;
  }
  if(trimed_master_addr_info.length() > config::kMasterAddrInfoMaxLength) {
    stringstream ss;
    ss << "Illegal parameter: over max ";
    ss << config::kMasterAddrInfoMaxLength;
    ss << " length of master_addrinfo parameter!";   
    err_info = ss.str();
    return false;
  }
  // parse and verify master address info
  // master_addrinfo's format like ip1:port1,ip2:port2,ip3:port3
  map<string, int> tgt_address_map;
  Utils::split(master_addrinfo, tgt_address_map, 
    delimiter::tDelimiterComma, delimiter::tDelimiterColon);
  if(tgt_address_map.empty()) {
    err_info = "Illegal parameter: master_addrinfo is blank!";
    return false;
  }
  this->master_addrinfo_ = trimed_master_addr_info;
  err_info = "Ok";
  return true;
}

bool BaseConfig::SetTlsInfo(string& err_info, bool tls_enable,
                const string& trust_store_path, const string& trust_store_password) {
  this->tls_enabled_ = tls_enable;
  if(tls_enable) {
    string trimed_trust_store_path = Utils::trim(trust_store_path);  
    if(trimed_trust_store_path.empty()) {
      err_info = "Illegal parameter: trust_store_path is empty!";
      return false;
    }
    string trimed_trust_store_password = Utils::trim(trust_store_password);  
    if(trimed_trust_store_password.empty()) {
      err_info = "Illegal parameter: trust_store_password is empty!";
      return false;
    }
      this->tls_trust_store_path_= trimed_trust_store_path;
      this->tls_trust_store_password_= trimed_trust_store_password;    
  } else {
    this->tls_trust_store_path_ = "";
    this->tls_trust_store_password_ = "";
  }
  err_info = "Ok";
  return true;  
}

bool BaseConfig::SetAuthenticInfo(string& err_info, bool authentic_enable, 
                                       const string& usr_name, const string& usr_password) {
  this->auth_enable_ = authentic_enable;
  if(authentic_enable) {
    string trimed_usr_name = Utils::trim(usr_name);
    if(trimed_usr_name.empty()) {
      err_info = "Illegal parameter: usr_name is empty!";
      return false;
    }
    string trimed_usr_password = Utils::trim(usr_password);  
    if(trimed_usr_password.empty()) {
      err_info = "Illegal parameter: usr_password is empty!";
      return false;
    }
    this->auth_usrname_ = trimed_usr_name;
    this->auth_usrpassword_ = trimed_usr_password;
  } else {
    this->auth_usrname_ = "";
    this->auth_usrpassword_ = "";
  }
  err_info = "Ok";
  return true;
}

const string& BaseConfig::GetMasterAddrInfo() const {
    return this->master_addrinfo_;
}

bool BaseConfig::IsTlsEnabled() {
  return this->tls_enabled_;
}

const string& BaseConfig::GetTrustStorePath() const {
  return this->tls_trust_store_path_;
}

const string& BaseConfig::GetTrustStorePassword() const {
  return this->tls_trust_store_password_;
}

bool BaseConfig::IsAuthenticEnabled() {
  return this->auth_enable_;
}

const string& BaseConfig::GetUsrName() const {
  return this->auth_usrname_;
}

const string& BaseConfig::GetUsrPassWord() const {
  return this->auth_usrpassword_;
}

void BaseConfig::SetRpcReadTimeoutSec(int rpc_read_timeout_sec) {
  if (rpc_read_timeout_sec >= config::kRpcTimoutMax) {
    this->rpc_read_timeout_sec_ = config::kRpcTimoutMax;
  } else if (rpc_read_timeout_sec <= config::kRpcTimoutMin) {
    this->rpc_read_timeout_sec_ = config::kRpcTimoutMin;
  } else {
    this->rpc_read_timeout_sec_ = rpc_read_timeout_sec;
  }
}

int BaseConfig::GetRpcReadTimeoutSec() {
  return this->rpc_read_timeout_sec_;
}

void BaseConfig::SetHeartbeatPeriodSec(int heartbeat_period_sec) {
  this->heartbeat_period_sec_ = heartbeat_period_sec;
}

int BaseConfig::GetHeartbeatPeriodSec() {
  return this->heartbeat_period_sec_;
}

void BaseConfig::SetMaxHeartBeatRetryTimes(int max_heartbeat_retry_times) {
  this->max_heartbeat_retry_times_ = max_heartbeat_retry_times;
}

int BaseConfig::GetMaxHeartBeatRetryTimes() {
  return this->max_heartbeat_retry_times_;
}

void BaseConfig::SetHeartbeatPeriodAftFailSec(int heartbeat_period_afterfail_sec) {
  this->heartbeat_period_afterfail_sec_ = heartbeat_period_afterfail_sec;
}

int BaseConfig::GetHeartbeatPeriodAftFailSec() {
  return this->heartbeat_period_afterfail_sec_;
}

string BaseConfig::ToString() {
  stringstream ss;
  ss << "BaseConfig={master_addrinfo_=";
  ss << this->master_addrinfo_;
  ss << ", authEnable=";
  ss << this->auth_enable_;
  ss << ", auth_usrname_='";
  ss << this->auth_usrname_;
  ss << "', auth_usrpassword_=";
  ss << this->auth_usrpassword_;
  ss << ", tls_enabled_=";
  ss << this->tls_enabled_;
  ss << ", tls_trust_store_path_=";
  ss << this->tls_trust_store_path_;
  ss << ", tls_trust_store_password_=";
  ss << this->tls_trust_store_password_;
  ss << ", rpc_read_timeout_sec_=";
  ss << this->rpc_read_timeout_sec_;
  ss << ", heartbeat_period_sec_=";
  ss << this->heartbeat_period_sec_;
  ss << ", max_heartbeat_retry_times_=";
  ss << this->max_heartbeat_retry_times_;
  ss << ", heartbeat_period_afterfail_sec_=";
  ss << this->heartbeat_period_afterfail_sec_;
  ss << "}";
  return ss.str();
}



}

