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


namespace TubeMQ {


BaseConfig::BaseConfig() {
  this->masterAddrStr_ = "";
  this->authEnable_ = false;
  this->authUsrName_ = "";
  this->authUsrPassWord_ = "";
  this->tlsEnabled_ = false;
  this->tlsTrustStorePath_ = "";
  this->tlsTrustStorePassword_ = "";
  this->rpcReadTimeoutSec_ = config::kRpcTimoutDef;
  this->heartbeatPeriodSec_ = config::kHeartBeatPeriodDef;
  this->maxHeartBeatRetryTimes_ = config::kHeartBeatFailRetryTimesDef;
  this->heartbeatPeriodAfterFailSec_ = config::kHeartBeatSleepPeriodDef;
}

BaseConfig::~BaseConfig() {

}

BaseConfig& BaseConfig::operator=(const BaseConfig& target) {
  if(this != &target) {
    this->masterAddrStr_ = target.masterAddrStr_;
    this->authEnable_    = target.authEnable_;
    this->authUsrName_   = target.authUsrName_;
    this->authUsrPassWord_ = target.authUsrPassWord_;
    this->tlsEnabled_      = target.tlsEnabled_;
    this->tlsTrustStorePath_      = target.tlsTrustStorePath_;
    this->tlsTrustStorePassword_  = target.tlsTrustStorePassword_;
    this->rpcReadTimeoutSec_      = target.rpcReadTimeoutSec_;
    this->heartbeatPeriodSec_     = target.heartbeatPeriodSec_;
    this->maxHeartBeatRetryTimes_ = target.maxHeartBeatRetryTimes_;
    this->heartbeatPeriodAfterFailSec_ = target.heartbeatPeriodAfterFailSec_;
  }
  return *this;
}
    
bool BaseConfig::SetMasterAddrInfo(string& errInfo, const string& masterAddrInfo) {
  // check parameter masterAddrInfo
  string trimMasterAddrInfo = Utils::trim(masterAddrInfo);
  if(trimMasterAddrInfo.empty()) {
    errInfo = "Illegal parameter: masterAddrInfo is blank!";
    return false;
  }
  if(trimMasterAddrInfo.length() > config::kMasterAddrInfoMaxLength) {
    stringstream ss;
    ss << "Illegal parameter: over max ";
    ss << config::kMasterAddrInfoMaxLength;
    ss << " length of masterAddrInfo parameter!";   
    errInfo = ss.str();
    return false;
  }
  // parse and verify master address info
  // masterAddrInfo's format like ip1:port1,ip2:port2,ip3:port3
  map<string, int> tgtAddressMap;
  Utils::split(masterAddrInfo, tgtAddressMap, 
    delimiter::tDelimiterComma, delimiter::tDelimiterColon);
  if(tgtAddressMap.empty()) {
    errInfo = "Illegal parameter: masterAddrInfo is blank!";
    return false;
  }
  this->masterAddrStr_ = trimMasterAddrInfo;
  errInfo = "Ok";
  return true;
}

bool BaseConfig::SetTlsInfo(string& errInfo, bool tlsEnable,
                              const string& trustStorePath, const string& trustStorePassword) {
  this->tlsEnabled_ = tlsEnable;
  if(tlsEnable) {
    string trimTrustStorePath = Utils::trim(trustStorePath);  
    if(trimTrustStorePath.empty()) {
      errInfo = "Illegal parameter: trustStorePath is empty!";
      return false;
    }
    string trimTrustStorePassword = Utils::trim(trustStorePassword);  
    if(trimTrustStorePassword.empty()) {
      errInfo = "Illegal parameter: trustStorePassword is empty!";
      return false;
    }
      this->tlsTrustStorePath_= trimTrustStorePath;
      this->tlsTrustStorePassword_= trimTrustStorePassword;    
  } else {
    this->tlsTrustStorePath_ = "";
    this->tlsTrustStorePassword_ = "";
  }
  errInfo = "Ok";
  return true;  
}

bool BaseConfig::SetAuthenticInfo(string& errInfo, bool needAuthentic, 
                                       const string& usrName, const string& usrPassWord) {
  this->authEnable_ = needAuthentic;
  if(needAuthentic) {
    string trimUsrName = Utils::trim(usrName);
    if(trimUsrName.empty()) {
      errInfo = "Illegal parameter: usrName is empty!";
      return false;
    }
    string trimUsrPassWord = Utils::trim(usrPassWord);  
    if(trimUsrPassWord.empty()) {
      errInfo = "Illegal parameter: usrPassWord is empty!";
      return false;
    }
    this->authUsrName_ = trimUsrName;
    this->authUsrPassWord_ = trimUsrPassWord;
  } else {
    this->authUsrName_ = "";
    this->authUsrPassWord_ = "";
  }
  errInfo = "Ok";
  return true;
}

const string& BaseConfig::GetMasterAddrInfo() const {
    return this->masterAddrStr_;
}

bool BaseConfig::IsTlsEnabled() {
  return this->tlsEnabled_;
}

const string& BaseConfig::GetTrustStorePath() const {
  return this->tlsTrustStorePath_;
}

const string& BaseConfig::GetTrustStorePassword() const {
  return this->tlsTrustStorePassword_;
}

bool BaseConfig::IsAuthenticEnabled() {
  return this->authEnable_;
}

const string& BaseConfig::GetUsrName() const {
  return this->authUsrName_;
}

const string& BaseConfig::GetUsrPassWord() const {
  return this->authUsrPassWord_;
}

void BaseConfig::SetRpcReadTimeoutSec(int rpcReadTimeoutSec) {
  if (rpcReadTimeoutSec >= config::kRpcTimoutMax) {
    this->rpcReadTimeoutSec_ = config::kRpcTimoutMax;
  } else if (rpcReadTimeoutSec <= config::kRpcTimoutMin) {
    this->rpcReadTimeoutSec_ = config::kRpcTimoutMin;
  } else {
    this->rpcReadTimeoutSec_ = rpcReadTimeoutSec;
  }
}

int BaseConfig::GetRpcReadTimeoutSec() {
  return this->rpcReadTimeoutSec_;
}

void BaseConfig::SetHeartbeatPeriodSec(int heartbeatPeriodSec) {
  this->heartbeatPeriodSec_ = heartbeatPeriodSec;
}

int BaseConfig::GetHeartbeatPeriodSec() {
  return this->heartbeatPeriodSec_;
}

void BaseConfig::SetMaxHeartBeatRetryTimes(int maxHeartBeatRetryTimes) {
  this->maxHeartBeatRetryTimes_ = maxHeartBeatRetryTimes;
}

int BaseConfig::GetMaxHeartBeatRetryTimes() {
  return this->maxHeartBeatRetryTimes_;
}

void BaseConfig::SetHeartbeatPeriodAftFailSec(int heartbeatPeriodSecAfterFailSec) {
  this->heartbeatPeriodAfterFailSec_ = heartbeatPeriodSecAfterFailSec;
}

int BaseConfig::GetHeartbeatPeriodAftFailSec() {
  return this->heartbeatPeriodAfterFailSec_;
}

string BaseConfig::ToString() {
  stringstream ss;
  ss << "BaseConfig={masterAddrStr=";
  ss << this->masterAddrStr_;
  ss << ", authEnable=";
  ss << this->authEnable_;
  ss << ", authUsrName='";
  ss << this->authUsrName_;
  ss << "', authUsrPassWord=";
  ss << this->authUsrPassWord_;
  ss << ", tlsEnable=";
  ss << this->tlsEnabled_;
  ss << ", tlsTrustStorePath=";
  ss << this->tlsTrustStorePath_;
  ss << ", tlsTrustStorePassword=";
  ss << this->tlsTrustStorePassword_;
  ss << ", rpcReadTimeoutSec=";
  ss << this->rpcReadTimeoutSec_;
  ss << ", heartbeatPeriodSec=";
  ss << this->heartbeatPeriodSec_;
  ss << ", maxHeartBeatRetryTimes=";
  ss << this->maxHeartBeatRetryTimes_;
  ss << ", heartbeatPeriodAfterFail=";
  ss << this->heartbeatPeriodAfterFailSec_;
  ss << "}";
  return ss.str();
}



}

