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
 
#ifndef _TUBEMQ_CLIENT_CONFIGURE_H_
#define _TUBEMQ_CLIENT_CONFIGURE_H_

#include <string>
#include <stdio.h>
#include <map>



namespace tubemq {

using namespace std;


// configuration value setting
namespace config {
// rpc timeout define  
static const int kRpcTimoutDef = 15;
static const int kRpcTimoutMax = 300;
static const int kRpcTimoutMin = 8;
// heartbeat period define
static const int kHeartBeatPeriodDef = 10;
static const int kHeartBeatFailRetryTimesDef = 5;
static const int kHeartBeatSleepPeriodDef = 60;
// max masterAddrInfo length
static const int kMasterAddrInfoMaxLength = 1024;
// max TopicName length
static const int kTopicNameMaxLength = 64;
// max Consume GroupName length
static const int kGroupNameMaxLength = 1024;
}  // namespace config


class BaseConfig {
 public:
  BaseConfig();
  ~BaseConfig();
  BaseConfig& operator=(const BaseConfig& target);
  bool SetMasterAddrInfo(string& err_info, const string& master_addrinfo);
  bool SetTlsInfo(string& err_info, bool tls_enable, 
                    const string& trust_store_path, const string& trust_store_password);
  bool SetAuthenticInfo(string& err_info, bool authentic_enable, 
                            const string& usr_name, const string& usr_password);
  const string& GetMasterAddrInfo() const;
  bool IsTlsEnabled();
  const string& GetTrustStorePath() const;
  const string& GetTrustStorePassword() const;
  bool IsAuthenticEnabled();
  const string& GetUsrName() const;
  const string& GetUsrPassWord() const;            
  // set the rpc timout, unit second, duration [8, 300], default 15 seconds;
  void SetRpcReadTimeoutSec(int rpc_read_timeout_sec);
  int GetRpcReadTimeoutSec();
  // Set the duration of the client's heartbeat cycle, in seconds, the default is 10 seconds
  void SetHeartbeatPeriodSec(int heartbeat_period_sec);
  int GetHeartbeatPeriodSec();
  void SetMaxHeartBeatRetryTimes(int max_heartbeat_retry_times);
  int GetMaxHeartBeatRetryTimes();
  void SetHeartbeatPeriodAftFailSec(int heartbeat_period_afterfail_sec);
  int GetHeartbeatPeriodAftFailSec();
  string ToString();

 private:
  string master_addrinfo_;
  // user authenticate
  bool   auth_enable_;
  string auth_usrname_;
  string auth_usrpassword_;
  // TLS configuration
  bool   tls_enabled_;
  string tls_trust_store_path_;
  string tls_trust_store_password_;
  // other setting
  int   rpc_read_timeout_sec_;
  int   heartbeat_period_sec_;
  int   max_heartbeat_retry_times_;
  int   heartbeat_period_afterfail_sec_;
};


class ConsumerConfig {
 public:
  ConsumerConfig();
};

}

#endif















