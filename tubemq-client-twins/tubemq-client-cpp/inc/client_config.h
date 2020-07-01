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
#include <set>



namespace tubemq {

using namespace std;


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


enum ConsumePosition {
  kConsumeFromFirstOffset = -1,
  kConsumeFromLatestOffset = 0,
  kComsumeFromMaxOffsetAlways = 1
};



class ConsumerConfig : public BaseConfig {
 public: 
  ConsumerConfig();
  ~ConsumerConfig();
  ConsumerConfig& operator=(const ConsumerConfig& target); 
  bool SetGroupConsumeTarget(string& err_info, 
    const string& group_name, const set<string>& subscribed_topicset);
  bool SetGroupConsumeTarget(string& err_info, 
    const string& group_name, const map<string, set<string> >& subscribed_topic_and_filter_map);
  bool SetGroupConsumeTarget(string& err_info, 
    const string& group_name, const map<string, set<string> >& subscribed_topic_and_filter_map,
    const string& session_key, int source_count, bool is_select_big, const map<string, long>& part_offset_map);
  const string& GetGroupName() const;
  const map<string, set<string> >& GetSubTopicAndFilterMap() const;
  void SetConsumePosition(ConsumePosition consume_from_where);
  const ConsumePosition GetConsumePosition() const;
  const int GetMsgNotFoundWaitPeriodMs() const;
  void SetMsgNotFoundWaitPeriodMs(int msg_notfound_wait_period_ms);
  const int GetMaxSubinfoReportIntvl() const;
  void SetMaxSubinfoReportIntvl(int max_subinfo_report_intvl);
  bool IsConfirmInLocal();
  void SetConfirmInLocal(bool confirm_in_local);
  bool IsRollbackIfConfirmTimeout();
  void setRollbackIfConfirmTimeout(bool is_rollback_if_confirm_timeout);
  const int GetWaitPeriodIfConfirmWaitRebalanceMs() const;
  void SetWaitPeriodIfConfirmWaitRebalanceMs(int reb_confirm_wait_period_ms);
  const int GetMaxConfirmWaitPeriodMs() const;
  void SetMaxConfirmWaitPeriodMs(int max_confirm_wait_period_ms);
  const int GetShutdownRebWaitPeriodMs() const;
  void SetShutdownRebWaitPeriodMs(int wait_period_when_shutdown_ms);
  string ToString();

 private:
  bool setGroupConsumeTarget(string& err_info, bool is_bound_consume,
    const string& group_name, const map<string, set<string> >& subscribed_topic_and_filter_map,
    const string& session_key, int source_count, bool is_select_big, const map<string, long>& part_offset_map);
    
  
 private: 
  string group_name_;
  map<string, set<string> > sub_topic_and_filter_map_;
  bool is_bound_consume_;
  string session_key_;
  int source_count_;
  bool is_select_big_;
  map<string, long> part_offset_map_;
  ConsumePosition consume_position_;
  int max_subinfo_report_intvl_;
  int msg_notfound_wait_period_ms_;
  bool is_confirm_in_local_;
  bool is_rollback_if_confirm_timout_;
  int reb_confirm_wait_period_ms_;
  int max_confirm_wait_period_ms_;
  int shutdown_reb_wait_period_ms_;
};




}

#endif















