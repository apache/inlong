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
#include <vector>
#include "client_config.h"
#include "const_config.h"
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

BaseConfig::~BaseConfig() {}

BaseConfig& BaseConfig::operator=(const BaseConfig& target) {
  if (this != &target) {
    this->master_addrinfo_ = target.master_addrinfo_;
    this->auth_enable_ = target.auth_enable_;
    this->auth_usrname_ = target.auth_usrname_;
    this->auth_usrpassword_ = target.auth_usrpassword_;
    this->tls_enabled_ = target.tls_enabled_;
    this->tls_trust_store_path_ = target.tls_trust_store_path_;
    this->tls_trust_store_password_ = target.tls_trust_store_password_;
    this->rpc_read_timeout_sec_ = target.rpc_read_timeout_sec_;
    this->heartbeat_period_sec_ = target.heartbeat_period_sec_;
    this->max_heartbeat_retry_times_ = target.max_heartbeat_retry_times_;
    this->heartbeat_period_afterfail_sec_ = target.heartbeat_period_afterfail_sec_;
  }
  return *this;
}

bool BaseConfig::SetMasterAddrInfo(string& err_info, const string& master_addrinfo) {
  // check parameter masterAddrInfo
  string trimed_master_addr_info = Utils::Trim(master_addrinfo);
  if (trimed_master_addr_info.empty()) {
    err_info = "Illegal parameter: master_addrinfo is blank!";
    return false;
  }
  if (trimed_master_addr_info.length() > config::kMasterAddrInfoMaxLength) {
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
  Utils::Split(master_addrinfo, tgt_address_map, delimiter::kDelimiterComma, delimiter::kDelimiterColon);
  if (tgt_address_map.empty()) {
    err_info = "Illegal parameter: master_addrinfo is blank!";
    return false;
  }
  this->master_addrinfo_ = trimed_master_addr_info;
  err_info = "Ok";
  return true;
}

bool BaseConfig::SetTlsInfo(string& err_info, bool tls_enable, const string& trust_store_path,
                            const string& trust_store_password) {
  this->tls_enabled_ = tls_enable;
  if (tls_enable) {
    string trimed_trust_store_path = Utils::Trim(trust_store_path);
    if (trimed_trust_store_path.empty()) {
      err_info = "Illegal parameter: trust_store_path is empty!";
      return false;
    }
    string trimed_trust_store_password = Utils::Trim(trust_store_password);
    if (trimed_trust_store_password.empty()) {
      err_info = "Illegal parameter: trust_store_password is empty!";
      return false;
    }
    this->tls_trust_store_path_ = trimed_trust_store_path;
    this->tls_trust_store_password_ = trimed_trust_store_password;
  } else {
    this->tls_trust_store_path_ = "";
    this->tls_trust_store_password_ = "";
  }
  err_info = "Ok";
  return true;
}

bool BaseConfig::SetAuthenticInfo(string& err_info, bool authentic_enable, const string& usr_name,
                                  const string& usr_password) {
  this->auth_enable_ = authentic_enable;
  if (authentic_enable) {
    string trimed_usr_name = Utils::Trim(usr_name);
    if (trimed_usr_name.empty()) {
      err_info = "Illegal parameter: usr_name is empty!";
      return false;
    }
    string trimed_usr_password = Utils::Trim(usr_password);
    if (trimed_usr_password.empty()) {
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

const string& BaseConfig::GetMasterAddrInfo() const { return this->master_addrinfo_; }

bool BaseConfig::IsTlsEnabled() { return this->tls_enabled_; }

const string& BaseConfig::GetTrustStorePath() const { return this->tls_trust_store_path_; }

const string& BaseConfig::GetTrustStorePassword() const { return this->tls_trust_store_password_; }

bool BaseConfig::IsAuthenticEnabled() { return this->auth_enable_; }

const string& BaseConfig::GetUsrName() const { return this->auth_usrname_; }

const string& BaseConfig::GetUsrPassWord() const { return this->auth_usrpassword_; }

void BaseConfig::SetRpcReadTimeoutSec(int rpc_read_timeout_sec) {
  if (rpc_read_timeout_sec >= config::kRpcTimoutMax) {
    this->rpc_read_timeout_sec_ = config::kRpcTimoutMax;
  } else if (rpc_read_timeout_sec <= config::kRpcTimoutMin) {
    this->rpc_read_timeout_sec_ = config::kRpcTimoutMin;
  } else {
    this->rpc_read_timeout_sec_ = rpc_read_timeout_sec;
  }
}

int BaseConfig::GetRpcReadTimeoutSec() { return this->rpc_read_timeout_sec_; }

void BaseConfig::SetHeartbeatPeriodSec(int heartbeat_period_sec) { this->heartbeat_period_sec_ = heartbeat_period_sec; }

int BaseConfig::GetHeartbeatPeriodSec() { return this->heartbeat_period_sec_; }

void BaseConfig::SetMaxHeartBeatRetryTimes(int max_heartbeat_retry_times) {
  this->max_heartbeat_retry_times_ = max_heartbeat_retry_times;
}

int BaseConfig::GetMaxHeartBeatRetryTimes() { return this->max_heartbeat_retry_times_; }

void BaseConfig::SetHeartbeatPeriodAftFailSec(int heartbeat_period_afterfail_sec) {
  this->heartbeat_period_afterfail_sec_ = heartbeat_period_afterfail_sec;
}

int BaseConfig::GetHeartbeatPeriodAftFailSec() { return this->heartbeat_period_afterfail_sec_; }

string BaseConfig::ToString() {
  stringstream ss;
  ss << "BaseConfig={master_addrinfo_='";
  ss << this->master_addrinfo_;
  ss << "', authEnable=";
  ss << this->auth_enable_;
  ss << ", auth_usrname_='";
  ss << this->auth_usrname_;
  ss << "', auth_usrpassword_='";
  ss << this->auth_usrpassword_;
  ss << "', tls_enabled_=";
  ss << this->tls_enabled_;
  ss << ", tls_trust_store_path_='";
  ss << this->tls_trust_store_path_;
  ss << "', tls_trust_store_password_='";
  ss << this->tls_trust_store_password_;
  ss << "', rpc_read_timeout_sec_=";
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

ConsumerConfig::ConsumerConfig() {
  this->group_name_ = "";
  this->is_bound_consume_ = false;
  this->session_key_ = "";
  this->source_count_ = -1;
  this->is_select_big_ = true;
  this->consume_position_ = kConsumeFromLatestOffset;
  this->is_confirm_in_local_ = false;
  this->is_rollback_if_confirm_timout_ = true;
  this->max_subinfo_report_intvl_ = config::kSubInfoReportMaxIntervalTimes;
  this->msg_notfound_wait_period_ms_ = config::kMsgNotfoundWaitPeriodMsDef;
  this->reb_confirm_wait_period_ms_ = config::kRebConfirmWaitPeriodMsDef;
  this->max_confirm_wait_period_ms_ = config::kConfirmWaitPeriodMsMax;
  this->shutdown_reb_wait_period_ms_ = config::kRebWaitPeriodWhenShutdownMs;
}

ConsumerConfig::~ConsumerConfig() {}

ConsumerConfig& ConsumerConfig::operator=(const ConsumerConfig& target) {
  if (this != &target) {
    // parent class
    BaseConfig::operator=(target);
    // child class
    this->group_name_ = target.group_name_;
    this->sub_topic_and_filter_map_ = target.sub_topic_and_filter_map_;
    this->is_bound_consume_ = target.is_bound_consume_;
    this->session_key_ = target.session_key_;
    this->source_count_ = target.source_count_;
    this->is_select_big_ = target.is_select_big_;
    this->part_offset_map_ = target.part_offset_map_;
    this->consume_position_ = target.consume_position_;
    this->max_subinfo_report_intvl_ = target.max_subinfo_report_intvl_;
    this->msg_notfound_wait_period_ms_ = target.msg_notfound_wait_period_ms_;
    this->is_confirm_in_local_ = target.is_confirm_in_local_;
    this->is_rollback_if_confirm_timout_ = target.is_rollback_if_confirm_timout_;
    this->reb_confirm_wait_period_ms_ = target.reb_confirm_wait_period_ms_;
    this->max_confirm_wait_period_ms_ = target.max_confirm_wait_period_ms_;
    this->shutdown_reb_wait_period_ms_ = target.shutdown_reb_wait_period_ms_;
  }
  return *this;
}

bool ConsumerConfig::SetGroupConsumeTarget(string& err_info, const string& group_name,
                                           const set<string>& subscribed_topicset) {
  string tgt_group_name;
  bool is_success = Utils::ValidGroupName(err_info, group_name, tgt_group_name);
  if (!is_success) {
    return false;
  }
  if (subscribed_topicset.empty()) {
    err_info = "Illegal parameter: subscribed_topicset is empty!";
    return false;
  }
  string topic_name;
  map<string, set<string> > tmp_sub_map;
  for (set<string>::iterator it = subscribed_topicset.begin(); it != subscribed_topicset.end(); ++it) {
    topic_name = Utils::Trim(*it);
    is_success = Utils::ValidString(err_info, topic_name, false, true, true, config::kTopicNameMaxLength);
    if (!is_success) {
      err_info = "Illegal parameter: subscribed_topicset's item error, " + err_info;
      return false;
    }
    set<string> tmp_filters;
    tmp_sub_map[topic_name] = tmp_filters;
  }
  this->is_bound_consume_ = false;
  this->group_name_ = tgt_group_name;
  this->sub_topic_and_filter_map_ = tmp_sub_map;
  err_info = "Ok";
  return true;
}

bool ConsumerConfig::SetGroupConsumeTarget(string& err_info, const string& group_name,
                                           const map<string, set<string> >& subscribed_topic_and_filter_map) {
  string session_key;
  int source_count = 0;
  bool is_select_big = false;
  map<string, long> part_offset_map;
  return setGroupConsumeTarget(err_info, false, group_name, subscribed_topic_and_filter_map, session_key, source_count,
                               is_select_big, part_offset_map);
}

bool ConsumerConfig::SetGroupConsumeTarget(string& err_info, const string& group_name,
                                           const map<string, set<string> >& subscribed_topic_and_filter_map,
                                           const string& session_key, int source_count, bool is_select_big,
                                           const map<string, long>& part_offset_map) {
  return setGroupConsumeTarget(err_info, true, group_name, subscribed_topic_and_filter_map, session_key, source_count,
                               is_select_big, part_offset_map);
}

bool ConsumerConfig::setGroupConsumeTarget(string& err_info, bool is_bound_consume, const string& group_name,
                                           const map<string, set<string> >& subscribed_topic_and_filter_map,
                                           const string& session_key, int source_count, bool is_select_big,
                                           const map<string, long>& part_offset_map) {
  // check parameter group_name
  string tgt_group_name;
  bool is_success = Utils::ValidGroupName(err_info, group_name, tgt_group_name);
  if (!is_success) {
    return false;
  }
  // check parameter subscribed_topic_and_filter_map
  if (subscribed_topic_and_filter_map.empty()) {
    err_info = "Illegal parameter: subscribed_topic_and_filter_map is empty!";
    return false;
  }
  map<string, set<string> > tmp_sub_map;
  map<string, set<string> >::const_iterator it_map;
  for (it_map = subscribed_topic_and_filter_map.begin(); it_map != subscribed_topic_and_filter_map.end(); ++it_map) {
    int count = 0;
    string tmp_filteritem;
    set<string> tgt_filters;
    // check topic_name info
    is_success = Utils::ValidString(err_info, it_map->first, false, true, true, config::kTopicNameMaxLength);
    if (!is_success) {
      stringstream ss;
      ss << "Check parameter subscribed_topic_and_filter_map error: topic ";
      ss << it_map->first;
      ss << " ";
      ss << err_info;
      err_info = ss.str();
      return false;
    }
    string topic_name = Utils::Trim(it_map->first);
    // check filter info
    set<string> subscribed_filters = it_map->second;
    for (set<string>::iterator it = subscribed_filters.begin(); it != subscribed_filters.end(); ++it) {
      is_success = Utils::ValidFilterItem(err_info, *it, tmp_filteritem);
      if (!is_success) {
        stringstream ss;
        ss << "Check parameter subscribed_topic_and_filter_map error: topic ";
        ss << topic_name;
        ss << "'s filter item ";
        ss << err_info;
        err_info = ss.str();
        return false;
      }
      tgt_filters.insert(tmp_filteritem);
      count++;
    }
    if (count > config::kFilterItemMaxCount) {
      stringstream ss;
      ss << "Check parameter subscribed_topic_and_filter_map error: topic ";
      ss << it_map->first;
      ss << "'s filter item over max item count : ";
      ss << config::kFilterItemMaxCount;
      err_info = ss.str();
      return false;
    }
    tmp_sub_map[topic_name] = tgt_filters;
  }
  // check if bound consume
  if (!is_bound_consume) {
    this->is_bound_consume_ = false;
    this->group_name_ = tgt_group_name;
    this->sub_topic_and_filter_map_ = tmp_sub_map;
    err_info = "Ok";
    return true;
  }
  // check session_key
  string tgt_session_key = Utils::Trim(session_key);
  if (tgt_session_key.length() == 0 || tgt_session_key.length() > config::kSessionKeyMaxLength) {
    if (tgt_session_key.length() == 0) {
      err_info = "Illegal parameter: session_key is empty!";
    } else {
      stringstream ss;
      ss << "Illegal parameter: session_key's length over max length ";
      ss << config::kSessionKeyMaxLength;
      err_info = ss.str();
    }
    return false;
  }
  // check source_count
  if (source_count <= 0) {
    err_info = "Illegal parameter: source_count must over zero!";
    return false;
  }
  // check part_offset_map
  string part_key;
  map<string, long> tmp_parts_map;
  map<string, long>::const_iterator it_part;
  for (it_part = part_offset_map.begin(); it_part != part_offset_map.end(); ++it_part) {
    vector<string> result;
    Utils::Split(it_part->first, result, delimiter::kDelimiterColon);
    if (result.size() != 3) {
      stringstream ss;
      ss << "Illegal parameter: part_offset_map's key ";
      ss << it_part->first;
      ss << " format error, value must be aaaa:bbbb:cccc !";
      err_info = ss.str();
      return false;
    }
    if (tmp_sub_map.find(result[1]) != tmp_sub_map.end()) {
      stringstream ss;
      ss << "Illegal parameter: ";
      ss << it_part->first;
      ss << " subscribed topic ";
      ss << result[1];
      ss << " not included in subscribed_topic_and_filter_map's topic list!";
      err_info = ss.str();
      return false;
    }
    if (it_part->first.find_first_of(delimiter::kDelimiterComma) != string::npos) {
      stringstream ss;
      ss << "Illegal parameter: key ";
      ss << it_part->first;
      ss << " include ";
      ss << delimiter::kDelimiterComma;
      ss << " char!";
      err_info = ss.str();
      return false;
    }
    if (it_part->second < 0) {
      stringstream ss;
      ss << "Illegal parameter: ";
      ss << it_part->first;
      ss << "'s offset must over or equal zero, value is ";
      ss << it_part->second;
      err_info = ss.str();
      return false;
    }
    Utils::Join(result, delimiter::kDelimiterColon, part_key);
    tmp_parts_map[part_key] = it_part->second;
  }
  // set verified data
  this->is_bound_consume_ = true;
  this->group_name_ = tgt_group_name;
  this->sub_topic_and_filter_map_ = tmp_sub_map;
  this->session_key_ = tgt_session_key;
  this->source_count_ = source_count;
  this->is_select_big_ = is_select_big;
  this->part_offset_map_ = tmp_parts_map;
  err_info = "Ok";
  return true;
}

const string& ConsumerConfig::GetGroupName() const { return this->group_name_; }

const map<string, set<string> >& ConsumerConfig::GetSubTopicAndFilterMap() const {
  return this->sub_topic_and_filter_map_;
}

void ConsumerConfig::SetConsumePosition(ConsumePosition consume_from_where) {
  this->consume_position_ = consume_from_where;
}

const ConsumePosition ConsumerConfig::GetConsumePosition() const { return this->consume_position_; }

const int ConsumerConfig::GetMsgNotFoundWaitPeriodMs() const { return this->msg_notfound_wait_period_ms_; }

void ConsumerConfig::SetMsgNotFoundWaitPeriodMs(int msg_notfound_wait_period_ms) {
  this->msg_notfound_wait_period_ms_ = msg_notfound_wait_period_ms;
}

const int ConsumerConfig::GetMaxSubinfoReportIntvl() const { return this->max_subinfo_report_intvl_; }

void ConsumerConfig::SetMaxSubinfoReportIntvl(int max_subinfo_report_intvl) {
  this->max_subinfo_report_intvl_ = max_subinfo_report_intvl;
}

bool ConsumerConfig::IsConfirmInLocal() { return this->is_confirm_in_local_; }

void ConsumerConfig::SetConfirmInLocal(bool confirm_in_local) { this->is_confirm_in_local_ = confirm_in_local; }

bool ConsumerConfig::IsRollbackIfConfirmTimeout() { return this->is_rollback_if_confirm_timout_; }

void ConsumerConfig::setRollbackIfConfirmTimeout(bool is_rollback_if_confirm_timeout) {
  this->is_rollback_if_confirm_timout_ = is_rollback_if_confirm_timeout;
}

const int ConsumerConfig::GetWaitPeriodIfConfirmWaitRebalanceMs() const { return this->reb_confirm_wait_period_ms_; }

void ConsumerConfig::SetWaitPeriodIfConfirmWaitRebalanceMs(int reb_confirm_wait_period_ms) {
  this->reb_confirm_wait_period_ms_ = reb_confirm_wait_period_ms;
}

const int ConsumerConfig::GetMaxConfirmWaitPeriodMs() const { this->max_confirm_wait_period_ms_; }

void ConsumerConfig::SetMaxConfirmWaitPeriodMs(int max_confirm_wait_period_ms) {
  this->max_confirm_wait_period_ms_ = max_confirm_wait_period_ms;
}

const int ConsumerConfig::GetShutdownRebWaitPeriodMs() const { return this->shutdown_reb_wait_period_ms_; }

void ConsumerConfig::SetShutdownRebWaitPeriodMs(int wait_period_when_shutdown_ms) {
  this->shutdown_reb_wait_period_ms_ = wait_period_when_shutdown_ms;
}

string ConsumerConfig::ToString() {
  int i = 0;
  stringstream ss;
  map<string, long>::iterator it;
  map<string, set<string> >::iterator it_map;

  // print info
  ss << "ConsumerConfig = {";
  ss << BaseConfig::ToString();
  ss << ", group_name_='";
  ss << this->group_name_;
  ss << "', sub_topic_and_filter_map_={";
  for (it_map = this->sub_topic_and_filter_map_.begin(); it_map != this->sub_topic_and_filter_map_.end(); ++it_map) {
    if (i++ > 0) {
      ss << ",";
    }
    ss << "'";
    ss << it_map->first;
    ss << "'=[";
    int j = 0;
    set<string> topic_set = it_map->second;
    for (set<string>::iterator it = topic_set.begin(); it != topic_set.end(); ++it) {
      if (j++ > 0) {
        ss << ",";
      }
      ss << "'";
      ss << *it;
      ss << "'";
    }
    ss << "]";
  }
  ss << "}, is_bound_consume_=";
  ss << this->is_bound_consume_;
  ss << ", session_key_='";
  ss << this->session_key_;
  ss << "', source_count_=";
  ss << this->source_count_;
  ss << ", is_select_big_=";
  ss << this->is_select_big_;
  ss << ", part_offset_map_={";
  i = 0;
  for (it = this->part_offset_map_.begin(); it != this->part_offset_map_.end(); ++it) {
    if (i++ > 0) {
      ss << ",";
    }
    ss << "'";
    ss << it->first;
    ss << "'=";
    ss << it->second;
  }
  ss << "}, consume_position_=";
  ss << this->consume_position_;
  ss << ", max_subinfo_report_intvl_=";
  ss << this->max_subinfo_report_intvl_;
  ss << ", msg_notfound_wait_period_ms_=";
  ss << this->msg_notfound_wait_period_ms_;
  ss << ", is_confirm_in_local_=";
  ss << this->is_confirm_in_local_;
  ss << ", is_rollback_if_confirm_timout_=";
  ss << this->is_rollback_if_confirm_timout_;
  ss << ", reb_confirm_wait_period_ms_=";
  ss << this->reb_confirm_wait_period_ms_;
  ss << ", max_confirm_wait_period_ms_=";
  ss << this->max_confirm_wait_period_ms_;
  ss << ", shutdown_reb_wait_period_ms_=";
  ss << this->shutdown_reb_wait_period_ms_;
  ss << "}";
  return ss.str();
}

}  // namespace tubemq

