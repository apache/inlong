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

#include "tubemq/client_subinfo.h"
#include "tubemq/const_config.h"
#include "tubemq/utils.h"



namespace tubemq {


MasterAddrInfo::MasterAddrInfo() {
  curr_master_addr_ = "";
  master_addr_.clear();
}

bool MasterAddrInfo::InitMasterAddress(string& err_info, 
                                       const string& master_info) {
  master_addr_.clear();
  Utils::Split(master_info, master_addr_, delimiter::kDelimiterComma,
               delimiter::kDelimiterColon);
  if (master_addr_.empty()) {
    err_info = "Illegal parameter: master_info is blank!";
    return false;
  }

  map<string, int32_t>::iterator it = master_addr_.begin();
  curr_master_addr_ = it->first;
  err_info = "Ok";
  return true;
}

void MasterAddrInfo::GetNextMasterAddr(string& ipaddr, int32_t& port) {
  map<string, int32_t>::iterator it;
  it = master_addr_.find(curr_master_addr_);
  if(it != master_addr_.end()) {
    it++;
    if (it == master_addr_.end()) {
      it = master_addr_.begin();
    }
  } else {
    it = master_addr_.begin();
  }
  port   = it->second;
  ipaddr = it->first;
  curr_master_addr_ = it->first;
}

void MasterAddrInfo::GetCurrentMasterAddr(string& ipaddr, int32_t& port) {
   ipaddr = curr_master_addr_;
   port = master_addr_[curr_master_addr_];
}


ClientSubInfo::ClientSubInfo() {
  bound_consume_ = false;
  select_big_ = false;
  source_count_ = 0;
  session_key_ = "";
  not_allocated_.Set(true);
  first_registered_.Set(false);
  subscribed_time_ = tb_config::kInvalidValue;
  bound_partions_ = "";
}


void ClientSubInfo::SetConsumeTarget(const ConsumerConfig& config) {
  int32_t count = 0;
  string tmpstr = "";
  // book register time
  subscribed_time_ = Utils::GetCurrentTimeMillis();
  //
  first_registered_.Set(false);
  bound_consume_ = config.IsBoundConsume();
  topic_and_filter_map_ = config.GetSubTopicAndFilterMap();
  // build topic filter info
  topics_.clear();
  topic_conds_.clear();
  set<string>::iterator it_set;
  map<string, set<string> >::const_iterator it_topic;
  for (it_topic = topic_and_filter_map_.begin();
      it_topic != topic_and_filter_map_.end(); it_topic++) {
    topics_.push_back(it_topic->first);
    if (it_topic->second.empty()) {
      topic_filter_map_[it_topic->first] = false;
    } else {
      topic_filter_map_[it_topic->first] = true;

      // build topic conditions
      count = 0;
      tmpstr = it_topic->first;
      tmpstr += delimiter::kDelimiterPound;
      for (it_set = it_topic->second.begin();
          it_set != it_topic->second.end(); it_set++) {
        if (count++ > 0) {
          tmpstr += delimiter::kDelimiterComma;
        }
        tmpstr += *it_set;
      }
      topic_conds_.push_back(tmpstr);
    }
  }

  // build bound_partition info
  if (bound_consume_) {
    session_key_ = config.GetSessionKey();
    source_count_ = config.GetSourceCount();
    select_big_ = config.IsSelectBig();
    assigned_part_map_ = config.GetPartOffsetInfo();
    count = 0;
    bound_partions_ = "";
    map<string, int64_t>::const_iterator it;
    for (it = assigned_part_map_.begin();
      it != assigned_part_map_.end(); it++) {
      if (count++ > 0) {
        bound_partions_ += delimiter::kDelimiterComma;
      }
      bound_partions_ += it->first;
      bound_partions_ += delimiter::kDelimiterEqual;
      bound_partions_ += Utils::Long2str(it->second);
    }
  }
}

bool ClientSubInfo::CompAndSetNotAllocated(bool expect, bool update) {
  return not_allocated_.CompareAndSet(expect, update);
}

bool ClientSubInfo::IsFilterConsume(const string& topic) {
  map<string, bool>::iterator it;
  it = topic_filter_map_.find(topic);
  if (it == topic_filter_map_.end()) {
    return false;
  }
  return it->second;
}

void ClientSubInfo::GetAssignedPartOffset(const string& partition_key, int64_t& offset) {
  map<string, int64_t>::iterator it;
  if (first_registered_.Get() && bound_consume_ && not_allocated_.Get()) {
    it = assigned_part_map_.find(partition_key);
    if (it != assigned_part_map_.end()) {
      offset = it->second;
    }
  }
  offset = tb_config::kInvalidValue;
}

const map<string, set<string> >& ClientSubInfo::GetTopicFilterMap() const {
  return topic_and_filter_map_;
}


}  // namespace tubemq

