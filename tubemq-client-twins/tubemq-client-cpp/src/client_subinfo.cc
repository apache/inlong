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

void ClientSubInfo::SetConsumeTarget(bool bound_consume,
         const map<string, set<string> >& topic_and_filter_map,
         const string& session_key, uint32_t source_count,
         bool select_big, const map<string, int64_t>& part_offset_map) {
  int32_t count = 0;
  string tmpstr = "";
  // book register time
  subscribed_time_ = Utils::GetCurrentTimeMillis();
  //
  first_registered_.Set(false);
  bound_consume_ = bound_consume;
  topic_and_filter_map_ = topic_and_filter_map;
  // build topic filter info
  topics_.clear();
  topic_conds_.clear();
  set<string>::iterator it_set;
  map<string, set<string> >::const_iterator it_topic;
  for (it_topic = topic_and_filter_map.begin();
      it_topic != topic_and_filter_map.end(); it_topic++) {
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
  if (bound_consume) {
    session_key_ = session_key;
    source_count_ = source_count;
    select_big_ = select_big;
    assigned_part_map_ = part_offset_map;
    count = 0;
    bound_partions_ = "";
    map<string, int64_t>::const_iterator it;
    for (it = part_offset_map.begin(); it != part_offset_map.end(); it++) {
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

