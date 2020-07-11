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

#include "tubemq/rmt_data_cache.h"

#include <string>

#include <stdlib.h>

#include "tubemq/const_config.h"
#include "tubemq/meta_info.h"
#include "tubemq/utils.h"



namespace tubemq {




RmtDataCacheCsm::RmtDataCacheCsm() {
  pthread_rwlock_init(&meta_rw_lock_, NULL);
  pthread_mutex_init(&part_mutex_, NULL);
  pthread_mutex_init(&data_book_mutex_, NULL);
  pthread_mutex_init(&event_read_mutex_, NULL);
  pthread_cond_init(&event_read_cond_, NULL);
  pthread_mutex_init(&event_write_mutex_, NULL);
}

RmtDataCacheCsm::~RmtDataCacheCsm() {
  pthread_mutex_destroy(&event_write_mutex_);
  pthread_mutex_destroy(&event_read_mutex_);
  pthread_mutex_destroy(&data_book_mutex_);
  pthread_cond_destroy(&event_read_cond_);
  pthread_mutex_destroy(&part_mutex_);
  pthread_rwlock_destroy(&meta_rw_lock_);
}

void RmtDataCacheCsm::AddNewPartition(const PartitionExt& partition_ext) {
  //
  map<string, PartitionExt>::iterator it_map;
  map<string, set<string> >::iterator it_topic;
  map<NodeInfo, set<string> >::iterator it_broker;
  //
  string partition_key = partition_ext.GetPartitionKey();
  pthread_rwlock_wrlock(&meta_rw_lock_);
  it_map = partitions_.find(partition_key);
  if (it_map == partitions_.end()) {
    partitions_[partition_key] = partition_ext;
    it_topic = topic_partition_.find(partition_ext.GetTopic());
    if (it_topic == topic_partition_.end()) {
      set<string> tmp_part_set;
      tmp_part_set.insert(partition_key);
      topic_partition_[partition_ext.GetTopic()] = tmp_part_set;
    } else {
      if (it_topic->second.find(partition_key) == it_topic->second.end()) {
        it_topic->second.insert(partition_key);
      }
    }
    it_broker = broker_partition_.find(partition_ext.GetBrokerInfo());
    if (it_broker == broker_partition_.end()) {
      set<string> tmp_part_set;
      tmp_part_set.insert(partition_key);
      broker_partition_[partition_ext.GetBrokerInfo()] = tmp_part_set;
    } else {
      if (it_broker->second.find(partition_key) == it_broker->second.end()) {
        it_broker->second.insert(partition_key);
      }
    }
  }
  // check partition_key status
  if (partition_useds_.find(partition_key) == partition_useds_.end()
    && partition_timeouts_.find(partition_key) == partition_timeouts_.end()) {
    index_partitions_.remove(partition_key);
    index_partitions_.push_back(partition_key);
  }
  pthread_rwlock_unlock(&meta_rw_lock_);
}

bool RmtDataCacheCsm::SelectPartition(string &err_info,
                        PartitionExt& partition_ext, string& confirm_context) {
  bool result = false;
  int64_t booked_time = 0;
  string partition_key;
  map<string, PartitionExt>::iterator it_map;

  pthread_rwlock_rdlock(&meta_rw_lock_);
  if (partitions_.empty()) {
    err_info = "No partition info in local cache, please retry later!";
    result = false;
  } else {
    pthread_mutex_lock(&part_mutex_);
    if (index_partitions_.empty()) {
      err_info = "No idle partition to consume, please retry later!";
      result = false;
    } else {
      result = false;
      err_info = "No idle partition to consume data 2, please retry later!";
      booked_time =Utils::GetCurrentTimeMillis();
      partition_key = index_partitions_.front();
      index_partitions_.pop_front();
      buildConfirmContext(partition_key, booked_time, confirm_context);
      it_map = partitions_.find(partition_key);
      if (it_map != partitions_.end()) {
        partition_ext = it_map->second;
        partition_useds_[partition_key] = booked_time;
        result = true;
        err_info = "Ok";
      }
    }
    pthread_mutex_unlock(&part_mutex_);
  }
  pthread_rwlock_unlock(&meta_rw_lock_);
  return result;
}

void RmtDataCacheCsm::BookedPartionInfo(const string& partition_key, int64_t curr_offset,
                                             int32_t err_code, bool esc_limit, int32_t msg_size, 
                                             int64_t limit_dlt, int64_t cur_data_dlt, bool require_slow) {
  map<string, PartitionExt>::iterator it_part;
  // book partition offset info
  if (curr_offset >= 0) {
    pthread_mutex_lock(&data_book_mutex_);
    partition_offset_[partition_key] = curr_offset;
    pthread_mutex_unlock(&data_book_mutex_);
  }
  // book partition temp info
  pthread_rwlock_rdlock(&meta_rw_lock_);
  it_part = partitions_.find(partition_key);
  if(it_part != partitions_.end()) {
    it_part->second.BookConsumeData(err_code, msg_size,
              esc_limit, limit_dlt, cur_data_dlt, require_slow);
  }
  pthread_rwlock_unlock(&meta_rw_lock_);
}

bool RmtDataCacheCsm::RelPartition(string &err_info, bool is_filterconsume,
                                 const string& confirm_context, bool is_consumed) {
  int64_t wait_time;
  int64_t booked_time;
  string  partition_key;
  map<string, PartitionExt>::iterator it_Part;
  map<string, int64_t>::iterator it_used;
  // parse confirm context  
  bool result = parseConfirmContext(err_info,
                      confirm_context, partition_key, booked_time);
  if (!result) {
    return false;
  }
  pthread_rwlock_rdlock(&meta_rw_lock_);
  it_Part = partitions_.find(partition_key);
  if (it_Part == partitions_.end()) {
    pthread_mutex_lock(&part_mutex_);
    partition_useds_.erase(partition_key);
    index_partitions_.remove(partition_key);
    pthread_mutex_unlock(&part_mutex_);
    err_info = "Not found the partition in Consume Partition set!";
    result = false;
  } else {
    pthread_mutex_lock(&part_mutex_);
    it_used = partition_useds_.find(partition_key);
    if (it_used == partition_useds_.end()) {
      index_partitions_.remove(partition_key);
      index_partitions_.push_back(partition_key);       
    } else {
      if (it_used->second == booked_time) {
        partition_useds_.erase(partition_key);
        wait_time = it_Part->second.ProcConsumeResult(def_flowctrl_handler_, 
                      group_flowctrl_handler_, is_filterconsume, is_consumed);
        if (wait_time >= 10) {
          // todo add timer 
          // end todo
        } else {
          partition_useds_.erase(partition_key);
          index_partitions_.remove(partition_key);
        }
        err_info = "Ok";
        result = true;    
      } else {
        err_info = "Illegel confirmContext content: context not equal!";
        result = false;
      }
    }
    pthread_mutex_unlock(&part_mutex_);
  }
  pthread_rwlock_unlock(&meta_rw_lock_);
  return result;
}

bool RmtDataCacheCsm::RemovePartition(string &err_info,
                                  const string& confirm_context) {
  int64_t booked_time;
  string  partition_key;
  map<string, PartitionExt>::iterator it_Part;
  map<string, set<string> >::iterator it_topic;
  map<NodeInfo, set<string> >::iterator it_broker;
  // parse confirm context  
  bool result = parseConfirmContext(err_info,
                      confirm_context, partition_key, booked_time);
  if (!result) {
    return false;
  }
  // remove partiton
  pthread_rwlock_wrlock(&meta_rw_lock_);
  partition_useds_.erase(partition_key);
  index_partitions_.remove(partition_key);
  // todo need modify if timer build finished
  partition_timeouts_.erase(partition_key);
  // end todo
  it_Part = partitions_.find(partition_key);
  if (it_Part != partitions_.end()) {
    it_topic = topic_partition_.find(it_Part->second.GetTopic());
    if (it_topic != topic_partition_.end()) {
      it_topic->second.erase(it_Part->second.GetPartitionKey());
      if (it_topic->second.empty()) {
        topic_partition_.erase(it_Part->second.GetTopic());
      }
    }
    it_broker = broker_partition_.find(it_Part->second.GetBrokerInfo());
    if (it_broker != broker_partition_.end()) {
      it_broker->second.erase(it_Part->second.GetPartitionKey());
      if (it_broker->second.empty()) {
        broker_partition_.erase(it_Part->second.GetBrokerInfo());
      }
    }
    partitions_.erase(partition_key);
  }
  pthread_rwlock_unlock(&meta_rw_lock_);
  err_info = "Ok";
  return true;
}

void RmtDataCacheCsm::RemovePartition(const list<PartitionExt>& partition_list) {
  list<PartitionExt>::const_iterator it_lst;
  map<string, PartitionExt>::iterator it_Part;
  map<string, set<string> >::iterator it_topic;
  map<NodeInfo, set<string> >::iterator it_broker;

  pthread_rwlock_wrlock(&meta_rw_lock_);
  for (it_lst = partition_list.begin(); it_lst != partition_list.end(); it_lst++) {
    partition_useds_.erase(it_lst->GetPartitionKey());
    index_partitions_.remove(it_lst->GetPartitionKey());
    partitions_.erase(it_lst->GetPartitionKey());
    // todo need modify if timer build finished
    partition_timeouts_.erase(it_lst->GetPartitionKey());
    // end todo
		it_topic = topic_partition_.find(it_lst->GetTopic());
		if (it_topic != topic_partition_.end()) {
      it_topic->second.erase(it_lst->GetPartitionKey());
      if (it_topic->second.empty()) {
        topic_partition_.erase(it_lst->GetTopic());
      }
    }
    it_broker = broker_partition_.find(it_lst->GetBrokerInfo());
		if (it_broker != broker_partition_.end()) {
      it_broker->second.erase(it_lst->GetPartitionKey());
      if (it_broker->second.empty()) {
        broker_partition_.erase(it_lst->GetBrokerInfo());
      }
    }
  }
  pthread_rwlock_unlock(&meta_rw_lock_);
}


void RmtDataCacheCsm::OfferEvent(const ConsumerEvent& event) {
  pthread_mutex_lock(&event_read_mutex_);
  this->rebalance_events_.push_back(event);
  pthread_cond_broadcast(&event_read_cond_);
  pthread_mutex_unlock(&event_read_mutex_);
}

void RmtDataCacheCsm::TakeEvent(ConsumerEvent& event) {
  pthread_mutex_lock(&event_read_mutex_);
  while (this->rebalance_events_.empty()) {
    pthread_cond_wait(&event_read_cond_, &event_read_mutex_);
  }
  event = rebalance_events_.front();
  rebalance_events_.pop_front();
  pthread_mutex_unlock(&event_read_mutex_);
}

void RmtDataCacheCsm::ClearEvent() {
  pthread_mutex_lock(&event_read_mutex_);
  rebalance_events_.clear();
  pthread_mutex_unlock(&event_read_mutex_);
}

void RmtDataCacheCsm::OfferEventResult(const ConsumerEvent& event) {
  pthread_mutex_lock(&event_write_mutex_);
  this->rebalance_events_.push_back(event);
  pthread_mutex_unlock(&event_write_mutex_);
}

bool RmtDataCacheCsm::PollEventResult(ConsumerEvent& event) {
  bool result = false;
  pthread_mutex_lock(&event_write_mutex_);
  if (!rebalance_events_.empty()) {
    event = rebalance_events_.front();
    rebalance_events_.pop_front();
    result = true;
  }
  pthread_mutex_unlock(&event_write_mutex_);
  return result;
}

void RmtDataCacheCsm::buildConfirmContext(const string& partition_key,
                                   int64_t booked_time, string& confirm_context) {
  confirm_context.clear();
  confirm_context += partition_key;
  confirm_context += delimiter::kDelimiterAt;
  confirm_context += Utils::Long2str(booked_time);
}

bool RmtDataCacheCsm::parseConfirmContext(string &err_info,
     const string& confirm_context, string& partition_key, int64_t& booked_time) {
  //
  vector<string> result;
  Utils::Split(confirm_context, result, delimiter::kDelimiterAt); 
  if(result.empty()) {
    err_info = "Illegel confirmContext content: unregular value format!";
    return false;
  }
  partition_key = result[0];
  booked_time = (int64_t)atol(result[1].c_str());
  err_info = "Ok";
  return true;
}

}  // namespace tubemq
