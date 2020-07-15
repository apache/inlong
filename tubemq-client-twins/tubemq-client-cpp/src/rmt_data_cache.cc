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




RmtDataCacheCsm::RmtDataCacheCsm(const string& client_id,
                                      const string& group_name) {
  consumer_id_ = client_id;
  group_name_ = group_name;
  under_groupctrl_.Set(false);
  last_checktime_.Set(0);
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

void RmtDataCacheCsm::UpdateDefFlowCtrlInfo(int64_t flowctrl_id,
                                                 const string& flowctrl_info) {
  if (flowctrl_id != def_flowctrl_handler_.GetFlowCtrlId()) {
    def_flowctrl_handler_.UpdateDefFlowCtrlInfo(true, 
      tb_config::kInvalidValue, flowctrl_id, flowctrl_info);
  }
}
void RmtDataCacheCsm::UpdateGroupFlowCtrlInfo(int32_t qyrpriority_id,
                             int64_t flowctrl_id, const string& flowctrl_info) {
  if (flowctrl_id != group_flowctrl_handler_.GetFlowCtrlId()) {
    group_flowctrl_handler_.UpdateDefFlowCtrlInfo(false, 
                qyrpriority_id, flowctrl_id, flowctrl_info);
  }
  if (qyrpriority_id != group_flowctrl_handler_.GetQryPriorityId()) {
    this->group_flowctrl_handler_.SetQryPriorityId(qyrpriority_id);

  }
  // update current if under group flowctrl 
  int64_t cur_time = Utils::GetCurrentTimeMillis();
  if (cur_time - last_checktime_.Get() > 10000) {
    FlowCtrlResult flowctrl_result;
    this->under_groupctrl_.Set(
      group_flowctrl_handler_.GetCurDataLimit(
        tb_config::kMaxLongValue, flowctrl_result));
    last_checktime_.Set(cur_time);
  }
}

const int64_t RmtDataCacheCsm::GetGroupQryPriorityId() const {
  return this->group_flowctrl_handler_.GetQryPriorityId();
}

bool RmtDataCacheCsm::IsUnderGroupCtrl() {
  return this->under_groupctrl_.Get();
}


void RmtDataCacheCsm::AddNewPartition(const PartitionExt& partition_ext) {
  //
  map<string, PartitionExt>::iterator it_map;
  map<string, set<string> >::iterator it_topic;
  map<NodeInfo, set<string> >::iterator it_broker;
  //
  SubscribeInfo sub_info(consumer_id_, group_name_, partition_ext);
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
    part_subinfo_[partition_key] = sub_info;
  }
  // check partition_key status
  pthread_mutex_lock(&part_mutex_);
  if (partition_useds_.find(partition_key) == partition_useds_.end()
    && partition_timeouts_.find(partition_key) == partition_timeouts_.end()) {
    index_partitions_.remove(partition_key);
    index_partitions_.push_back(partition_key);
  }
  pthread_mutex_unlock(&part_mutex_);
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
      booked_time = Utils::GetCurrentTimeMillis();
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

void RmtDataCacheCsm::BookedPartionInfo(const string& partition_key, 
                     int64_t curr_offset, int32_t err_code, bool esc_limit,
                     int32_t msg_size,int64_t limit_dlt, int64_t cur_data_dlt,
                     bool require_slow) {
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
  if (it_part != partitions_.end()) {
    it_part->second.BookConsumeData(err_code, msg_size,
              esc_limit, limit_dlt, cur_data_dlt, require_slow);
  }
  pthread_rwlock_unlock(&meta_rw_lock_);
}

// success process release partition
bool RmtDataCacheCsm::RelPartition(string &err_info, bool filter_consume,
                                 const string& confirm_context, bool is_consumed) {
  return inRelPartition(err_info, true, filter_consume, confirm_context, is_consumed);
}

// release partiton without response return
bool RmtDataCacheCsm::RelPartition(string &err_info,
                                 const string& confirm_context, bool is_consumed) {
  return inRelPartition(err_info, true, false, confirm_context, is_consumed);
}

// release partiton with error response return
bool RmtDataCacheCsm::RelPartition(string &err_info, bool filter_consume,
                              const string& confirm_context, bool is_consumed,
                              int64_t curr_offset, int32_t err_code, bool esc_limit,
                              int32_t msg_size, int64_t limit_dlt, int64_t cur_data_dlt) {
  int64_t booked_time;
  string  partition_key;
  // parse confirm context
  bool result = parseConfirmContext(err_info,
                      confirm_context, partition_key, booked_time);
  if (!result) {
    return false;
  }
  BookedPartionInfo(partition_key, curr_offset, err_code,
            esc_limit, msg_size, limit_dlt, cur_data_dlt, false);
  return inRelPartition(err_info, true,
    filter_consume, confirm_context, is_consumed);
}

void RmtDataCacheCsm::FilterPartitions(const list<SubscribeInfo>& subscribe_info_lst,
            list<PartitionExt>& subscribed_partitions, list<PartitionExt>& unsub_partitions) {
  //
  map<string, PartitionExt>::iterator it_part;
  list<SubscribeInfo>::const_iterator it_lst;
  // initial return;
  subscribed_partitions.clear();
  unsub_partitions.clear();
  pthread_rwlock_rdlock(&meta_rw_lock_);
  if (partitions_.empty()) {
    for (it_lst = subscribe_info_lst.begin(); it_lst != subscribe_info_lst.end(); it_lst++) {
      unsub_partitions.push_back(it_lst->GetPartitionExt());
    }
  } else {
    for (it_lst = subscribe_info_lst.begin(); it_lst != subscribe_info_lst.end(); it_lst++) {
      it_part = partitions_.find(it_lst->GetPartitionExt().GetPartitionKey());
      if (it_part == partitions_.end()) {
        unsub_partitions.push_back(it_lst->GetPartitionExt());
      } else {
        subscribed_partitions.push_back(it_lst->GetPartitionExt());
      }
    }
  }
  pthread_rwlock_unlock(&meta_rw_lock_);
}

void RmtDataCacheCsm::GetSubscribedInfo(list<SubscribeInfo>& subscribe_info_lst) {
  map<string, SubscribeInfo>::iterator it_sub;
  subscribe_info_lst.clear();
  pthread_rwlock_rdlock(&meta_rw_lock_);
  for (it_sub = part_subinfo_.begin(); it_sub != part_subinfo_.end(); ++it_sub) {
    subscribe_info_lst.push_back(it_sub->second);
  }
  pthread_rwlock_unlock(&meta_rw_lock_);
}

void RmtDataCacheCsm::GetAllBrokerPartitions(
                    map<NodeInfo, list<PartitionExt> >& broker_parts) {
  map<string, PartitionExt>::iterator it_part;
  map<NodeInfo, list<PartitionExt> >::iterator it_broker;

  broker_parts.clear();
  pthread_rwlock_rdlock(&meta_rw_lock_);
  for (it_part = partitions_.begin(); it_part != partitions_.end(); ++it_part) {
    it_broker = broker_parts.find(it_part->second.GetBrokerInfo());
    if (it_broker == broker_parts.end()) {
      list<PartitionExt> tmp_part_lst;
      tmp_part_lst.push_back(it_part->second);
      broker_parts[it_part->second.GetBrokerInfo()] = tmp_part_lst;
    } else {
      it_broker->second.push_back(it_part->second);
    }
  }
  pthread_rwlock_unlock(&meta_rw_lock_);
}

bool RmtDataCacheCsm::GetPartitionExt(const string& part_key, PartitionExt& partition_ext) {
  bool result = false;
  map<string, PartitionExt>::iterator it_map;
  
  pthread_rwlock_rdlock(&meta_rw_lock_);
  it_map = partitions_.find(part_key);
  if (it_map != partitions_.end()) {
    result = true;
    partition_ext = it_map->second;
  }
  pthread_rwlock_unlock(&meta_rw_lock_);
  return result;
}

void RmtDataCacheCsm::GetRegBrokers(list<NodeInfo>& brokers) {
  map<NodeInfo, set<string> >::iterator it;

  brokers.clear();
  pthread_rwlock_rdlock(&meta_rw_lock_);
  for (it = broker_partition_.begin(); it != broker_partition_.end(); ++it) {
    brokers.push_back(it->first);
  }
  pthread_rwlock_unlock(&meta_rw_lock_);
}

void RmtDataCacheCsm::GetPartitionByBroker(const NodeInfo& broker_info,
                                            list<PartitionExt>& partition_list) {
  set<string>::iterator it_key;
  map<NodeInfo, set<string> >::iterator it_broker;
  map<string, PartitionExt>::iterator it_part;
  
  partition_list.clear();
  pthread_rwlock_rdlock(&meta_rw_lock_);
  it_broker = broker_partition_.find(broker_info);
  if (it_broker != broker_partition_.end()) {
    for (it_key = it_broker->second.begin();
    it_key != it_broker->second.end(); it_key++) {
      it_part = partitions_.find(*it_key);
      if (it_part != partitions_.end()) {
        partition_list.push_back(it_part->second);
      }
    }
  }
  pthread_rwlock_unlock(&meta_rw_lock_);
}


void RmtDataCacheCsm::GetCurPartitionOffsets(map<string, int64_t> part_offset_map) {
  map<string, int64_t>::iterator it;

  part_offset_map.clear();
  pthread_mutex_lock(&data_book_mutex_);
  for (it = partition_offset_.begin(); it != partition_offset_.end(); ++it) {
    part_offset_map[it->first] = it->second;
  }
  pthread_mutex_unlock(&data_book_mutex_);
}


//
bool RmtDataCacheCsm::RemovePartition(string &err_info,
                                  const string& confirm_context) {
  int64_t booked_time;
  string  partition_key;
  map<string, PartitionExt>::iterator it_part;
  map<string, set<string> >::iterator it_topic;
  map<NodeInfo, set<string> >::iterator it_broker;
  // parse confirm context
  bool result = parseConfirmContext(err_info,
                      confirm_context, partition_key, booked_time);
  if (!result) {
    return false;
  }
  // remove partiton
  set<string> partition_keys;
  partition_keys.insert(partition_key);
  RemovePartition(partition_keys);
  err_info = "Ok";
  return true;
}

void RmtDataCacheCsm::RemovePartition(const list<PartitionExt>& partition_list) {
  set<string> partition_keys;
  list<PartitionExt>::const_iterator it_lst;
  for (it_lst = partition_list.begin(); it_lst != partition_list.end(); it_lst++) {
    partition_keys.insert(it_lst->GetPartitionKey());
  }
  if (!partition_keys.empty()) {
    RemovePartition(partition_keys);
  }
}

void RmtDataCacheCsm::RemovePartition(const set<string>& partition_keys) {
  set<string>::const_iterator it_lst;

  pthread_rwlock_wrlock(&meta_rw_lock_);
  for (it_lst = partition_keys.begin(); it_lst != partition_keys.end(); it_lst++) {
    pthread_mutex_lock(&part_mutex_);
    partition_useds_.erase(*it_lst);
    index_partitions_.remove(*it_lst);
    // todo need modify if timer build finished
    partition_timeouts_.erase(*it_lst);
    // end todo
    pthread_mutex_unlock(&part_mutex_);
    // remove meta info set info
    rmvMetaInfo(*it_lst);
  }
  pthread_rwlock_unlock(&meta_rw_lock_);
}

void RmtDataCacheCsm::RemoveAndGetPartition(const list<SubscribeInfo>& subscribe_infos,
        bool is_processing_rollback, map<NodeInfo, list<PartitionExt> >& broker_parts) {
  //
  string part_key;
  list<SubscribeInfo>::const_iterator it;
  map<string, PartitionExt>::iterator it_part;
  map<NodeInfo, list<PartitionExt> >::iterator it_broker;

  broker_parts.clear();
  // check if empty
  if (subscribe_infos.empty()) {
    return;
  }
  pthread_rwlock_wrlock(&meta_rw_lock_);
  pthread_mutex_lock(&part_mutex_);
  for (it = subscribe_infos.begin(); it != subscribe_infos.end(); ++it) {
    part_key = it->GetPartitionExt().GetPartitionKey();
    it_part = partitions_.find(part_key);
    if (it_part != partitions_.end()) {
      if (partition_useds_.find(part_key) != partition_useds_.end()) {
        if (is_processing_rollback) {
          it_part->second.SetLastConsumed(false);
        } else {
          it_part->second.SetLastConsumed(true);
        }
      }
      it_broker = broker_parts.find(it_part->second.GetBrokerInfo());
      if (it_broker == broker_parts.end()) {
        list<PartitionExt> tmp_part_list;
        tmp_part_list.push_back(it_part->second);
        broker_parts[it_part->second.GetBrokerInfo()] = tmp_part_list;
      } else {
        it_broker->second.push_back(it_part->second);
      }
      rmvMetaInfo(part_key);  
    }
    partition_useds_.erase(part_key);
    index_partitions_.remove(part_key);
    // todo need modify if timer build finished
    partition_timeouts_.erase(part_key);
    // end todo
  }
  pthread_mutex_unlock(&part_mutex_);
  pthread_rwlock_unlock(&meta_rw_lock_);
}



bool RmtDataCacheCsm::BookPartition(const string& partition_key) {
  bool result = false;
  map<string, bool>::iterator it;
  pthread_mutex_lock(&data_book_mutex_);
  it = part_reg_booked_.find(partition_key);
  if (it == part_reg_booked_.end()) {
    part_reg_booked_[partition_key] = true;
  }
  pthread_mutex_unlock(&data_book_mutex_);
  return result;
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
  if (result.empty()) {
    err_info = "Illegel confirmContext content: unregular value format!";
    return false;
  }
  partition_key = result[0];
  booked_time = (int64_t)atol(result[1].c_str());
  err_info = "Ok";
  return true;
}

void RmtDataCacheCsm::rmvMetaInfo(const string& partition_key) {
  map<string, PartitionExt>::iterator it_part;
  map<string, set<string> >::iterator it_topic;
  map<NodeInfo, set<string> >::iterator it_broker;
  it_part = partitions_.find(partition_key);
  if (it_part != partitions_.end()) {
    it_topic = topic_partition_.find(it_part->second.GetTopic());
    if (it_topic != topic_partition_.end()) {
      it_topic->second.erase(it_part->second.GetPartitionKey());
      if (it_topic->second.empty()) {
        topic_partition_.erase(it_part->second.GetTopic());
      }
    }
    it_broker = broker_partition_.find(it_part->second.GetBrokerInfo());
    if (it_broker != broker_partition_.end()) {
      it_broker->second.erase(it_part->second.GetPartitionKey());
      if (it_broker->second.empty()) {
        broker_partition_.erase(it_part->second.GetBrokerInfo());
      }
    }
    partitions_.erase(partition_key);
    part_subinfo_.erase(partition_key);
  }
}

bool RmtDataCacheCsm::inRelPartition(string &err_info, bool need_delay_check,
                     bool filter_consume, const string& confirm_context, bool is_consumed) {
  int64_t wait_time;
  int64_t booked_time;
  string  partition_key;
  map<string, PartitionExt>::iterator it_part;
  map<string, int64_t>::iterator it_used;
  // parse confirm context  
  bool result = parseConfirmContext(err_info,
                      confirm_context, partition_key, booked_time);
  if (!result) {
    return false;
  }
  pthread_rwlock_rdlock(&meta_rw_lock_);
  it_part = partitions_.find(partition_key);
  if (it_part == partitions_.end()) {
    // partition is unregister, release partition
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
      // partition is release but registered
      index_partitions_.remove(partition_key);
      index_partitions_.push_back(partition_key);
    } else {
      if (it_used->second == booked_time) {
        // wait release
        partition_useds_.erase(partition_key);
        index_partitions_.remove(partition_key);
        wait_time = 0;
        if (need_delay_check) {
          wait_time = it_part->second.ProcConsumeResult(def_flowctrl_handler_,
                        group_flowctrl_handler_, filter_consume, is_consumed);
        }
        if (wait_time >= 10) {
          // todo add timer 
          // end todo
        } else {
          index_partitions_.push_back(partition_key);
        }
        err_info = "Ok";
        result = true;
      } else {
        // partiton is used by other thread
        err_info = "Illegel confirmContext content: context not equal!";
        result = false;
      }
    }
    pthread_mutex_unlock(&part_mutex_);
  }
  pthread_rwlock_unlock(&meta_rw_lock_);
  return result;
}



}  // namespace tubemq
