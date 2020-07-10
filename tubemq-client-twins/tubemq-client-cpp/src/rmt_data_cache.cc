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
#include "tubemq/meta_info.h"



namespace tubemq {
 

RmtDataCacheCsm::RmtDataCacheCsm() {
  pthread_rwlock_init(&meta_rw_lock_, NULL);
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







}  // namespace tubemq
