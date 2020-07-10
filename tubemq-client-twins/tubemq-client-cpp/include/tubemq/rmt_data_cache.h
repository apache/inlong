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

#ifndef TUBEMQ_CLIENT_RMT_DATA_CACHE_H_
#define TUBEMQ_CLIENT_RMT_DATA_CACHE_H_

#include <stdint.h>
#include <pthread.h>

#include <atomic>
#include <list>
#include <map>
#include <set>

#include "tubemq/flowctrl_def.h"
#include "tubemq/meta_info.h"




namespace tubemq {

using std::map;
using std::set;
using std::list;


// consumer remote data cache
class RmtDataCacheCsm {
 public:
  RmtDataCacheCsm();
  ~RmtDataCacheCsm();
  void AddNewPartition(const PartitionExt& partition_ext);
  void OfferEvent(const ConsumerEvent& event);
  void TakeEvent(ConsumerEvent& event);
  void ClearEvent();
  void OfferEventResult(const ConsumerEvent& event);
  bool PollEventResult(ConsumerEvent& event);


 private:
  // timer begin

  // timer end
  // flow ctrl
  FlowCtrlRuleHandler group_flowctrl_handler_;
  FlowCtrlRuleHandler def_flowctrl_handler_;
  pthread_rwlock_t meta_rw_lock_;
  // partiton allocated map
  map<string, PartitionExt> partitions_;
  // topic partiton map
  map<string, set<string> > topic_partition_;
  // broker parition map
  map<NodeInfo, set<string> > broker_partition_;
  // for partiton idle map
  list<string> index_partitions_;
  // for partition used map
  map<string, int64_t> partition_useds_;
  // for partiton timer map
  map<string, int64_t> partition_timeouts_;
  // data book
  pthread_mutex_t data_book_mutex_;
  // for partition offset cache
  map<string, int64_t> partition_offset_;
  // for partiton register booked
  map<string, bool> part_reg_booked_;
  // event
  pthread_mutex_t  event_read_mutex_;
  pthread_cond_t   event_read_cond_;
  list<ConsumerEvent> rebalance_events_;
  pthread_mutex_t  event_write_mutex_;
  list<ConsumerEvent> rebalance_results_;
};


}  // namespace tubemq

#endif  // TUBEMQ_CLIENT_RMT_DATA_CACHE_H_
