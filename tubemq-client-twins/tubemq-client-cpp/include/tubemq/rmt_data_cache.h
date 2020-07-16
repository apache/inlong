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

#include <pthread.h>
#include <stdint.h>

#include <list>
#include <map>
#include <set>
#include <string>

#include "tubemq/atomic_def.h"
#include "tubemq/flowctrl_def.h"
#include "tubemq/meta_info.h"





namespace tubemq {

using std::map;
using std::set;
using std::list;


// consumer remote data cache
class RmtDataCacheCsm {
 public:
  RmtDataCacheCsm(const string& client_id, const string& group_name);
  ~RmtDataCacheCsm();
  void UpdateDefFlowCtrlInfo(int64_t flowctrl_id,
                                     const string& flowctrl_info);
  void UpdateGroupFlowCtrlInfo(int32_t qyrpriority_id,
                 int64_t flowctrl_id, const string& flowctrl_info);
  const int64_t GetGroupQryPriorityId() const;
  bool IsUnderGroupCtrl();
  void AddNewPartition(const PartitionExt& partition_ext);
  bool SelectPartition(string &err_info,
           PartitionExt& partition_ext, string& confirm_context);
  void BookedPartionInfo(const string& partition_key, int64_t curr_offset,
                            int32_t err_code, bool esc_limit, int32_t msg_size,
                            int64_t limit_dlt, int64_t cur_data_dlt, bool require_slow);
  bool RelPartition(string &err_info, bool filter_consume,
                         const string& confirm_context, bool is_consumed);
  bool RelPartition(string &err_info, const string& confirm_context, bool is_consumed);
  bool RelPartition(string &err_info, bool filter_consume,
                         const string& confirm_context, bool is_consumed,
                         int64_t curr_offset, int32_t err_code, bool esc_limit,
                         int32_t msg_size, int64_t limit_dlt, int64_t cur_data_dlt);
  void FilterPartitions(const list<SubscribeInfo>& subscribe_info_lst,
          list<PartitionExt>& subscribed_partitions, list<PartitionExt>& unsub_partitions);
  void GetSubscribedInfo(list<SubscribeInfo>& subscribe_info_lst);
  bool GetPartitionExt(const string& part_key, PartitionExt& partition_ext);
  void GetRegBrokers(list<NodeInfo>& brokers);
  void GetPartitionByBroker(const NodeInfo& broker_info,
                                    list<PartitionExt>& partition_list);
  void GetCurPartitionOffsets(map<string, int64_t> part_offset_map);
  void GetAllBrokerPartitions(map<NodeInfo, list<PartitionExt> >& broker_parts);
  void RemovePartition(const list<PartitionExt>& partition_list);
  void RemovePartition(const set<string>& partition_keys);
  bool RemovePartition(string &err_info, const string& confirm_context);
  void RemoveAndGetPartition(const list<SubscribeInfo>& subscribe_infos,
        bool is_processing_rollback, map<NodeInfo, list<PartitionExt> >& broker_parts);
  bool BookPartition(const string& partition_key);
  void OfferEvent(const ConsumerEvent& event);
  void TakeEvent(ConsumerEvent& event);
  void ClearEvent();
  void OfferEventResult(const ConsumerEvent& event);
  bool PollEventResult(ConsumerEvent& event);

 private:
  void rmvMetaInfo(const string& partition_key);
  void buildConfirmContext(const string& partition_key,
                    int64_t booked_time, string& confirm_context);
  bool parseConfirmContext(string &err_info,
    const string& confirm_context, string& partition_key, int64_t& booked_time);
  bool inRelPartition(string &err_info, bool need_delay_check,
    bool filter_consume, const string& confirm_context, bool is_consumed);


 private:
  // timer begin

  // timer end
  string consumer_id_;
  string group_name_;
  // flow ctrl
  FlowCtrlRuleHandler group_flowctrl_handler_;
  FlowCtrlRuleHandler def_flowctrl_handler_;
  AtomicBoolean under_groupctrl_;
  AtomicLong last_checktime_;
  // meta info
  pthread_rwlock_t meta_rw_lock_;
  // partiton allocated map
  map<string, PartitionExt> partitions_;
  // topic partiton map
  map<string, set<string> > topic_partition_;
  // broker parition map
  map<NodeInfo, set<string> > broker_partition_;
  map<string, SubscribeInfo>  part_subinfo_;
  // for idle partitions occupy
  pthread_mutex_t  part_mutex_;
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
