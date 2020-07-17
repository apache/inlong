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

#include "tubemq/meta_info.h"

#include <stdlib.h>

#include <sstream>
#include <vector>

#include "tubemq/const_config.h"
#include "tubemq/tubemq_errcode.h"
#include "tubemq/utils.h"

namespace tubemq {

using std::stringstream;
using std::vector;

NodeInfo::NodeInfo() {
  this->node_id_ = 0;
  this->node_host_ = " ";
  this->node_port_ = tb_config::kBrokerPortDef;
  buildStrInfo();
}

// node_info = node_id:host:port
NodeInfo::NodeInfo(bool is_broker, const string& node_info) {
  vector<string> result;
  Utils::Split(node_info, result, delimiter::kDelimiterColon);
  if (is_broker) {
    this->node_id_ = atoi(result[0].c_str());
    this->node_host_ = result[1];
    this->node_port_ = tb_config::kBrokerPortDef;
    if (result.size() >= 3) {
      this->node_port_ = atoi(result[2].c_str());
    }
  } else {
    this->node_id_ = 0;
    this->node_host_ = result[0];
    this->node_port_ = tb_config::kBrokerPortDef;
    if (result.size() >= 2) {
      this->node_port_ = atoi(result[1].c_str());
    }
  }
  buildStrInfo();
}

NodeInfo::NodeInfo(const string& node_host, uint32_t node_port) {
  this->node_id_ = tb_config::kInvalidValue;
  this->node_host_ = node_host;
  this->node_port_ = node_port;
  buildStrInfo();
}

NodeInfo::NodeInfo(int node_id, const string& node_host, uint32_t node_port) {
  this->node_id_ = node_id;
  this->node_host_ = node_host;
  this->node_port_ = node_port;
  buildStrInfo();
}

NodeInfo::~NodeInfo() {
  //
}

NodeInfo& NodeInfo::operator=(const NodeInfo& target) {
  if (this != &target) {
    this->node_id_ = target.node_id_;
    this->node_host_ = target.node_host_;
    this->node_port_ = target.node_port_;
    this->addr_info_ = target.addr_info_;
    this->node_info_ = target.node_info_;
  }
  return *this;
}

bool NodeInfo::operator==(const NodeInfo& target) {
  if (this == &target) {
    return true;
  }
  if (this->node_info_ == target.node_info_) {
    return true;
  }
  return false;
}

bool NodeInfo::operator<(const NodeInfo& target) const {
  return this->node_info_ < target.node_info_;
}
const uint32_t NodeInfo::GetNodeId() const { return this->node_id_; }

const string& NodeInfo::GetHost() const { return this->node_host_; }

const uint32_t NodeInfo::GetPort() const { return this->node_port_; }

const string& NodeInfo::GetAddrInfo() const { return this->addr_info_; }

const string& NodeInfo::GetNodeInfo() const { return this->node_info_; }

void NodeInfo::buildStrInfo() {
  stringstream ss1;
  ss1 << this->node_host_;
  ss1 << delimiter::kDelimiterColon;
  ss1 << this->node_port_;
  this->addr_info_ = ss1.str();

  stringstream ss2;
  ss2 << this->node_id_;
  ss2 << delimiter::kDelimiterColon;
  ss2 << this->addr_info_;
  this->node_info_ = ss2.str();
}

Partition::Partition() {
  this->topic_ = " ";
  this->partition_id_ = 0;
  buildPartitionKey();
}

// partition_info = broker_info#topic:partitionId
Partition::Partition(const string& partition_info) {
  // initial process
  this->topic_ = " ";
  this->partition_id_ = 0;
  // parse partition_info string
  string::size_type pos = 0;
  string seg_key = delimiter::kDelimiterPound;
  string token_key = delimiter::kDelimiterColon;
  // parse broker_info
  pos = partition_info.find(seg_key);
  if (pos != string::npos) {
    string broker_info = partition_info.substr(0, pos);
    broker_info = Utils::Trim(broker_info);
    this->broker_info_ = NodeInfo(true, broker_info);
    string part_str = partition_info.substr(pos + seg_key.size(), partition_info.size());
    part_str = Utils::Trim(part_str);
    pos = part_str.find(token_key);
    if (pos != string::npos) {
      string topic_str = part_str.substr(0, pos);
      string part_id_str = part_str.substr(pos + token_key.size(), part_str.size());
      topic_str = Utils::Trim(topic_str);
      part_id_str = Utils::Trim(part_id_str);
      this->topic_ = topic_str;
      this->partition_id_ = atoi(part_id_str.c_str());
    }
  }
  buildPartitionKey();
}

// part_str = topic:partition_id
Partition::Partition(const NodeInfo& broker_info, const string& part_str) {
  vector<string> result;
  this->topic_ = " ";
  this->partition_id_ = 0;
  this->broker_info_ = broker_info;
  Utils::Split(part_str, result, delimiter::kDelimiterColon);
  if (result.size() >= 2) {
    this->topic_ = result[0];
    this->partition_id_ = atoi(result[1].c_str());
  }
  buildPartitionKey();
}

Partition::Partition(const NodeInfo& broker_info, const string& topic, uint32_t partition_id) {
  this->topic_ = topic;
  this->partition_id_ = partition_id;
  this->broker_info_ = broker_info;
  buildPartitionKey();
}

Partition::~Partition() {
  //
}

Partition& Partition::operator=(const Partition& target) {
  if (this != &target) {
    this->topic_ = target.topic_;
    this->partition_id_ = target.partition_id_;
    this->broker_info_ = target.broker_info_;
    this->partition_key_ = target.partition_key_;
    this->partition_info_ = target.partition_info_;
  }
  return *this;
}

bool Partition::operator==(const Partition& target) {
  if (this == &target) {
    return true;
  }
  if (this->partition_info_ == target.partition_info_) {
    return true;
  }
  return false;
}

const uint32_t Partition::GetBrokerId() const { return this->broker_info_.GetNodeId(); }

const string& Partition::GetBrokerHost() const { return this->broker_info_.GetHost(); }

const uint32_t Partition::GetBrokerPort() const { return this->broker_info_.GetPort(); }

const string& Partition::GetPartitionKey() const { return this->partition_key_; }

const string& Partition::GetTopic() const { return this->topic_; }

const NodeInfo& Partition::GetBrokerInfo() const { return this->broker_info_; }

const uint32_t Partition::GetPartitionId() const { return this->partition_id_; }

const string& Partition::ToString() const { return this->partition_info_; }

void Partition::buildPartitionKey() {
  stringstream ss1;
  ss1 << this->broker_info_.GetNodeId();
  ss1 << delimiter::kDelimiterColon;
  ss1 << this->topic_;
  ss1 << delimiter::kDelimiterColon;
  ss1 << this->partition_id_;
  this->partition_key_ = ss1.str();

  stringstream ss2;
  ss2 << this->broker_info_.GetNodeInfo();
  ss2 << delimiter::kDelimiterPound;
  ss2 << this->topic_;
  ss2 << delimiter::kDelimiterColon;
  ss2 << this->partition_id_;
  this->partition_info_ = ss2.str();
}

PartitionExt::PartitionExt() : Partition() {
  resetParameters();
}

PartitionExt::PartitionExt(const string& partition_info) : Partition(partition_info) {
  resetParameters();
}

PartitionExt::PartitionExt(const NodeInfo& broker_info, const string& part_str)
  : Partition(broker_info, part_str) {
  resetParameters();
}

PartitionExt::~PartitionExt() {
  //
}

PartitionExt& PartitionExt::operator=(const PartitionExt& target) {
  if (this != &target) {
    // parent class
    Partition::operator=(target);
    // child class
    this->is_last_consumed_ = target.is_last_consumed_;
    this->cur_flowctrl_ = target.cur_flowctrl_;
    this->cur_freqctrl_ = target.cur_freqctrl_;
    this->next_stage_updtime_ = target.next_stage_updtime_;
    this->next_slice_updtime_ = target.next_slice_updtime_;
    this->limit_slice_msgsize_ = target.limit_slice_msgsize_;
    this->cur_stage_msgsize_ = target.cur_stage_msgsize_;
    this->cur_slice_msgsize_ = target.cur_slice_msgsize_;
    this->total_zero_cnt_ = target.total_zero_cnt_;
    this->booked_time_ = target.booked_time_;
    this->booked_errcode_ = target.booked_errcode_;
    this->booked_esc_limit_ = target.booked_esc_limit_;
    this->booked_msgsize_ = target.booked_msgsize_;
    this->booked_dlt_limit_ = target.booked_dlt_limit_;
    this->booked_curdata_dlt_ = target.booked_curdata_dlt_;
    this->booked_require_slow_ = target.booked_require_slow_;
    this->booked_errcode_ = target.booked_errcode_;
    this->booked_errcode_ = target.booked_errcode_;
  }
  return *this;
}


void PartitionExt::BookConsumeData(int32_t errcode, int32_t msg_size,
  bool req_esc_limit, int64_t rsp_dlt_limit, int64_t last_datadlt, bool require_slow) {
  this->booked_time_ = Utils::GetCurrentTimeMillis();
  this->booked_errcode_ = errcode;
  this->booked_esc_limit_ = req_esc_limit;
  this->booked_msgsize_ = msg_size;
  this->booked_dlt_limit_ = rsp_dlt_limit;
  this->booked_curdata_dlt_ = last_datadlt;
  this->booked_require_slow_ = require_slow;
}

int64_t PartitionExt::ProcConsumeResult(const FlowCtrlRuleHandler& def_flowctrl_handler,
  const FlowCtrlRuleHandler& group_flowctrl_handler, bool filter_consume, bool last_consumed) {
  int64_t dlt_time = Utils::GetCurrentTimeMillis() - this->booked_time_;
  return ProcConsumeResult(def_flowctrl_handler, group_flowctrl_handler, filter_consume,
    last_consumed, this->booked_errcode_, this->booked_msgsize_, this->booked_esc_limit_,
    this->booked_dlt_limit_, this->booked_curdata_dlt_, this->booked_require_slow_) - dlt_time;
}

int64_t PartitionExt::ProcConsumeResult(const FlowCtrlRuleHandler& def_flowctrl_handler,
  const FlowCtrlRuleHandler& group_flowctrl_handler, bool filter_consume, bool last_consumed,
  int32_t errcode, int32_t msg_size, bool req_esc_limit, int64_t rsp_dlt_limit,
  int64_t last_datadlt, bool require_slow) {
  // #lizard forgives
  // record consume status
  this->is_last_consumed_ = last_consumed;
  // Update strategy data values
  updateStrategyData(def_flowctrl_handler, group_flowctrl_handler, msg_size, last_datadlt);
  // Perform different strategies based on error codes
  switch (errcode) {
    case err_code::kErrNotFound:
    case err_code::kErrSuccess:
      if (msg_size == 0 && errcode != err_code::kErrSuccess) {
        this->total_zero_cnt_ += 1;
      } else {
        this->total_zero_cnt_ = 0;
      }
      if (this->total_zero_cnt_ > 0) {
        if (group_flowctrl_handler.GetMinZeroCnt() != tb_config::kMaxIntValue) {
          return (int64_t)(group_flowctrl_handler.GetCurFreqLimitTime(
            this->total_zero_cnt_, (int32_t)rsp_dlt_limit));
        } else {
          return (int64_t)def_flowctrl_handler.GetCurFreqLimitTime(
            this->total_zero_cnt_, (int32_t)rsp_dlt_limit);
        }
      }
      if (req_esc_limit) {
        return 0;
      } else {
        if (this->cur_stage_msgsize_ >= this->cur_flowctrl_.GetDataSizeLimit()
          || this->cur_slice_msgsize_ >= this->limit_slice_msgsize_) {
          return this->cur_flowctrl_.GetFreqMsLimit() > rsp_dlt_limit
            ? this->cur_flowctrl_.GetFreqMsLimit() : rsp_dlt_limit;
        }
        if (errcode == err_code::kErrSuccess) {
          if (filter_consume && this->cur_freqctrl_.GetFreqMsLimit() >= 0) {
            if (require_slow) {
              return this->cur_freqctrl_.GetZeroCnt();
            } else {
              return this->cur_freqctrl_.GetFreqMsLimit();
            }
          } else if (!filter_consume && this->cur_freqctrl_.GetDataSizeLimit() >=0) {
            return this->cur_freqctrl_.GetDataSizeLimit();
          }
        }
        return rsp_dlt_limit;
      }
      break;

    default:
      return rsp_dlt_limit;
  }
}

void PartitionExt::SetLastConsumed(bool last_consumed) {
  this->is_last_consumed_ = last_consumed;
}

bool PartitionExt::IsLastConsumed() {
  return this->is_last_consumed_;
}

void PartitionExt::resetParameters() {
  this->is_last_consumed_ = false;
  this->cur_flowctrl_.SetDataDltAndFreqLimit(tb_config::kMaxLongValue, 20);
  this->next_stage_updtime_ = 0;
  this->next_slice_updtime_ = 0;
  this->limit_slice_msgsize_ = 0;
  this->cur_stage_msgsize_ = 0;
  this->cur_slice_msgsize_ = 0;
  this->total_zero_cnt_ = 0;
  this->booked_time_ = 0;
  this->booked_errcode_ = 0;
  this->booked_esc_limit_ = false;
  this->booked_msgsize_ = 0;
  this->booked_dlt_limit_ = 0;
  this->booked_curdata_dlt_ = 0;
  this->booked_require_slow_ = false;
}

void PartitionExt::updateStrategyData(const FlowCtrlRuleHandler& def_flowctrl_handler,
  const FlowCtrlRuleHandler& group_flowctrl_handler, int32_t msg_size, int64_t last_datadlt) {
  bool result = false;
  // Accumulated data received
  this->cur_stage_msgsize_ += msg_size;
  this->cur_slice_msgsize_ += msg_size;
  int64_t curr_time = Utils::GetCurrentTimeMillis();
  // Update strategy data values
  if (curr_time > this->next_stage_updtime_) {
    this->cur_stage_msgsize_ = 0;
    this->cur_slice_msgsize_ = 0;
    if (last_datadlt >= 0) {
      result = group_flowctrl_handler.GetCurDataLimit(last_datadlt, this->cur_flowctrl_);
      if (!result) {
        result = def_flowctrl_handler.GetCurDataLimit(last_datadlt, this->cur_flowctrl_);
        if (!result) {
          this->cur_flowctrl_.SetDataDltAndFreqLimit(tb_config::kMaxLongValue, 0);
        }
      }
      group_flowctrl_handler.GetFilterCtrlItem(this->cur_freqctrl_);
      if (this->cur_freqctrl_.GetFreqMsLimit() < 0) {
        def_flowctrl_handler.GetFilterCtrlItem(this->cur_freqctrl_);
      }
      curr_time = Utils::GetCurrentTimeMillis();
    }
    this->limit_slice_msgsize_ = this->cur_flowctrl_.GetDataSizeLimit() / 12;
    this->next_stage_updtime_ = curr_time + 60000;
    this->next_slice_updtime_ = curr_time + 5000;
  } else if (curr_time > this->next_slice_updtime_) {
    this->cur_slice_msgsize_ = 0;
    this->next_slice_updtime_ = curr_time + 5000;
  }
}

SubscribeInfo::SubscribeInfo() {
  this->consumer_id_ = " ";
  this->group_ = " ";
  this->partitionext_;
  buildSubInfo();
}

// sub_info = consumerId@group#broker_info#topic:partitionId
SubscribeInfo::SubscribeInfo(const string& sub_info) {
  string::size_type pos = 0;
  string seg_key = delimiter::kDelimiterPound;
  string at_key = delimiter::kDelimiterAt;
  this->consumer_id_ = " ";
  this->group_ = " ";
  // parse sub_info
  pos = sub_info.find(seg_key);
  if (pos != string::npos) {
    string consumer_info = sub_info.substr(0, pos);
    consumer_info = Utils::Trim(consumer_info);
    string partition_info = sub_info.substr(pos + seg_key.size(), sub_info.size());
    partition_info = Utils::Trim(partition_info);
    this->partitionext_ = PartitionExt(partition_info);
    pos = consumer_info.find(at_key);
    this->consumer_id_ = consumer_info.substr(0, pos);
    this->consumer_id_ = Utils::Trim(this->consumer_id_);
    this->group_ = consumer_info.substr(pos + at_key.size(), consumer_info.size());
    this->group_ = Utils::Trim(this->group_);
  }
  buildSubInfo();
}

SubscribeInfo::SubscribeInfo(const string& consumer_id,
        const string& group_name, const PartitionExt& partition_ext) {
  this->consumer_id_ = consumer_id;
  this->group_ = group_name;
  this->partitionext_ = partition_ext;
  buildSubInfo();
}

SubscribeInfo& SubscribeInfo::operator=(const SubscribeInfo& target) {
  if (this != &target) {
    this->consumer_id_ = target.consumer_id_;
    this->group_ = target.group_;
    this->partitionext_ = target.partitionext_;
  }
  return *this;
}

const string& SubscribeInfo::GetConsumerId() const { return this->consumer_id_; }

const string& SubscribeInfo::GetGroup() const { return this->group_; }

const PartitionExt& SubscribeInfo::GetPartitionExt() const { return this->partitionext_; }

const uint32_t SubscribeInfo::GgetBrokerId() const { return this->partitionext_.GetBrokerId(); }

const string& SubscribeInfo::GetBrokerHost() const { return this->partitionext_.GetBrokerHost(); }

const uint32_t SubscribeInfo::GetBrokerPort() const { return this->partitionext_.GetBrokerPort(); }

const string& SubscribeInfo::GetTopic() const { return this->partitionext_.GetTopic(); }

const uint32_t SubscribeInfo::GetPartitionId() const {
  return this->partitionext_.GetPartitionId();
}

const string& SubscribeInfo::ToString() const { return this->sub_info_; }

void SubscribeInfo::buildSubInfo() {
  stringstream ss;
  ss << this->consumer_id_;
  ss << delimiter::kDelimiterAt;
  ss << this->group_;
  ss << delimiter::kDelimiterPound;
  ss << this->partitionext_.ToString();
  this->sub_info_ = ss.str();
}

ConsumerEvent::ConsumerEvent() {
  this->rebalance_id_ = tb_config::kInvalidValue;
  this->event_type_ = tb_config::kInvalidValue;
  this->event_status_ = tb_config::kInvalidValue;
}

ConsumerEvent::ConsumerEvent(const ConsumerEvent& target) {
  this->rebalance_id_ = target.rebalance_id_;
  this->event_type_ = target.event_type_;
  this->event_status_ = target.event_status_;
  this->subscribe_list_ = target.subscribe_list_;
}

ConsumerEvent::ConsumerEvent(int64_t rebalance_id, int32_t event_type,
                             const list<SubscribeInfo>& subscribeInfo_lst, int32_t event_status) {
  list<SubscribeInfo>::const_iterator it;
  this->rebalance_id_ = rebalance_id;
  this->event_type_ = event_type;
  this->event_status_ = event_status;
  for (it = subscribeInfo_lst.begin(); it != subscribeInfo_lst.end(); ++it) {
    this->subscribe_list_.push_back(*it);
  }
}

ConsumerEvent& ConsumerEvent::operator=(const ConsumerEvent& target) {
  if (this != &target) {
    this->rebalance_id_ = target.rebalance_id_;
    this->event_type_ = target.event_type_;
    this->event_status_ = target.event_status_;
    this->subscribe_list_ = target.subscribe_list_;
  }
  return *this;
}

const int64_t ConsumerEvent::GetRebalanceId() const { return this->rebalance_id_; }

const int32_t ConsumerEvent::GetEventType() const { return this->event_type_; }

const int32_t ConsumerEvent::GetEventStatus() const { return this->event_status_; }

void ConsumerEvent::SetEventType(int32_t event_type) { this->event_type_ = event_type; }

void ConsumerEvent::SetEventStatus(int32_t event_status) { this->event_status_ = event_status; }

const list<SubscribeInfo>& ConsumerEvent::GetSubscribeInfoList() const {
  return this->subscribe_list_;
}

string ConsumerEvent::ToString() {
  uint32_t count = 0;
  stringstream ss;
  list<SubscribeInfo>::const_iterator it;
  ss << "ConsumerEvent [rebalanceId=";
  ss << this->rebalance_id_;
  ss << ", type=";
  ss << this->event_type_;
  ss << ", status=";
  ss << this->event_status_;
  ss << ", subscribeInfoList=[";
  for (it = this->subscribe_list_.begin(); it != this->subscribe_list_.end(); ++it) {
    if (count++ > 0) {
      ss << ",";
    }
    ss << it->ToString();
  }
  ss << "]]";
  return ss.str();
}




};  // namespace tubemq
