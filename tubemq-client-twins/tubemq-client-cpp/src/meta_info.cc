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
#include <stdlib.h>
#include "utils.h"
#include "meta_info.h"
#include "const_config.h"


namespace tubemq {


NodeInfo::NodeInfo() {
  this->node_id_   = config::kInvalidValue;
  this->node_host_ = " ";
  this->node_port_ = config::kInvalidValue;
  buildStrInfo();
}

// node_info = node_id:host:port
NodeInfo::NodeInfo(bool is_broker, const string& node_info) {
  vector<string> result;
  Utils::Split(node_info, result, delimiter::kDelimiterColon);
  if (is_broker) {
    this->node_id_   = atoi(result[0].c_str());
    this->node_host_ = result[1];
    this->node_port_ = config::kBrokerPortDef;
    if (result.size() >= 3){
      this->node_port_ = atoi(result[2].c_str());
    }
  } else {
    this->node_id_   = config::kInvalidValue;
    this->node_host_ = result[0];
    this->node_port_ = config::kBrokerPortDef;
    if (result.size() >= 2) {
      this->node_port_ = atoi(result[1].c_str());
    }
  }
  buildStrInfo();
}

NodeInfo::NodeInfo(const string& node_host, int node_port) {
  this->node_id_   = config::kInvalidValue;
  this->node_host_ = node_host;
  this->node_port_ = node_port;
  buildStrInfo();

}

NodeInfo::NodeInfo(int node_id, const string& node_host, int node_port) {
  this->node_id_   = node_id;
  this->node_host_ = node_host;
  this->node_port_ = node_port;
  buildStrInfo();
}

NodeInfo::~NodeInfo() {

}

NodeInfo& NodeInfo::operator=(const NodeInfo& target) {
  if (this != &target){
    this->node_id_   = target.node_id_;
    this->node_host_ = target.node_host_;
    this->node_port_ = target.node_port_;
    this->addr_info_ = target.addr_info_;
    this->node_info_ = target.node_info_;
  }
  return *this;
}

bool NodeInfo::operator== (const NodeInfo& target) {
  if (this == &target) {
    return true;
  }
  if (this->node_info_ == target.node_info_) {
    return true;
  }
  return false;

}

bool NodeInfo::operator< (const NodeInfo& target) const {
  return this->node_info_ < target.node_info_;
}
  
const int NodeInfo::GetNodeId() const {
  return this->node_id_;
}

const string& NodeInfo::GetHost() const {
  return this->node_host_;
}

const int NodeInfo::GetPort() const {
  return this->node_port_;
}
  
const string& NodeInfo::GetAddrInfo() const {
  return this->addr_info_;
}

const string& NodeInfo::GetNodeInfo() const {
  return this->node_info_;
}

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
  this->partition_id_ = config::kInvalidValue;
  buildPartitionKey();
}

// partition_info = broker_info#topic:partitionId
Partition::Partition(const string& partition_info) {
  // initial process
  this->topic_ = " ";
  this->partition_id_ = config::kInvalidValue;
  // parse partition_info string
  string::size_type pos=0;
  string seg_key = delimiter::kDelimiterPound;
  string token_key = delimiter::kDelimiterColon;
  // parse broker_info
  pos = partition_info.find(seg_key);
  if (pos != string::npos){
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
  this->partition_id_ = config::kInvalidValue;
  this->broker_info_ = broker_info;
  Utils::Split(part_str, result, delimiter::kDelimiterColon);
  if (result.size() >= 2) {
    this->topic_ = result[0];
    this->partition_id_ = atoi(result[1].c_str());
  }
  buildPartitionKey();
}

Partition::Partition(const NodeInfo& broker_info, const string& topic, int partition_id) {
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

bool Partition::operator== (const Partition& target) {
  if (this == &target) {
    return true;
  }
  if (this->partition_info_ == target.partition_info_) {
    return true;
  }
  return false;

}

const int Partition::GetBrokerId() const {
  return this->broker_info_.GetNodeId();
}

const string& Partition::GetBrokerHost() const {
  return this->broker_info_.GetHost();
}

const int Partition::GetBrokerPort() const {
  return this->broker_info_.GetPort();
}

const string& Partition::GetPartitionKey() const {
  return this->partition_key_;
}

const string& Partition::GetTopic() const {
  return this->topic_;
}

const NodeInfo& Partition::GetBrokerInfo() const {
  return this->broker_info_;
}

const int Partition::GetPartitionId() const {
  return this->partition_id_;
}

const string& Partition::ToString() const {
  return this->partition_info_;
}

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


// sub_info = consumerId@group#broker_info#topic:partitionId
SubscribeInfo::SubscribeInfo(const string& sub_info) {
  string::size_type pos=0;
  string seg_key = delimiter::kDelimiterPound;
  string at_key = delimiter::kDelimiterAt;
  this->consumer_id_ = " ";
  this->group_ = " ";
  // parse sub_info
  pos=sub_info.find(seg_key);
  if (pos != string::npos) {
    string consumer_info = sub_info.substr(0, pos);
    consumer_info = Utils::Trim(consumer_info);
    string partition_info = sub_info.substr(pos + seg_key.size(), sub_info.size());
    partition_info = Utils::Trim(partition_info);
    this->partition_ = Partition(partition_info);
    pos = consumer_info.find(at_key);
    this->consumer_id_ = consumer_info.substr(0, pos);
    this->consumer_id_ = Utils::Trim(this->consumer_id_);
    this->group_ = consumer_info.substr(pos + at_key.size(), consumer_info.size());
    this->group_ = Utils::Trim(this->group_);
  }
  buildSubInfo();
}

SubscribeInfo::SubscribeInfo(const string& consumer_id, 
                 const string& group, const Partition& partition) {
  this->consumer_id_ = consumer_id;
  this->group_       = group;
  this->partition_   = partition;
  buildSubInfo();
}


SubscribeInfo& SubscribeInfo::operator=(const SubscribeInfo& target) {
  if (this != &target) {
    this->consumer_id_ = target.consumer_id_;
    this->group_       = target.group_;
    this->partition_   = target.partition_;
  }
  return *this;
}

const string& SubscribeInfo::GetConsumerId() const {
  return this->consumer_id_;
}

const string& SubscribeInfo::GetGroup() const {
  return this->group_;
}

const Partition& SubscribeInfo::GetPartition() const {
  return this->partition_;
}

const int SubscribeInfo::GgetBrokerId() const {
  return this->partition_.GetBrokerId();
}

const string& SubscribeInfo::GetBrokerHost() const {
  return this->partition_.GetBrokerHost();
}

const int SubscribeInfo::GetBrokerPort() const {
  return this->partition_.GetBrokerPort();
}

const string& SubscribeInfo::GetTopic() const {
  return this->partition_.GetTopic();
}

const int SubscribeInfo::GetPartitionId() const {
  return this->partition_.GetPartitionId();
}

const string& SubscribeInfo::ToString() const {
  return this->sub_info_;
}

void SubscribeInfo::buildSubInfo() {
  stringstream ss;
  ss << this->consumer_id_;
  ss << delimiter::kDelimiterAt;
  ss << this->group_;
  ss << delimiter::kDelimiterPound;
  ss << this->partition_.ToString();
  this->sub_info_ = ss.str();
}


ConsumerEvent::ConsumerEvent() {
  this->rebalance_id_ = config::kInvalidValue;
  this->event_type_   = config::kInvalidValue;
  this->event_status_ = config::kInvalidValue;
}

ConsumerEvent::ConsumerEvent(const ConsumerEvent& target) {
  this->rebalance_id_ = target.rebalance_id_;
  this->event_type_   = target.event_type_;
  this->event_status_ = target.event_status_;
  this->subscribe_list_ = target.subscribe_list_;
}

ConsumerEvent::ConsumerEvent(long rebalance_id,int event_type, 
    const list<SubscribeInfo>& subscribeInfo_lst, int event_status) {
  list<SubscribeInfo>::const_iterator it;
  this->rebalance_id_ = rebalance_id;
  this->event_type_   = event_type;
  this->event_status_ = event_status;
  for (it = subscribeInfo_lst.begin(); it != subscribeInfo_lst.end(); ++it) {
    this->subscribe_list_.push_back(*it);
  }
}

ConsumerEvent& ConsumerEvent::operator=(const ConsumerEvent& target) {
  if(this != &target){
    this->rebalance_id_ = target.rebalance_id_;
    this->event_type_ = target.event_type_;
    this->event_status_ = target.event_status_;
    this->subscribe_list_ = target.subscribe_list_;
  }
  return *this;
}

const long ConsumerEvent::GetRebalanceId() const {
  return this->rebalance_id_;
}

const int ConsumerEvent::GetEventType() const {
  return this->event_type_;
}

const int ConsumerEvent::GetEventStatus() const {
  return this->event_status_;
}

void ConsumerEvent::SetEventType(int event_type) {
  this->event_type_ = event_type;
}

void ConsumerEvent::SetEventStatus(int event_status) {
  this->event_status_ = event_status;
}

const list<SubscribeInfo>& ConsumerEvent::GetSubscribeInfoList() const {
  return this->subscribe_list_;
}

string ConsumerEvent::ToString() {
  int count = 0;
  stringstream ss;
  list<SubscribeInfo>::const_iterator it;
  ss << "ConsumerEvent [rebalanceId=";
  ss << this->rebalance_id_;
  ss << ", type=";
  ss << this->event_type_;
  ss << ", status=";
  ss << this->event_status_;
  ss << ", subscribeInfoList=[";
  for (it = this->subscribe_list_.begin(); 
          it != this->subscribe_list_.end(); ++it) {
    if(count++ > 0) {
      ss << ",";
    }
    ss << it->ToString();
  }
  ss << "]]";
  return ss.str();   
}

};

