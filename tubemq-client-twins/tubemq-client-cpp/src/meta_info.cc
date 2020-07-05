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

NodeInfo::NodeInfo(bool is_broker, const string& node_info) {
  vector<string> result;
  Utils::Split(node_info, result, delimiter::kDelimiterColon);
  if (is_broker) {
    this->node_id_   = atoi(result[0].c_str());
    this->node_host_ = result[1];
    this->node_port_ = config::kBrokerPortDef;
    if(result.size() >= 3){
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
  return this->node_id_ < target.node_id_;
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


}

