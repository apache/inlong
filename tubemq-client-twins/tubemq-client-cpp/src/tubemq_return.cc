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

#include "tubemq/tubemq_return.h"
#include "tubemq/const_config.h"



namespace tubemq {


PeerInfo::PeerInfo() {
  broker_host_ = "";
  partition_key_ = "";
  partition_id_ = 0;
  curr_offset_ = tb_config::kInvalidValue;
}

PeerInfo::PeerInfo(const Partition& partition, int64_t offset) {
  SetMsgSourceInfo(partition, offset);
}

PeerInfo& PeerInfo::operator=(const PeerInfo& target) {
  if (this != &target) {
    this->partition_id_ = target.partition_id_;
    this->broker_host_ = target.broker_host_;
    this->partition_key_ = target.partition_key_;
    this->curr_offset_ = target.curr_offset_;
  }
  return *this;
}

void PeerInfo::SetMsgSourceInfo(const Partition& partition, int64_t offset) {
  partition_id_ = partition.GetPartitionId();
  broker_host_ = partition.GetBrokerHost();
  partition_key_ = partition.GetPartitionKey();
  curr_offset_ = offset;
}

ConsumerResult::ConsumerResult() {
  success_ = false;
  err_code_ = tb_config::kInvalidValue;
  err_msg_ = "";
  topic_name_ = "";
  confirm_context_ = "";
}

ConsumerResult::ConsumerResult(const ConsumerResult& target) {
  this->success_ = target.success_;
  this->err_code_ = target.err_code_;
  this->err_msg_ = target.err_msg_;
  this->topic_name_ = target.topic_name_;
  this->peer_info_ = target.peer_info_;
  this->confirm_context_ = target.confirm_context_;
  this->message_list_ = target.message_list_;
}

ConsumerResult::ConsumerResult(int32_t err_code, string err_msg) {
  success_ = false;
  err_code_ = err_code;
  err_msg_ = err_msg;
  topic_name_ = "";
  confirm_context_ = "";
}

ConsumerResult::~ConsumerResult() {
  this->message_list_.clear();
  success_ = false;
  err_code_ = tb_config::kInvalidValue;
  err_msg_ = "";
  topic_name_ = "";
  confirm_context_ = "";
}

ConsumerResult& ConsumerResult::operator=(const ConsumerResult& target) {
  if (this != &target) {
    this->success_ = target.success_;
    this->err_code_ = target.err_code_;
    this->err_msg_ = target.err_msg_;
    this->topic_name_ = target.topic_name_;
    this->peer_info_ = target.peer_info_;
    this->confirm_context_ = target.confirm_context_;
    this->message_list_ = target.message_list_;
  }
  return *this;
}

void ConsumerResult::SetFailureResult(int32_t err_code, string err_msg) {
  success_ = false;
  err_code_ = err_code;
  err_msg_ = err_msg;
}

void ConsumerResult::SetFailureResult(int32_t err_code, string err_msg,
                            const string& topic_name, const PeerInfo& peer_info) {
  success_ = false;
  err_code_ = err_code;
  err_msg_ = err_msg;
  topic_name_ = topic_name;
  peer_info_ = peer_info;
}

void ConsumerResult::SetSuccessResult(int32_t err_code,
                                             const string& topic_name,
                                             const PeerInfo& peer_info,
                                             const string& confirm_context,
                                             const list<Message>& message_list) {
  this->success_ = true;
  this->err_code_ = err_code;
  this->err_msg_ = "Ok";
  this->topic_name_ = topic_name;
  this->peer_info_ = peer_info;
  this->confirm_context_ = confirm_context;
  this->message_list_ = message_list;
}

const string& ConsumerResult::GetPartitionKey() const {
  return this->peer_info_.GetPartitionKey();
}

const int64_t ConsumerResult::GetCurrOffset() const {
  return this->peer_info_.GetCurrOffset();
}


}  // namespace tubemq

