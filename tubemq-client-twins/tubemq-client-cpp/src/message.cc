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

#include "tubemq/message.h"

#include <string.h>

#include <sstream>

#include "tubemq/const_config.h"
#include "tubemq/utils.h"

namespace tubemq {

using std::stringstream;


// message flag's properties settings
static const int32_t kMsgFlagIncProperties = 0x01;
// reserved property key Filter Item
static const string kRsvPropKeyFilterItem = "$msgType$";
// reserved property key message send time
static const string kRsvPropKeyMsgTime = "$msgTime$";

Message::Message() {
  this->topic_ = "";
  this->flag_ = 0;
  this->message_id_ = tb_config::kInvalidValue;
  this->data_ = NULL;
  this->datalen_ = 0;
  this->properties_.clear();
}

Message::Message(const Message& target) {
  this->topic_ = target.topic_;
  this->message_id_ = target.message_id_;
  copyData(target.data_, target.datalen_);
  copyProperties(target.properties_);
  this->flag_ = target.flag_;
}

Message::Message(const string& topic, const char* data, uint32_t datalen) {
  this->topic_ = topic;
  this->flag_ = 0;
  this->message_id_ = tb_config::kInvalidValue;
  copyData(data, datalen);
  this->properties_.clear();
}

Message::~Message() { clearData(); }

Message& Message::operator=(const Message& target) {
  if (this == &target) return *this;
  this->topic_ = target.topic_;
  this->message_id_ = target.message_id_;
  clearData();
  copyData(target.data_, target.datalen_);
  copyProperties(target.properties_);
  this->flag_ = target.flag_;
  return *this;
}

const int64_t Message::GetMessageId() const { return this->message_id_; }

void Message::SetMessageId(int64_t message_id) { this->message_id_ = message_id; }

const string& Message::GetTopic() const { return this->topic_; }

void Message::SetTopic(const string& topic) { this->topic_ = topic; }

const char* Message::GetData() const { return this->data_; }

uint32_t Message::GetDataLength() const { return this->datalen_; }

void Message::setData(const char* data, uint32_t datalen) {
  clearData();
  copyData(data, datalen);
}

const int32_t Message::GetFlag() const { return this->flag_; }

void Message::SetFlag(int32_t flag) { this->flag_ = flag; }

const map<string, string>& Message::GetProperties() const { return this->properties_; }

int32_t Message::GetProperties(string& attribute) {
  attribute.clear();
  map<string, string>::iterator it_map;
  for (it_map = this->properties_.begin(); it_map != this->properties_.end(); ++it_map) {
    if (!attribute.empty()) {
      attribute += delimiter::kDelimiterComma;
    }
    attribute += it_map->first;
    attribute += delimiter::kDelimiterEqual;
    attribute += it_map->second;
  }
  return attribute.length();
}

bool Message::HasProperty(const string& key) {
  map<string, string>::iterator it_map;
  string trimed_key = Utils::Trim(key);
  if (!trimed_key.empty()) {
    it_map = this->properties_.find(trimed_key);
    if (it_map != this->properties_.end()) {
      return true;
    }
  }
  return false;
}

bool Message::GetProperty(const string& key, string& value) {
  map<string, string>::iterator it_map;
  string trimed_key = Utils::Trim(key);
  if (!trimed_key.empty()) {
    it_map = this->properties_.find(trimed_key);
    if (it_map != this->properties_.end()) {
      value = it_map->second;
      return true;
    }
  }
  return false;
}

bool Message::GetFilterItem(string& value) { return GetProperty(kRsvPropKeyFilterItem, value); }

bool Message::AddProperty(string& err_info, const string& key, const string& value) {
  string trimed_key = Utils::Trim(key);
  string trimed_value = Utils::Trim(value);
  if (trimed_key.empty() || trimed_value.empty()) {
    err_info = "Not allowed null value of parmeter key or value";
    return false;
  }
  if ((string::npos != trimed_key.find(delimiter::kDelimiterComma)) ||
      (string::npos != trimed_key.find(delimiter::kDelimiterEqual))) {
    stringstream ss;
    ss << "Reserved token '";
    ss << delimiter::kDelimiterComma;
    ss << "' or '";
    ss << delimiter::kDelimiterEqual;
    ss << "' in parmeter key!";
    err_info = ss.str();
    return false;
  }
  if ((string::npos != trimed_value.find(delimiter::kDelimiterComma)) ||
      (string::npos != trimed_value.find(delimiter::kDelimiterEqual))) {
    stringstream ss;
    ss << "Reserved token '";
    ss << delimiter::kDelimiterComma;
    ss << "' or '";
    ss << delimiter::kDelimiterEqual;
    ss << "' in parmeter value!";
    err_info = ss.str();
    return false;
  }
  if (trimed_key == kRsvPropKeyFilterItem || trimed_key == kRsvPropKeyMsgTime) {
    stringstream ss;
    ss << "Reserved token '";
    ss << kRsvPropKeyFilterItem;
    ss << "' or '";
    ss << kRsvPropKeyMsgTime;
    ss << "' must not be used in parmeter key!";
    err_info = ss.str();
    return false;
  }
  // add key and value
  this->properties_[trimed_key] = trimed_value;
  if (!this->properties_.empty()) {
    this->flag_ |= kMsgFlagIncProperties;
  }
  err_info = "Ok";
  return true;
}

void Message::clearData() {
  if (this->data_ != NULL) {
    delete[] this->data_;
    this->data_ = NULL;
    this->datalen_ = 0;
  }
}

void Message::copyData(const char* data, uint32_t datalen) {
  if (data == NULL) {
    this->data_ = NULL;
    this->datalen_ = 0;
  } else {
    this->datalen_ = datalen;
    this->data_ = new char[datalen];
    memset(this->data_, 0, datalen);
    memcpy(this->data_, data, datalen);
  }
}

void Message::copyProperties(const map<string, string>& properties) {
  this->properties_.clear();
  map<string, string>::const_iterator it_map;
  for (it_map = properties.begin(); it_map != properties.end(); ++it_map) {
    this->properties_[it_map->first] = it_map->second;
  }
  if (!this->properties_.empty()) {
    this->flag_ |= kMsgFlagIncProperties;
  }
}

}  // namespace tubemq
