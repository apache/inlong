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

#ifndef TUBEMQ_CLIENT_CONST_CONFIG_H_
#define TUBEMQ_CLIENT_CONST_CONFIG_H_

#include <stdint.h>

#include <map>
#include <string>

namespace tubemq {

using std::string;

// configuration value setting
namespace config {
// heartbeat period define
static const int32_t kHeartBeatPeriodDef = 10;
static const int32_t kHeartBeatFailRetryTimesDef = 5;
static const int32_t kHeartBeatSleepPeriodDef = 60;
// max masterAddrInfo length
static const int32_t kMasterAddrInfoMaxLength = 1024;

// max TopicName length
static const int32_t kTopicNameMaxLength = 64;
// max Consume GroupName length
static const int32_t kGroupNameMaxLength = 1024;
// max filter item length
static const int32_t kFilterItemMaxLength = 256;
// max allowed filter item count
static const int32_t kFilterItemMaxCount = 500;
// max session key length
static const int32_t kSessionKeyMaxLength = 1024;

// max subscribe info report times
static const int32_t kSubInfoReportMaxIntervalTimes = 6;
// default message not found response wait period
static const int32_t kMsgNotfoundWaitPeriodMsDef = 200;
// default confirm wait period if rebalance meeting
static const int32_t kRebConfirmWaitPeriodMsDef = 3000;
// max confirm wait period anyway
static const int32_t kConfirmWaitPeriodMsMax = 60000;
// default rebalance wait if shutdown meeting
static const int32_t kRebWaitPeriodWhenShutdownMs = 10000;

// max int value
static const int32_t kMaxIntValue = 0x7fffffff;
// max long value
static const int64_t kMaxLongValue = 0x7fffffffffffffffL;

// default broker port
static const uint32_t kBrokerPortDef = 8123;
// default broker TLS port
static const uint32_t kBrokerTlsPortDef = 8124;

// invalid value
static const int32_t kInvalidValue = -2;

}  // namespace config

namespace delimiter {
static const string kDelimiterDot = ".";
static const string kDelimiterEqual = "=";
static const string kDelimiterAnd = "&";
static const string kDelimiterComma = ",";
static const string kDelimiterColon = ":";
static const string kDelimiterAt = "@";
static const string kDelimiterPound = "#";
static const string kDelimiterSemicolon = ";";
// Double slash
static const string kDelimiterDbSlash = "//";
// left square bracket
static const string kDelimiterLftSB = "[";
// right square bracket
static const string kDelimiterRgtSB = "]";

}  // namespace delimiter

}  // namespace tubemq

#endif  // TUBEMQ_CLIENT_CONST_CONFIG_H_
