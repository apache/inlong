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
      
#ifndef _TUBEMQ_CLIENT_CONST_CONFIG_H_
#define _TUBEMQ_CLIENT_CONST_CONFIG_H_

#include <map>
#include <string>

namespace tubemq {

using namespace std;

// configuration value setting
namespace config {
// rpc timeout define  
static const int kRpcTimoutDef = 15;
static const int kRpcTimoutMax = 300;
static const int kRpcTimoutMin = 8;
// heartbeat period define
static const int kHeartBeatPeriodDef = 10;
static const int kHeartBeatFailRetryTimesDef = 5;
static const int kHeartBeatSleepPeriodDef = 60;
// max masterAddrInfo length
static const int kMasterAddrInfoMaxLength = 1024;

// max TopicName length
static const int kTopicNameMaxLength = 64;
// max Consume GroupName length
static const int kGroupNameMaxLength = 1024;
// max filter item length
static const int kFilterItemMaxLength = 256;
// max allowed filter item count
static const int kFilterItemMaxCount = 500;
// max session key length
static const int kSessionKeyMaxLength = 1024;


// max subscribe info report times
static const int kSubInfoReportMaxIntervalTimes = 6;
// default message not found response wait period
static const int kMsgNotfoundWaitPeriodMsDef = 200;
// default confirm wait period if rebalance meeting
static const int kRebConfirmWaitPeriodMsDef = 3000;
// max confirm wait period anyway
static const int kConfirmWaitPeriodMsMax = 60000;
// default rebalance wait if shutdown meeting
static const int kRebWaitPeriodWhenShutdownMs = 10000;

// max int value
static const int kMaxIntValue = 0x7fffffff;
// max long value
static const long kMaxLongValue = 0x7fffffffffffffffL;

// invalid value
static const int kInvalidValue = -2;


}  // namespace config


namespace delimiter {
  static const string kDelimiterEqual = "=";
  static const string kDelimiterAnd   = "&";
  static const string kDelimiterComma = ",";
  static const string kDelimiterColon = ":";
  static const string kDelimiterAt    = "@";
  static const string kDelimiterPound = "#";
} // namespace delimiter


 
}

#endif

