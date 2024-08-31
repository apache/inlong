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

#ifndef INLONG_SDK_CONSTANT_H
#define INLONG_SDK_CONSTANT_H

#include "string.h"
#include <stdint.h>
#include <limits>

namespace inlong {
namespace constants {
enum IsolationLevel { kLevelOne = 1, kLevelSecond = 2, kLevelThird = 3 };
static const int32_t kMaxRequestTDMTimes = 4;
static const char kAttrFormat[] =
    "__addcol1__reptime=yyyymmddHHMMSS&__addcol2_ip=xxx.xxx.xxx.xxx";
static const int32_t kAttrLen = strlen(kAttrFormat);

static const char kVersion[] = "inlong-sdk-cpp-v3.2";
static const uint16_t kBinaryMagic = 0xEE01;
static const uint32_t kBinPackMethod = 7;
static const uint8_t kBinSnappyFlag = 1 << 5;

static const int32_t kPerGroupidThreadNums = 1;
static const int32_t kSendBufSize = 10240000;
static const int32_t kRecvBufSize = 10240000;
static const uint32_t kMaxGroupIdNum = 50;
static const uint32_t kMaxStreamIdNum = 100;
static const uint32_t kMaxCacheNum = 10;
static const uint32_t kMaxInstance = 30;

static const int32_t kDispatchIntervalZip = 8;
static const int32_t kDispatchIntervalSend = 10;
static const int32_t kLoadBalanceInterval = 300000;
static const int32_t kHeartBeatInterval = 60000;
static const bool kEnableBalance = true;
static const bool kEnableLocalCache = true;
static const bool kEnableShareMsg = true;

static const bool kEnablePack = true;
static const uint32_t kPackSize = 409600;
static const uint32_t kPackTimeout = 3000;
static const uint32_t kExtPackSize = 409600;

static const bool kEnableZip = true;
static const uint32_t kMinZipLen = 512;

static const uint32_t kLogNum = 5;
static const uint32_t kLogSize = 100 * 1024 * 1024;
static const uint8_t kLogLevel = 2;
static const char kLogPath[] = "./";

static const char kManagerURL[] =
    "http://127.0.0.1:8099/inlong/manager/openapi/dataproxy/getIpList";
static const bool kEnableManagerFromCluster = false;
static const char kManagerClusterURL[] =
    "http://127.0.0.1:8099/heartbeat/"
    "dataproxy_ip_v2?cluster_id=0&net_tag=normal";
static const uint32_t kManagerUpdateInterval = 2;
static const uint32_t kManagerTimeout = 5;
static const uint32_t kMaxProxyNum = 8;
static const uint32_t kReserveProxyNum = 2;

static const bool kEnableTCPNagle = true;
static const uint32_t kTcpIdleTime = 600000;
static const uint32_t kTcpDetectionInterval = 60000;
static const uint32_t kMaxRetryIntervalMs= 3000;
static const uint32_t kRetryIntervalMs= 200;
static const int32_t kRetryTimes = 1;
static const uint32_t kProxyRepeatTimes = 1;

static const char kSerIP[] = "127.0.0.1";
static const uint32_t kSerPort = 46801;
static const uint32_t kMsgType = 7;

static const bool kEnableSetAffinity = false;
static const uint32_t kMaskCPUAffinity = 0xff;
static const uint16_t kExtendField = 0;
static const uint64_t kMaxSnowFlake = std::numeric_limits<uint64_t>::max();
static const bool kExtendReport = false;

// http basic auth
static const char kBasicAuthHeader[] = "Authorization:";
static const char kBasicAuthPrefix[] = "Basic";
static const char kBasicAuthSeparator[] = " ";
static const char kBasicAuthJoiner[] = ":";
static const char kProtocolType[] = "TCP";
static const bool kNeedAuth = false;

static const uint32_t kMaxAttrLen = 2048;
const uint32_t ATTR_LENGTH = 10;
static const bool kEnableIsolation = false;

static const int32_t kDefaultLoadThreshold = 200;
const uint32_t MAX_STAT = 10000000;
static const int32_t kWeight[30] = {1,  1,  1,  1,  1,  2,  2,  2,   2,   2,
                                    3,  3,  3,  3,  3,  6,  6,  6,   6,   6,
                                    12, 12, 12, 12, 12, 48, 96, 192, 384, 1000};

static const char kCacheFile[] = ".proxy_list.ini";
static const char kCacheTmpFile[] = ".proxy_list.ini.tmp";
const int MAX_RETRY = 10;
static const int kMetricIntervalMinutes = 1;

} // namespace constants
} // namespace inlong
#endif // INLONG_SDK_CONSTANT_H