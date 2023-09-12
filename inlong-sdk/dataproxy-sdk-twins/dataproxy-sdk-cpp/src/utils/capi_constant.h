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

namespace inlong {
namespace constants {
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

static const int32_t kDispatchIntervalZip = 8;
static const int32_t kDispatchIntervalSend = 10;

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
static const bool kEnableBusURLFromCluster = false;
static const char kBusClusterURL[] =
    "127.0.0.1/heartbeat/tdbus_ip_v2?cluster_id=0&net_tag=all";
static const uint32_t kBusUpdateInterval = 2;
static const uint32_t kBusURLTimeout = 5;
static const uint32_t kMaxBusNum = 200;

static const bool kEnableTCPNagle = true;
static const uint32_t kTcpIdleTime = 600000;
static const uint32_t kTcpDetectionInterval = 60000;

static const char kSerIP[] = "127.0.0.1";
static const uint32_t kSerPort = 46801;
static const uint32_t kMsgType = 7;

static const bool kEnableSetAffinity = false;
static const uint32_t kMaskCPUAffinity = 0xff;
static const uint16_t kExtendField = 0;

// http basic auth
static const char kBasicAuthHeader[] = "Authorization:";
static const char kBasicAuthPrefix[] = "Basic";
static const char kBasicAuthSeparator[] = " ";
static const char kBasicAuthJoiner[] = ":";
static const char kProtocolType[] = "TCP";
static const bool kNeedAuth = false;

static const uint32_t kMaxAttrLen = 2048;

} // namespace constants
} // namespace inlong
#endif // INLONG_SDK_CONSTANT_H