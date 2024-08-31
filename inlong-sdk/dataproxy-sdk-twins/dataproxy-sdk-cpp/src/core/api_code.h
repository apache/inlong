/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef INLONG_SDK_API_CODE_H
#define INLONG_SDK_API_CODE_H
namespace inlong {
enum SdkCode {
  kSuccess = 0,
  kMultiInit = 4,
  kErrorInit = 5,
  kMsgTooLong = 6,
  kInvalidInput = 7,
  kRecvBufferFull = 8,
  kFailGetConn = 9,
  kSendAfterClose = 10,
  kMultiExits = 11,
  kInvalidBid = 12,
  kFailGetBufferPool = 13,
  kFailGetSendBuf = 14,
  kMsgEmpty = 15,
  kErrorCURL = 16,
  kErrorParseJson = 17,
  kFailGetRevGroup = 18,
  kFailGetProxyConf = 19,
  kFailSendProxy = 20,
  kFailSParseResponse = 21,
  kSendBeforeInit = 22,
  kFailMallocBuf = 23,
  kMsgSizeLargerThanPackSize = 24,
  kSendBufferFull = 25,
  kBufferManagerFull = 26
};
}

#endif // INLONG_SDK_API_CODE_H
