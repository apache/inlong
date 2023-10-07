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

#ifndef INLONG_SDK_MSG_PROTOCOL_H
#define INLONG_SDK_MSG_PROTOCOL_H

#include "../utils/send_buffer.h"
#include <memory>
#include <stdint.h>
#include <string>
#include <vector>

namespace inlong {
//  totalLen(4)|msgtype(1)|bodyLen(4)|body(x)|attrLen(4)|attr(attr_len)
#pragma pack(1)
struct ProtocolMsgHead {
  uint32_t total_len;
  char msg_type;
};
struct ProtocolMsgBody {
  uint32_t body_len;
  char body[0];
};
struct ProtocolMsgTail {
  uint32_t attr_len;
  char attr[0];
};
//  totalLen(4)|msgtype(1)|bid_num(2)|tid_num(2)|ext_field(2)|data_time(4)|cnt(2)|uniq(4)|bodyLen(4)|body(x)|attrLen(4)|attr(attr_len)|magic(2)
struct BinaryMsgHead {
  uint32_t total_len;
  char msg_type;
  uint16_t bid_num;
  uint16_t tid_num;
  uint16_t ext_field;
  uint32_t data_time;
  uint16_t cnt;
  uint32_t uniq;
};

struct BinaryMsgAck {
  uint32_t total_len;
  char msg_type;
  uint32_t uniq;
  uint16_t attr_len;
  char attr[0];
  uint16_t magic;
};

struct BinaryHB {
  uint32_t total_len;
  char msg_type;
  uint32_t data_time;
  uint8_t body_ver;  // body_ver=1
  uint32_t body_len; // body_len=0
  char body[0];
  uint16_t attr_len; // attr_len=0;
  char attr[0];
  uint16_t magic;
};
#pragma pack()

class APIEncode {
public:
  static void decodeProtocoMsg(SendBuffer *buf);

  static const uint32_t kRecvLen = 1024 * 1024 * 11;
  static char recvBuf[kRecvLen];
};

} // namespace inlong

#endif // INLONG_SDK_MSG_PROTOCOL_H