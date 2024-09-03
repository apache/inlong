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

#include "msg_protocol.h"

#include "../utils/logger.h"
#include <arpa/inet.h>
#include <assert.h>
#include <snappy.h>
#include <string.h>
namespace inlong {
char APIEncode::recvBuf[APIEncode::kRecvLen] = {0};

void APIEncode::decodeProtocoMsg(SendBuffer *buf) {
  memset(recvBuf, 0x0, kRecvLen);
  memcpy(recvBuf, buf->GetData(), buf->GetDataLen());

  char *p = recvBuf;

  uint32_t total_len = 0;
  memcpy(&total_len, p, 4);
  total_len = ntohl(total_len);
  p += 4;

  uint8_t msg_type = 0;
  memcpy(&msg_type, p, 1);
  ++p;

  uint32_t body_len = 0;
  memcpy(&body_len, p, 4);
  body_len = ntohl(body_len);
  p += 4;

  std::string body_uncompress;
  bool res = snappy::Uncompress(p, body_len, &body_uncompress);
  // LOG_DEBUG("uncompress res:%d, body_len:%d, body:%s", res,
  // body_uncompress.size(), body_uncompress.c_str());

  p += body_len; // skip body

  uint32_t attr_len = 0;
  memcpy(&attr_len, p, 4);
  attr_len = ntohl(attr_len);
  p += 4;
  // LOG_WARN("decode,attr_len:%d", attr_len);

  char attr[attr_len + 1];
  memset(attr, 0x0, attr_len);
  memcpy(attr, p, attr_len);

  if (attr_len <= 0) {
    LOG_ERROR("decode protoco error!!!!!!!, total_len:"
              << total_len << " msg_type:" << msg_type
              << " body_len:" << body_len << "attr_len :" << attr_len);
    return;
  }
}
} // namespace inlong