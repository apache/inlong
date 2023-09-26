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

#ifndef SDK_USER_MSG_H_
#define SDK_USER_MSG_H_

#include <functional>
#include <memory>
#include <stdint.h>
#include <string>
namespace inlong {
typedef int (*UserCallBack)(const char *, const char *, const char *, int32_t,
                            const int64_t, const char *);

struct SdkMsg {
  std::string msg_;
  std::string client_ip_;
  int64_t report_time_;
  UserCallBack cb_;

  int64_t user_report_time_;
  std::string user_client_ip_;

  std::string data_pack_format_attr_;
  SdkMsg(const std::string &mmsg, const std::string &mclient_ip,
         int64_t mreport_time, UserCallBack mcb, const std::string &attr,
         const std::string &u_ip, int64_t u_time)
      : msg_(mmsg), client_ip_(mclient_ip), report_time_(mreport_time),
        cb_(mcb), user_report_time_(u_time), user_client_ip_(u_ip),
        data_pack_format_attr_(attr) {}
};
using SdkMsgPtr = std::shared_ptr<SdkMsg>;

} // namespace inlong

#endif // SDK_USER_MSG_H_