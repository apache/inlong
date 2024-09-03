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

#ifndef SDK_USER_MSG_H
#define SDK_USER_MSG_H

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

  std::string inlong_group_id_;
  std::string inlong_stream_id_;

  SdkMsg(const std::string &mmsg,
         const std::string &mclient_ip,
         int64_t mreport_time,
         UserCallBack mcb,
         const std::string &attr,
         const std::string &u_ip,
         int64_t u_time,
         const std::string &inlong_group_id,
         const std::string &inlong_stream_id)
      : msg_(mmsg), client_ip_(mclient_ip), report_time_(mreport_time),
        cb_(mcb), user_report_time_(u_time), user_client_ip_(u_ip),
        data_pack_format_attr_(attr),
        inlong_group_id_(inlong_group_id),
        inlong_stream_id_(inlong_stream_id) {}
  SdkMsg() {};
  void setMsg(const std::string &msg) { msg_ = msg; }
  void setClientIp(const std::string &clientIp) { client_ip_ = clientIp; }
  void setReportTime(uint64_t reportTime) { report_time_ = reportTime; }
  void setCb(UserCallBack cb) { cb_ = cb; }
  void setUserReportTime(uint64_t userReportTime) { user_report_time_ = userReportTime; }
  void setUserClientIp(const std::string &userClientIp) { user_client_ip_ = userClientIp; }
  void setDataPackFormatAttr(const std::string &dataPackFormatAttr) { data_pack_format_attr_ = dataPackFormatAttr; }
  void setGroupId(const std::string &inlong_group_id) { inlong_group_id_ = inlong_group_id; }
  void setStreamId(const std::string &inlong_stream_id) { inlong_stream_id_ = inlong_stream_id; }

  void clear() {
    msg_ = "";
    client_ip_ = "";
    report_time_ = 0;
    cb_ = nullptr;
    user_report_time_ = 0;
    user_client_ip_ = "";
    data_pack_format_attr_ = "";
    inlong_group_id_ = "";
    inlong_stream_id_ = "";
  }
};
using SdkMsgPtr = std::shared_ptr<SdkMsg>;

} // namespace inlong
#endif // SDK_USER_MSG_H