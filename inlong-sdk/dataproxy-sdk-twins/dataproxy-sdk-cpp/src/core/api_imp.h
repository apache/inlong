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

#ifndef INLONG_API_IMP_H
#define INLONG_API_IMP_H

#include <cstdint>
#include <functional>
#include "../config/sdk_conf.h"
#include "../manager/recv_manager.h"
#include "../manager/send_manager.h"
#include "../utils/atomic.h"

namespace inlong {
typedef int (*UserCallBack)(const char *, const char *, const char *, int32_t, const int64_t, const char *);

class ApiImp {
 public:
  ApiImp();
  ~ApiImp();
  int32_t InitApi(const char *config_file_path);

  int32_t Send(const char *inlong_group_id, const char *inlong_stream_id, const char *msg, int32_t msg_len,
               UserCallBack call_back = nullptr);

  int32_t Send(const char *inlong_group_id, const char *inlong_stream_id, const char *msg, int32_t msg_len,
               int64_t report_time, UserCallBack call_back = nullptr);
  int32_t CloseApi(int32_t max_waitms);

  int32_t AddInLongGroupId(const std::vector<std::string> &group_ids);
  int32_t SendHttp(const char *url, const char *body, int timeout = 5);

 private:
  int32_t DoInit();
  int32_t InitManager();
  int32_t SendBase(const std::string& inlong_group_id, const std::string& inlong_stream_id, const std::string& msg, UserCallBack call_back,
                   int64_t report_time = 0);

  int32_t CheckData(const std::string& inlong_group_id, const std::string& inlong_stream_id, const std::string& msg);

  int32_t ValidateParams(const char *inlong_group_id, const char *inlong_stream_id, const char *msg, int32_t msg_len);

  volatile bool inited_ = false;
  volatile bool init_succeed_ = false;
  volatile bool exit_flag_ = false;

  uint32_t max_msg_length_;
  std::string local_ip_;

  std::shared_ptr<RecvManager> recv_manager_;
  std::shared_ptr<SendManager> send_manager_;
};

}  // namespace inlong
#endif  // INLONG_API_IMP_H
