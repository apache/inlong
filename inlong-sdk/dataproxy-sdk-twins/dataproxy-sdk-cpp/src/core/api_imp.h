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

#ifndef INLONG_SDK_API_IMP_H
#define INLONG_SDK_API_IMP_H

#include "../manager/recv_manager.h"
#include "../manager/send_manager.h"
#include "../utils/atomic.h"
#include "sdk_conf.h"
#include <cstdint>
#include <functional>

namespace inlong {
using UserCallBack =
    std::function<int32_t(const char *, const char *, const char *, int32_t,
                          const int64_t, const char *)>;

class ApiImp {
public:
  ApiImp();
  ~ApiImp();
  int32_t InitApi(const char *config_file_path);

  int32_t Send(const char *business_id, const char *table_id, const char *msg,
               int32_t msg_len, UserCallBack call_back = nullptr);

  int32_t CloseApi(int32_t max_waitms);

  int32_t AddBid(const std::vector<std::string> &bids);

private:
  int32_t DoInit();
  int32_t InitManager();
  int32_t SendBase(const std::string inlong_group_id,
                   const std::string inlong_stream_id,
                   const std::string client_ip, int64_t report_time,
                   const std::string msg, UserCallBack call_back);

  int32_t CheckData(const std::string inlong_group_id,
                    const std::string inlong_stream_id, const std::string msg);

  AtomicInt user_exit_flag_{0};
  volatile bool init_flag_ = false;
  volatile bool inited_ = false;
  volatile bool init_succeed_ = false;
  AtomicInt buf_full_{0};
  uint32_t max_msg_length_;
  std::string local_ip_;

  std::shared_ptr<RecvManager> recv_manager_;
  std::shared_ptr<SendManager> send_manager_;
};

} // namespace inlong
#endif // INLONG_SDK_API_IMP_H
