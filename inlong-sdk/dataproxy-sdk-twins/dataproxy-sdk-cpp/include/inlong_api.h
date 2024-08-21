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

#ifndef INLONG_SDK_API_H
#define INLONG_SDK_API_H

#include <clocale>
#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

namespace inlong {

typedef  int (*UserCallBack)(const char *, const char *, const char *, int32_t,
                             const int64_t, const char *);

class ApiImp;

class InLongApi {
 public:
  InLongApi();
  ~InLongApi();
  int32_t InitApi(const char *config_path);

  int32_t AddInLongGroupId(const std::vector<std::string> &group_ids);

  int32_t Send(const char *inlong_group_id, const char *inlong_stream_id, const char *msg, int32_t msg_len,
               UserCallBack call_back = nullptr);

  int32_t Send(const char *inlong_group_id, const char *inlong_stream_id, const char *msg, int32_t msg_len,
               int64_t data_time, UserCallBack call_back = nullptr);

  int32_t CloseApi(int32_t max_waitms);

 private:
  std::shared_ptr<ApiImp> api_impl_;
};
}  // namespace inlong
#endif  // INLONG_SDK_API_H
