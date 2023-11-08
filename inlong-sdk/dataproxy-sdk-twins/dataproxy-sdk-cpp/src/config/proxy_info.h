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

#include <string>
#include <vector>

#ifndef INLONG_SDK_PROXY_INFO_H
#define INLONG_SDK_PROXY_INFO_H
namespace inlong {

class ProxyInfo {
private:
  std::string proxy_str_id_;
  std::string ip_;
  int32_t port_;
  int32_t load_;

public:
  ProxyInfo(std::string proxy_str_id, std::string ip, int32_t port,int32_t load)
      : proxy_str_id_(proxy_str_id), ip_(ip), port_(port), load_(load) {}
  ProxyInfo(){};
  void setIp(const std::string& ip) { ip_ = ip; }
  void setPort(int32_t port) { port_ = port; }
  std::string ip() const { return ip_; }
  int32_t port() const { return port_; }
  int32_t GetLoad() const { return load_; }
};
using ProxyInfoVec = std::vector<ProxyInfo>;
} // namespace inlong
#endif // INLONG_SDK_PROXY_INFO_H