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

#ifndef INLONG_ENVIRONMENT_H
#define INLONG_ENVIRONMENT_H

#include <string>
#include <sstream>
namespace inlong {
class Environment {
 public:
  std::string type_;
  std::string version_;
  std::string ip_;
  uint64_t pid_;
  bool extend_report_;
  const std::string &getType() const { return type_; }
  void setType(const std::string &type) { type_ = type; }
  std::string getVersion() { return version_; }
  void setVersion(const std::string &version) { version_ = version; }
  const std::string &getIp() const { return ip_; }
  void setIp(const std::string &ip) { ip_ = ip; }
  uint64_t getPid() const { return pid_; }
  void setPid(uint64_t pid) { pid_ = pid; }
  void SetExtendReport(bool extend_report) {extend_report_ = extend_report;}

  std::string ToString() const {
    std::stringstream metric;
    metric << "local ip[" << ip_ << "] ";
    metric << "version[" << version_ << "] ";
    metric << "pid[" << pid_ << "] ";
    metric << "extend report[" << extend_report_ << "]";
    return metric.str();
  }
};
} // namespace inlong
#endif  // INLONG_ENVIRONMENT_H