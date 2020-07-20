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

#ifndef TUBEMQ_CLIENT_UTILS_H_
#define TUBEMQ_CLIENT_UTILS_H_

#include <stdint.h>

#include <map>
#include <string>
#include <vector>

namespace tubemq {

using std::map;
using std::string;
using std::vector;

class Utils {
 public:
  // trim string info
  static string Trim(const string& source);
  // split string to vector
  static void Split(const string& source, vector<string>& result, const string& delimiter);
  // split string to map<string, int>
  static void Split(const string& source, map<string, int32_t>& result, const string& delimiter_step1,
                    const string& delimiter_step2);
  static void Join(const vector<string>& vec, const string& delimiter, string& target);
  static bool ValidString(string& err_info, const string& source, bool allow_empty, bool pat_match,
                          bool check_max_length, unsigned int maxlen);
  static bool ValidGroupName(string& err_info, const string& group_name, string& tgt_group_name);
  static bool ValidFilterItem(string& err_info, const string& src_filteritem,
                              string& tgt_filteritem);
  static string Int2str(int32_t data);
  static string Long2str(int64_t data);
  static uint32_t IpToInt(const string& ipv4_addr);
  static int64_t GetCurrentTimeMillis();
  static bool ValidConfigFile(string& err_info, const string& conf_file);
};

}  // namespace tubemq

#endif  // TUBEMQ_CLIENT_UTILS_H_
