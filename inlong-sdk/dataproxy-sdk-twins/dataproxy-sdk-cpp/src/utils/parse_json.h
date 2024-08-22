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

#ifndef DATAPROXYSDK_INLONG_SDK_DATAPROXY_SDK_TWINS_DATAPROXY_SDK_CPP_SRC_UTILS_PARSE_JSON_H
#define DATAPROXYSDK_INLONG_SDK_DATAPROXY_SDK_TWINS_DATAPROXY_SDK_CPP_SRC_UTILS_PARSE_JSON_H

#include <string>
#include <unordered_map>
#include "../config/proxy_info.h"

namespace inlong {
using GroupId2ClusterIdMap = std::unordered_map<std::string, int32_t>;
class ParseJson {
 public:
  static int32_t ParseProxyInfo(const std::string &inlong_group_id, const std::string &meta_data,
                                ProxyInfoVec &proxy_info_vec, GroupId2ClusterIdMap &group_id_2_cluster_id);
  static int32_t ParseProxyInfo(const std::string &inlong_group_id, const std::string &meta_data,
                                GroupId2ClusterIdMap &group_id_2_cluster_id, ProxyInfoVec &proxy_info_vec);
};
}
#endif //DATAPROXYSDK_INLONG_SDK_DATAPROXY_SDK_TWINS_DATAPROXY_SDK_CPP_SRC_UTILS_PARSE_JSON_H
