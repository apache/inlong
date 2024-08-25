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

#include "parse_json.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/document.h"
#include "../core/api_code.h"
#include "../utils/logger.h"

namespace inlong {
static const char kJsonKeySuccess[] = "success";
static const char kJsonKeyData[] = "data";
static const char kJsonKeyNodeList[] = "nodeList";
static const char kJsonKeyClusterId[] = "clusterId";
static const char kJsonKeyLoad[] = "load";
static const char kJsonKeyIp[] = "ip";
static const char kJsonKeyPort[] = "port";
static const char kJsonKeyId[] = "id";
static const char kJsonKeySize[] = "size";
static const char kJsonKeyClusterIdV2[] = "cluster_id";
static const char kJsonKeyAddress[] = "address";
static const char kJsonKeyHost[] = "host";

int32_t ParseJson::ParseProxyInfo(const std::string &inlong_group_id,
                                  const std::string &meta_data,
                                  ProxyInfoVec &proxy_info_vec,
                                  GroupId2ClusterIdMap &group_id_2_cluster_id) {
  rapidjson::Document doc;
  if (doc.Parse(meta_data.c_str()).HasParseError()) {
    return SdkCode::kErrorParseJson;
  }

  if (!(doc.HasMember(kJsonKeySuccess) && doc[kJsonKeySuccess].IsBool() &&
      doc[kJsonKeySuccess].GetBool())) {
    return SdkCode::kErrorParseJson;
  }

  if (!doc.HasMember(kJsonKeyData) || doc[kJsonKeyData].IsNull()) {
    return SdkCode::kErrorParseJson;
  }

  const rapidjson::Value &clusterInfo = doc[kJsonKeyData];
  if (!clusterInfo.HasMember(kJsonKeyNodeList) || clusterInfo[kJsonKeyNodeList].IsNull()) {
    return SdkCode::kErrorParseJson;
  }

  const rapidjson::Value &nodeList = clusterInfo[kJsonKeyNodeList];
  if (nodeList.GetArray().Size() <= 0) {
    return SdkCode::kErrorParseJson;
  }

  if (!clusterInfo.HasMember(kJsonKeyClusterId) ||
      !clusterInfo[kJsonKeyClusterId].IsInt() ||
      clusterInfo[kJsonKeyClusterId].GetInt() <= 0) {
    return SdkCode::kErrorParseJson;
  }

  group_id_2_cluster_id[inlong_group_id] = clusterInfo[kJsonKeyClusterId].GetInt();

  int32_t load = 0;
  if (clusterInfo.HasMember(kJsonKeyLoad) && clusterInfo[kJsonKeyLoad].IsInt() &&
      !clusterInfo[kJsonKeyLoad].IsNull()) {
    load = clusterInfo[kJsonKeyLoad].GetInt();
  }

  for (auto &proxy : nodeList.GetArray()) {
    std::string ip;
    std::string id;
    int32_t port;
    if (!proxy.HasMember(kJsonKeyIp) || proxy[kJsonKeyIp].IsNull()) {
      continue;
    }
    ip = proxy[kJsonKeyIp].GetString();

    if (!proxy.HasMember(kJsonKeyPort) || proxy[kJsonKeyPort].IsNull()) {
      continue;
    }
    if (proxy[kJsonKeyPort].IsString()) port = std::stoi(proxy[kJsonKeyPort].GetString());
    if (proxy[kJsonKeyPort].IsInt()) port = proxy[kJsonKeyPort].GetInt();

    if (!proxy.HasMember(kJsonKeyId) || proxy[kJsonKeyId].IsNull()) {
      continue;
    }
    if (proxy[kJsonKeyId].IsString()) id = proxy[kJsonKeyId].GetString();
    if (proxy[kJsonKeyId].IsInt()) id = proxy[kJsonKeyId].GetInt();

    proxy_info_vec.emplace_back(id, ip, port, load);
  }
  return SdkCode::kSuccess;
}
int32_t ParseJson::ParseProxyInfo(const std::string &inlong_group_id, const std::string &meta_data,
                                  GroupId2ClusterIdMap &group_id_2_cluster_id, ProxyInfoVec &proxy_info_vec) {
  rapidjson::Document doc;
  if (doc.Parse(meta_data.c_str()).HasParseError()) {
    return SdkCode::kErrorParseJson;
  }

  if (!doc.HasMember(kJsonKeySize) || !doc[kJsonKeySize].IsInt() || doc[kJsonKeySize].IsNull()
      || doc[kJsonKeySize].GetInt() <= 0) {
    return SdkCode::kErrorParseJson;
  }

  if (doc.HasMember(kJsonKeyClusterIdV2) && doc[kJsonKeyClusterIdV2].IsInt() && !doc[kJsonKeyClusterIdV2].IsNull()) {
    const rapidjson::Value &obj = doc[kJsonKeyClusterIdV2];
    group_id_2_cluster_id[inlong_group_id] = obj.GetInt();
  } else {
    return SdkCode::kErrorParseJson;
  }

  int32_t load = 0;
  if (doc.HasMember(kJsonKeyLoad) && doc[kJsonKeyLoad].IsInt() && !doc[kJsonKeyLoad].IsNull()) {
    load = doc[kJsonKeyLoad].GetInt();
  }

  if (!doc.HasMember(kJsonKeyAddress) || doc[kJsonKeyAddress].IsNull()) {
    return SdkCode::kErrorParseJson;
  }

  const rapidjson::Value &host_list = doc[kJsonKeyAddress];
  for (auto &info : host_list.GetArray()) {
    std::string id, ip;
    if (!info.HasMember(kJsonKeyHost) || info[kJsonKeyHost].IsNull()) {
      continue;
    }
    ip = info[kJsonKeyHost].GetString();

    if (!info.HasMember(kJsonKeyPort) || info[kJsonKeyPort].IsNull()) {
      continue;
    }

    int32_t port;
    if (info[kJsonKeyPort].IsString()) port = std::stoi(info[kJsonKeyPort].GetString());
    if (info[kJsonKeyPort].IsInt()) port = info[kJsonKeyPort].GetInt();

    if (!info.HasMember(kJsonKeyId) || info[kJsonKeyId].IsNull()) {
      continue;
    }
    if (info[kJsonKeyId].IsString()) id = info[kJsonKeyId].GetString();
    if (info[kJsonKeyId].IsInt()) id = std::to_string(info[kJsonKeyId].GetInt());

    proxy_info_vec.emplace_back(id, ip, port, load);
  }
  return SdkCode::kSuccess;
}
}

