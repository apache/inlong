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

package org.apache.inlong.manager.common.pojo.sort;

import io.swagger.annotations.ApiModel;
import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
@Builder
@ApiModel("Sort source sdk config")
public class SortSourceConfigResponse {
    String msg;
    int code;
    String md5;
    SortSourceConfig data;

    @Data
    @Builder
    public static class SortSourceConfig {
        String sortClusterName;
        String sortId;
        Map<String, CacheZone> cacheZones;
    }

    @Data
    @Builder
    public static class CacheZone {
        String zoneName;
        String serviceUrl;
        String authentication;
        List<Topic> topics;
        Map<String, String> cacheZoneProperties;
        String zoneType;
    }

    @Data
    @Builder
    public static class Topic {
        String topic;
        int partitionCnt;
        Map<String, String> topicProperties;
    }
}


