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

package org.apache.inlong.dataproxy.config.pojo;

import org.apache.commons.lang.builder.ToStringBuilder;

import java.util.Map;

public class InLongMetaConfig {

    private String md5;
    private CacheType mqType;
    private Map<String, CacheClusterConfig> clusterConfigMap;
    private Map<String, IdTopicConfig> idTopicConfigMap;

    public InLongMetaConfig() {

    }

    public InLongMetaConfig(String md5, CacheType mqType,
            Map<String, CacheClusterConfig> clusterConfigMap,
            Map<String, IdTopicConfig> idTopicConfigMap) {
        this.md5 = md5;
        this.mqType = mqType;
        this.clusterConfigMap = clusterConfigMap;
        this.idTopicConfigMap = idTopicConfigMap;
    }

    public String getMd5() {
        return md5;
    }

    public CacheType getMqType() {
        return mqType;
    }

    public Map<String, CacheClusterConfig> getClusterConfigMap() {
        return clusterConfigMap;
    }

    public Map<String, IdTopicConfig> getIdTopicConfigMap() {
        return idTopicConfigMap;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("md5", md5)
                .append("mqType", mqType)
                .append("clusterConfigMap", clusterConfigMap)
                .append("idTopicConfigMap", idTopicConfigMap)
                .toString();
    }
}
