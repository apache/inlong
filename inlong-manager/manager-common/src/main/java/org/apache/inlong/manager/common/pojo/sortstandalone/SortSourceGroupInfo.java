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

package org.apache.inlong.manager.common.pojo.sortstandalone;

import com.google.gson.Gson;
import lombok.Data;
import org.apache.pulsar.shade.org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Data
public class SortSourceGroupInfo {
    private static final Logger LOGGER = LoggerFactory.getLogger(SortSourceGroupInfo.class);
    private static final String KEY_BACK_UP_CLUSTER_TAG = "back_up_cluster_tag";
    private static final String KEY_BACK_UP_TOPIC = "back_up_topic";

    private static final long serialVersionUID = 1L;
    String groupId;
    String clusterTag;
    String topic;
    String extParams;
    String mqType;
    Map<String, String> extParamsMap = new ConcurrentHashMap<>();

    public Map<String, String> getExtParamsMap() {
        if (extParamsMap.isEmpty() && StringUtils.isNotBlank(extParams)) {
            try {
                Gson gson = new Gson();
                extParamsMap = gson.fromJson(extParams, Map.class);
            } catch (Throwable t) {
                LOGGER.error("fail to parse group ext params", t);
            }
        }
        return extParamsMap;
    }

    public String getBackUpClusterTag() {
        return getExtParamsMap().get(KEY_BACK_UP_CLUSTER_TAG);
    }

    public String getBackUpTopic() {
        return getExtParamsMap().get(KEY_BACK_UP_TOPIC);
    }
}
