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

package org.apache.inlong.manager.pojo.sort.standalone;

import org.apache.inlong.manager.common.consts.InlongConstants;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Data
public class SortSourceClusterInfo {

    private static final Logger LOGGER = LoggerFactory.getLogger(SortSourceClusterInfo.class);
    private static final Gson GSON = new Gson();
    private static final Splitter.MapSplitter MAP_SPLITTER = Splitter.on("&").trimResults()
            .withKeyValueSeparator("=");
    private static final String KEY_IS_CONSUMABLE = "consumer";

    private static final long serialVersionUID = 1L;
    String name;
    String type;
    String url;
    String clusterTags;
    String extTag;
    String extParams;
    Set<String> clusterTagsSet;
    Map<String, String> extTagMap = new ConcurrentHashMap<>();
    Map<String, String> extParamsMap = new ConcurrentHashMap<>();

    public Set<String> getClusterTagsSet() {
        if (CollectionUtils.isEmpty(clusterTagsSet) && StringUtils.isNotBlank(clusterTags)) {
            clusterTagsSet = ImmutableSet.copyOf(clusterTags.split(InlongConstants.COMMA));
        }
        return clusterTagsSet;
    }

    public Map<String, String> getExtParamsMap() {
        if (extParamsMap.isEmpty() && extParams != null) {
            try {
                JsonObject jo = GSON.fromJson(extParams, JsonObject.class);
                extParamsMap = new HashMap<>();
                jo.keySet().forEach(k -> {
                    JsonElement element = jo.get(k);
                    if (element.isJsonPrimitive()) {
                        extParamsMap.put(k, element.getAsString());
                    } else if (!element.isJsonNull()) {
                        extParamsMap.put(k, element.toString());
                    }
                });
            } catch (Throwable t) {
                LOGGER.error("fail to parse cluster ext params", t);
            }
        }
        return extParamsMap;
    }

    public Map<String, String> getExtTagMap() {
        if (extTagMap.isEmpty() && StringUtils.isNotBlank(extTag)) {
            try {
                extTagMap = MAP_SPLITTER.split(extTag);
            } catch (Throwable t) {
                LOGGER.error("fail to parse cluster ext tag params", t);
            }
        }
        return extTagMap;
    }

    public boolean isConsumable() {
        String isConsumable = this.getExtTagMap().get(KEY_IS_CONSUMABLE);
        return isConsumable == null || "true".equalsIgnoreCase(isConsumable);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof SortSourceClusterInfo)) {
            return false;
        }
        SortSourceClusterInfo other = (SortSourceClusterInfo) o;
        return Objects.equals(this.name, other.name)
                && Objects.equals(this.clusterTags, other.clusterTags)
                && Objects.equals(this.type, other.type)
                && Objects.equals(this.extParams, other.extParams)
                && Objects.equals(this.extTag, other.extTag);

    }
}
