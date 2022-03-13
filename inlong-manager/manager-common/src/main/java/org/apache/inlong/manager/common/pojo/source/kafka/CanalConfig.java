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

package org.apache.inlong.manager.common.pojo.source.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import lombok.Data;
import org.apache.inlong.manager.common.util.Preconditions;

import java.util.Map;

@Data
public class CanalConfig {

    @JsonProperty("database")
    private String database;

    @JsonProperty("table")
    private String table;

    @JsonProperty("timestamp_format_standard")
    private String timestampFormatStandard = "SQL";

    @JsonProperty("include_metadata")
    private boolean includeMetadata = true;

    @JsonProperty("ignore_parse_errors")
    private boolean ignoreParseErrors = false;

    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("database", this.database);
        map.put("table", this.table);
        map.put("ignore_parse_errors", this.ignoreParseErrors);
        map.put("timestamp_format_standard", this.timestampFormatStandard);
        map.put("include_metadata", this.includeMetadata);
        return map;
    }

    public static CanalConfig forCanalConfig(Map<String, Object> properties) {
        Preconditions.checkNotEmpty(properties, "Canal Config Properties should not be empty");
        CanalConfig canalConfig = new CanalConfig();
        Preconditions.checkNotNull(properties.get("database"), "database should not be empty");
        canalConfig.setDatabase(properties.get("database").toString());
        Preconditions.checkNotNull(properties.get("table"), "table should not be empty");
        canalConfig.setTable(properties.get("table").toString());
        Preconditions.checkNotNull(properties.get("timestamp_format_standard"),
                "timestamp_format_standard should not be empty");
        canalConfig.setTimestampFormatStandard(properties.get("timestamp_format_standard").toString());
        if (properties.get("ignore_parse_errors") == null) {
            canalConfig.setIgnoreParseErrors(false);
        } else {
            canalConfig.setIgnoreParseErrors(Boolean.parseBoolean(properties.get("ignore_parse_errors").toString()));
        }
        if (properties.get("include_metadata") == null) {
            canalConfig.setIncludeMetadata(true);
        } else {
            canalConfig.setIncludeMetadata(Boolean.parseBoolean(properties.get("include_metadata").toString()));
        }
        return canalConfig;
    }
}
