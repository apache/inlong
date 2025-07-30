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

package org.apache.inlong.sort.protocol.node.extract;

import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.enums.ClickHouseScanStartupMode;
import org.apache.inlong.sort.protocol.node.ExtractNode;
import org.apache.inlong.sort.protocol.transformation.WatermarkField;

import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude.Include;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * ClickHouse extract node for extracting data from ClickHouse database.
 */
@EqualsAndHashCode(callSuper = true)
@JsonTypeName("clickHouseExtract")
@JsonInclude(Include.NON_NULL)
@Data
public class ClickHouseExtractNode extends ExtractNode implements Serializable {

    private static final long serialVersionUID = 1L;

    @Nonnull
    @JsonProperty("url")
    private String url;

    @Nonnull
    @JsonProperty("tableName")
    private String tableName;

    @Nullable
    @JsonProperty("username")
    private String username;

    @Nullable
    @JsonProperty("password")
    private String password;

    @Nullable
    @JsonProperty("scanStartupMode")
    private ClickHouseScanStartupMode scanStartupMode;

    @Nullable
    @JsonProperty("timestampField")
    private String timestampField;

    @Nullable
    @JsonProperty("timestamp")
    private String timestamp;

    @JsonCreator
    public ClickHouseExtractNode(
            @JsonProperty("id") String id,
            @JsonProperty("name") String name,
            @JsonProperty("fields") List<FieldInfo> fields,
            @Nullable @JsonProperty("watermarkField") WatermarkField watermarkField,
            @Nullable @JsonProperty("properties") Map<String, String> properties,
            @Nonnull @JsonProperty("url") String url,
            @Nonnull @JsonProperty("tableName") String tableName,
            @Nullable @JsonProperty("username") String username,
            @Nullable @JsonProperty("password") String password,
            @Nullable @JsonProperty("scanStartupMode") ClickHouseScanStartupMode scanStartupMode,
            @Nullable @JsonProperty("timestampField") String timestampField,
            @Nullable @JsonProperty("timestamp") String timestamp) {
        super(id, name, fields, watermarkField, properties);
        this.url = Preconditions.checkNotNull(url, "url is null");
        this.tableName = Preconditions.checkNotNull(tableName, "tableName is null");
        this.username = username;
        this.password = password;
        this.scanStartupMode = scanStartupMode;
        this.timestampField = timestampField;
        this.timestamp = timestamp;

        if (scanStartupMode == ClickHouseScanStartupMode.TIMESTAMP) {
            Preconditions.checkNotNull(timestampField, "timestampField must be set when scanStartupMode is TIMESTAMP");
            Preconditions.checkNotNull(timestamp, "timestamp must be set when scanStartupMode is TIMESTAMP");
        }
    }

    @Override
    public String genTableName() {
        return tableName;
    }
}
