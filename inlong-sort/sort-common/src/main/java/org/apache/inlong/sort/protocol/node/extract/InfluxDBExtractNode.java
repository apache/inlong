/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.protocol.node.extract;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.inlong.common.enums.MetaField;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.InlongMetric;
import org.apache.inlong.sort.protocol.Metadata;
import org.apache.inlong.sort.protocol.node.ExtractNode;
import org.apache.inlong.sort.protocol.transformation.WatermarkField;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Extract node for influxDB, note that InfluxDB should work in replicaSet mode
 */
@EqualsAndHashCode(callSuper = true)
@JsonTypeName("influxDBExtract")
@Data
public class InfluxDBExtractNode extends ExtractNode implements InlongMetric, Metadata, Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("influxDBUrl")
    private String influxDBUrl;

    @JsonProperty("influxDBUsername")
    private String influxDBUsername;

    @JsonProperty("influxDBPassword")
    private String influxDBPassword;

    @JsonProperty("influxDBToken")
    private String influxDBToken;

    @JsonProperty("bucketName")
    private String bucketName;

    @JsonProperty("organizationName")
    private String organizationName;


    public InfluxDBExtractNode(@JsonProperty("id") String id,
                               @JsonProperty("name") String name,
                               @JsonProperty("fields") List<FieldInfo> fields,
                               @Nullable @JsonProperty("watermarkField") WatermarkField waterMarkField,
                               @JsonProperty("properties") Map<String, String> properties,
                               @JsonProperty("bucketName") String bucketName,
                               @JsonProperty("influxDBToken") String influxDBToken,
                               @JsonProperty("hostname") String hostname,
                               @JsonProperty("username") String username,
                               @JsonProperty("password") String password,
                               @JsonProperty("influxDBUrl") String influxDBUrl,
                               @JsonProperty("organizationName") String organizationName) {
        super(id, name, fields, waterMarkField, properties);
        this.influxDBUrl = influxDBUrl;
        this.bucketName = bucketName;
        this.influxDBPassword = password;
        this.influxDBToken = influxDBToken;
        this.influxDBUsername = username;
        this.organizationName = organizationName;
    }

    @Override
    public boolean isVirtual(MetaField metaField) {
        return true;
    }

    @Override
    public Set<MetaField> supportedMetaFields() {
        return EnumSet.of(MetaField.PROCESS_TIME, MetaField.COLLECTION_NAME, MetaField.DATABASE_NAME, MetaField.OP_TS);
    }

    @Override
    public String genTableName() {
        return String.format("table_%s", super.getId());
    }

    @Override
    public Map<String, String> tableOptions() {
        Map<String, String> options = super.tableOptions();
        options.put("connector", "influx-cdc-inlong");
        options.put("influxDBUrl", influxDBUrl);
        options.put("influxDBUsername", influxDBUsername);
        options.put("influxDBPassword", influxDBPassword);
        options.put("influxDBToken", influxDBToken);
        options.put("bucketName", bucketName);
        options.put("organizationName", organizationName);
        return options;
    }
}
