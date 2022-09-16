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

package org.apache.inlong.sort.protocol.node.load;

import javax.annotation.Nullable;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.InlongMetric;
import org.apache.inlong.sort.protocol.enums.FilterStrategy;
import org.apache.inlong.sort.protocol.node.LoadNode;
import org.apache.inlong.sort.protocol.transformation.FieldRelation;
import org.apache.inlong.sort.protocol.transformation.FilterFunction;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.apache.inlong.sort.protocol.transformation.WatermarkField;

/**
 * influxDB load node can load data into InfluxDB.
 */
@EqualsAndHashCode(callSuper = true)
@JsonTypeName("influxdbLoadNode")
@Data
@NoArgsConstructor
public class InfluxDBLoadNode extends LoadNode implements InlongMetric, Serializable {

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

    public InfluxDBLoadNode(
            @JsonProperty("id") String id,
            @JsonProperty("name") String name,
            @JsonProperty("fields") List<FieldInfo> fields,
            @JsonProperty("fieldRelations") List<FieldRelation> fieldRelations,
            @JsonProperty("filters") List<FilterFunction> filters,
            @JsonProperty("filterStrategy") FilterStrategy filterStrategy,
            @JsonProperty("sinkParallelism") Integer sinkParallelism,
            @Nullable @JsonProperty("watermarkField") WatermarkField waterMarkField,
            @JsonProperty("properties") Map<String, String> properties,
            @JsonProperty("bucketName") String bucketName,
            @JsonProperty("influxDBToken") String influxDBToken,
            @JsonProperty("hostname") String hostname,
            @JsonProperty("username") String username,
            @JsonProperty("password") String password,
            @JsonProperty("influxDBUrl") String influxDBUrl,
            @JsonProperty("organizationName") String organizationName) {
        super(id, name, fields, fieldRelations, filters, filterStrategy, sinkParallelism, properties);
        this.influxDBUrl = influxDBUrl;
        this.bucketName = bucketName;
        this.influxDBPassword = password;
        this.influxDBToken = influxDBToken;
        this.influxDBUsername = username;
        this.organizationName = organizationName;
    }

    @Override
    public Map<String, String> tableOptions() {
        Map<String, String> options = super.tableOptions();
        options.put("connector", "jdbc-inlong");
        options.put("influxDBUrl", influxDBUrl);
        options.put("influxDBUsername", influxDBUsername);
        options.put("influxDBPassword", influxDBPassword);
        options.put("influxDBToken", influxDBToken);
        options.put("bucketName", bucketName);
        options.put("organizationName", organizationName);
        return options;
    }

    @Override
    public String genTableName() {
        return String.format("table_%s", super.getId());
    }

}
