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

package org.apache.inlong.sort.protocol.node;

import org.apache.inlong.common.bounded.Boundaries;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.transformation.WatermarkField;

import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude.Include;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/**
 * extract node extracts data from external system
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = MySqlExtractNode.class, name = "mysqlExtract"),
        @JsonSubTypes.Type(value = KafkaExtractNode.class, name = "kafkaExtract"),
        @JsonSubTypes.Type(value = PostgresExtractNode.class, name = "postgresExtract"),
        @JsonSubTypes.Type(value = FileSystemExtractNode.class, name = "fileSystemExtract"),
        @JsonSubTypes.Type(value = MongoExtractNode.class, name = "mongoExtract"),
        @JsonSubTypes.Type(value = SqlServerExtractNode.class, name = "sqlserverExtract"),
        @JsonSubTypes.Type(value = OracleExtractNode.class, name = "oracleExtract"),
        @JsonSubTypes.Type(value = TubeMQExtractNode.class, name = "tubeMQExtract"),
        @JsonSubTypes.Type(value = PulsarExtractNode.class, name = "pulsarExtract"),
        @JsonSubTypes.Type(value = RedisExtractNode.class, name = "redisExtract"),
        @JsonSubTypes.Type(value = DorisExtractNode.class, name = "dorisExtract"),
        @JsonSubTypes.Type(value = HudiExtractNode.class, name = "hudiExtract"),
        @JsonSubTypes.Type(value = IcebergExtractNode.class, name = "icebergExtract"),
        @JsonSubTypes.Type(value = OceanBaseExtractNode.class, name = "oceanbaseExtract"),
        @JsonSubTypes.Type(value = ClickHouseExtractNode.class, name = "clickHouseExtract"),
})
@Data
@NoArgsConstructor
public abstract class ExtractNode implements Node {

    public static final String INLONG_MSG = "inlong-msg";

    public static final String INLONG_MSG_AUDIT_TIME = "value.data-time";

    public static final String INLONG_MSG_PROPERTIES = "value.inlong-msg-properties";

    public static final String CONSUME_AUDIT_TIME = "consume_time";

    @JsonProperty("id")
    private String id;
    @JsonInclude(Include.NON_NULL)
    @JsonProperty("name")
    private String name;
    @JsonProperty("fields")
    private List<FieldInfo> fields;
    @Nullable
    @JsonProperty("watermarkField")
    @JsonInclude(Include.NON_NULL)
    private WatermarkField watermarkField;
    @Nullable
    @JsonInclude(Include.NON_NULL)
    @JsonProperty("properties")
    private Map<String, String> properties;

    @JsonCreator
    public ExtractNode(@JsonProperty("id") String id,
            @JsonProperty("name") String name,
            @JsonProperty("fields") List<FieldInfo> fields,
            @Nullable @JsonProperty("watermark_field") WatermarkField watermarkField,
            @Nullable @JsonProperty("properties") Map<String, String> properties) {
        this.id = Preconditions.checkNotNull(id, "id is null");
        this.name = name;
        this.fields = Preconditions.checkNotNull(fields, "fields is null");
        Preconditions.checkState(!fields.isEmpty(), "fields is empty");
        this.watermarkField = watermarkField;
        this.properties = properties;
    }

    public void fillInBoundaries(Boundaries boundaries) {
        Preconditions.checkNotNull(boundaries, "boundaries is null");
        // every single kind of extract node should provide the way to fill in boundaries individually
    }

}
