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

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.InlongMetric;
import org.apache.inlong.sort.protocol.Metadata;
import org.apache.inlong.sort.protocol.constant.TdsqlKafkaConstant;
import org.apache.inlong.sort.protocol.enums.KafkaScanStartupMode;
import org.apache.inlong.sort.protocol.node.format.Format;
import org.apache.inlong.sort.protocol.node.format.ProtobufFormat;
import org.apache.inlong.sort.protocol.transformation.WatermarkField;

/**
 * Kafka extract node for extract data from kafka
 */
@EqualsAndHashCode(callSuper = true)
@JsonTypeName("tdsqlKafkaExtract")
public class TdsqlKafkaExtractNode extends KafkaExtractNode implements InlongMetric, Metadata, Serializable {

    private static final long serialVersionUID = 1L;

    public TdsqlKafkaExtractNode(@JsonProperty("id") String id,
            @JsonProperty("name") String name,
            @JsonProperty("fields") List<FieldInfo> fields,
            @Nullable @JsonProperty("watermarkField") WatermarkField watermarkField,
            @JsonProperty("properties") Map<String, String> properties,
            @Nonnull @JsonProperty("topic") String topic,
            @Nonnull @JsonProperty("bootstrapServers") String bootstrapServers,
            @Nonnull @JsonProperty("format") Format format,
            @JsonProperty("scanStartupMode") KafkaScanStartupMode kafkaScanStartupMode,
            @JsonProperty("primaryKey") String primaryKey,
            @JsonProperty("groupId") String groupId,
            @JsonProperty("scanSpecificOffsets") String scanSpecificOffsets) {
        super(id, name, fields, watermarkField, properties, topic, bootstrapServers, format, kafkaScanStartupMode,
                primaryKey, groupId, scanSpecificOffsets);
    }

    /**
     * generate table options
     *
     * @return options
     */
    @Override
    public Map<String, String> tableOptions() {
        Map<String, String> options = new LinkedHashMap<>();
        if (getProperties() != null && !getProperties().isEmpty()) {
            options.putAll(getProperties());
        }
        options.put(TdsqlKafkaConstant.TOPIC, getTopic());
        options.put(TdsqlKafkaConstant.PROPERTIES_BOOTSTRAP_SERVERS, getBootstrapServers());
        if (getFormat() instanceof ProtobufFormat) {
            options.put(TdsqlKafkaConstant.CONNECTOR, TdsqlKafkaConstant.TDSQL_SUBSCRIBE);
            options.put(TdsqlKafkaConstant.SCAN_STARTUP_MODE, getKafkaScanStartupMode().getValue());
            if (StringUtils.isNotEmpty(getScanSpecificOffsets())) {
                options.put(TdsqlKafkaConstant.SCAN_STARTUP_SPECIFIC_OFFSETS, getScanSpecificOffsets());
            }
            options.putAll(getFormat().generateOptions(false));
        } else {
            throw new IllegalArgumentException("kafka extract node format is IllegalArgument");
        }
        if (StringUtils.isNotEmpty(getGroupId())) {
            options.put(TdsqlKafkaConstant.PROPERTIES_GROUP_ID, getGroupId());
        }
        for (String key : getProperties().keySet()) {
            options.put(key, getProperties().get(key));
        }
        return options;
    }

}
