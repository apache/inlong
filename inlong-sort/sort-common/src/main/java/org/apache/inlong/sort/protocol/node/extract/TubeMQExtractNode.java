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

import org.apache.inlong.common.enums.MetaField;
import org.apache.inlong.sort.formats.util.StringUtils;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.InlongMetric;
import org.apache.inlong.sort.protocol.Metadata;
import org.apache.inlong.sort.protocol.constant.TubeMQConstant;
import org.apache.inlong.sort.protocol.node.ExtractNode;
import org.apache.inlong.sort.protocol.node.format.Format;
import org.apache.inlong.sort.protocol.node.format.InLongMsgFormat;
import org.apache.inlong.sort.protocol.transformation.WatermarkField;

import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * TubeMQ extract node for extracting data from Tube.
 */
@EqualsAndHashCode(callSuper = true)
@JsonTypeName("tubeMQExtract")
@Data
public class TubeMQExtractNode extends ExtractNode implements Serializable, InlongMetric, Metadata {

    private static final long serialVersionUID = -2544747886429528474L;

    @Nonnull
    @JsonProperty("masterRpc")
    private String masterRpc;

    @Nonnull
    @JsonProperty("topic")
    private String topic;

    @Nonnull
    @JsonProperty("format")
    private Format format;

    @Nonnull
    @JsonProperty("consumeGroup")
    private String consumeGroup;

    @JsonProperty("sessionKey")
    private String sessionKey;

    /**
     * The tubemq consumers use this streamId set to filter records reading from server.
     */
    @JsonProperty("streamId")
    private TreeSet<String> streamId;

    @JsonCreator
    public TubeMQExtractNode(
            @JsonProperty("id") String id,
            @JsonProperty("name") String name,
            @JsonProperty("fields") List<FieldInfo> fields,
            @Nullable @JsonProperty("watermarkField") WatermarkField waterMarkField,
            @JsonProperty("properties") Map<String, String> properties,
            @Nonnull @JsonProperty("masterRpc") String masterRpc,
            @Nonnull @JsonProperty("topic") String topic,
            @Nonnull @JsonProperty("format") Format format,
            @Nonnull @JsonProperty("consumeGroup") String consumeGroup,
            @JsonProperty("sessionKey") String sessionKey,
            @JsonProperty("streamId") TreeSet<String> streamId) {
        super(id, name, fields, waterMarkField, properties);
        this.masterRpc = Preconditions.checkNotNull(masterRpc, "TubeMQ masterRpc is null");
        this.topic = Preconditions.checkNotNull(topic, "TubeMQ topic is null");
        this.format = Preconditions.checkNotNull(format, "Format is null");
        this.consumeGroup = Preconditions.checkNotNull(consumeGroup, "Group id is null");
        this.sessionKey = sessionKey;
        this.streamId = streamId;
    }

    @Override
    public Map<String, String> tableOptions() {
        Map<String, String> map = super.tableOptions();
        map.put(TubeMQConstant.CONNECTOR, TubeMQConstant.TUBEMQ);
        map.putAll(format.generateOptions(false));
        map.put(TubeMQConstant.TOPIC, topic);
        map.put(TubeMQConstant.MASTER_RPC, masterRpc);
        map.put(TubeMQConstant.CONSUME_GROUP, consumeGroup);
        map.put(TubeMQConstant.SESSION_KEY, sessionKey);

        if (null != streamId && !streamId.isEmpty()) {
            map.put(TubeMQConstant.STREAMID, StringUtils.concatCsv(streamId.toArray(new String[0]),
                    ',', null, null));
        }

        return map;
    }

    @Override
    public String genTableName() {
        return String.format("table_%s", super.getId());
    }

    @Override
    public String getMetadataKey(MetaField metaField) {
        String metadataKey;
        switch (metaField) {
            case AUDIT_DATA_TIME:
                if (format instanceof InLongMsgFormat) {
                    metadataKey = INLONG_MSG_AUDIT_TIME;
                } else {
                    metadataKey = CONSUME_AUDIT_TIME;
                }
                break;
            default:
                throw new UnsupportedOperationException(String.format("Unsupported meta field for %s: %s",
                        this.getClass().getSimpleName(), metaField));
        }
        return metadataKey;
    }

    @Override
    public boolean isVirtual(MetaField metaField) {
        return true;
    }

    @Override
    public Set<MetaField> supportedMetaFields() {
        return EnumSet.of(MetaField.AUDIT_DATA_TIME);
    }

}
