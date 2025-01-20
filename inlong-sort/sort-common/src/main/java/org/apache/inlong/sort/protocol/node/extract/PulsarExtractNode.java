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

import org.apache.inlong.common.bounded.Boundaries;
import org.apache.inlong.common.bounded.BoundaryType;
import org.apache.inlong.common.enums.MetaField;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.InlongMetric;
import org.apache.inlong.sort.protocol.Metadata;
import org.apache.inlong.sort.protocol.node.ExtractNode;
import org.apache.inlong.sort.protocol.node.format.Format;
import org.apache.inlong.sort.protocol.node.format.InLongMsgFormat;
import org.apache.inlong.sort.protocol.transformation.WatermarkField;

import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@EqualsAndHashCode(callSuper = true)
@JsonTypeName("pulsarExtract")
@Data
public class PulsarExtractNode extends ExtractNode implements InlongMetric, Metadata {

    private static final Logger log = LoggerFactory.getLogger(PulsarExtractNode.class);
    private static final long serialVersionUID = 1L;

    @Nonnull
    @JsonProperty("topic")
    private String topic;
    @JsonProperty("adminUrl")
    private String adminUrl;
    @Nonnull
    @JsonProperty("serviceUrl")
    private String serviceUrl;
    @Nonnull
    @JsonProperty("format")
    private Format format;

    @JsonProperty("scanStartupMode")
    private String scanStartupMode;

    @JsonProperty("primaryKey")
    private String primaryKey;

    @JsonProperty("scanStartupSubName")
    private String scanStartupSubName;

    @JsonProperty("scanStartupSubStartOffset")
    private String scanStartupSubStartOffset;

    /**
     * pulsar client auth plugin class name
     * e.g. org.apache.pulsar.client.impl.auth.AuthenticationToken
     */
    @JsonProperty("clientAuthPluginClassName")
    private String clientAuthPluginClassName;

    /**
     * pulsar client auth params
     * e.g. token:{tokenString}
     * the tokenString should be compatible with the clientAuthPluginClassName see also in:
     * <a href="https://pulsar.apache.org/docs/next/security-jwt/"> pulsar auth </a>
     */
    @JsonProperty("clientAuthParams")
    private String clientAuthParams;

    Map<String, String> sourceBoundaryOptions = new HashMap<>();

    @JsonCreator
    public PulsarExtractNode(@JsonProperty("id") String id,
            @JsonProperty("name") String name,
            @JsonProperty("fields") List<FieldInfo> fields,
            @Nullable @JsonProperty("watermarkField") WatermarkField watermarkField,
            @JsonProperty("properties") Map<String, String> properties,
            @Nonnull @JsonProperty("topic") String topic,
            @JsonProperty("adminUrl") String adminUrl,
            @Nonnull @JsonProperty("serviceUrl") String serviceUrl,
            @Nonnull @JsonProperty("format") Format format,
            @Nonnull @JsonProperty("scanStartupMode") String scanStartupMode,
            @JsonProperty("primaryKey") String primaryKey,
            @JsonProperty("scanStartupSubName") String scanStartupSubName,
            @JsonProperty("scanStartupSubStartOffset") String scanStartupSubStartOffset,
            @JsonProperty("clientAuthPluginClassName") String clientAuthPluginClassName,
            @JsonProperty("clientAuthParams") String clientAuthParams) {

        super(id, name, fields, watermarkField, properties);
        this.topic = Preconditions.checkNotNull(topic, "pulsar topic is null.");
        this.serviceUrl = Preconditions.checkNotNull(serviceUrl, "pulsar serviceUrl is null.");
        this.format = Preconditions.checkNotNull(format, "pulsar format is null.");
        this.scanStartupMode = Preconditions.checkNotNull(scanStartupMode,
                "pulsar scanStartupMode is null.");
        this.adminUrl = adminUrl;
        this.primaryKey = primaryKey;
        this.scanStartupSubName = scanStartupSubName;
        this.scanStartupSubStartOffset = scanStartupSubStartOffset;
        this.clientAuthPluginClassName = clientAuthPluginClassName;
        this.clientAuthParams = clientAuthParams;

    }

    /**
     * generate table options
     *
     * @return options
     */
    @Override
    public Map<String, String> tableOptions() {
        Map<String, String> options = super.tableOptions();
        if (StringUtils.isBlank(this.primaryKey)) {
            options.put("connector", "pulsar-inlong");
            options.putAll(format.generateOptions(false));
        } else {
            options.put("connector", "upsert-pulsar-inlong");
            options.putAll(format.generateOptions(true));
        }
        if (StringUtils.isNotBlank(adminUrl)) {
            options.put("admin-url", adminUrl);
        }
        options.put("service-url", serviceUrl);
        options.put("topic", topic);
        options.put("scan.startup.mode", scanStartupMode);
        if (StringUtils.isNotBlank(scanStartupSubName)) {
            options.put("scan.startup.sub-name", scanStartupSubName);
            options.put("scan.startup.sub-start-offset", scanStartupSubStartOffset);
        }

        if (StringUtils.isNotBlank(clientAuthPluginClassName)
                && StringUtils.isNotBlank(clientAuthParams)) {
            options.put("pulsar.client.authPluginClassName", clientAuthPluginClassName);
            options.put("pulsar.client.authParams", clientAuthParams);
        }

        // add boundary options
        if (!sourceBoundaryOptions.isEmpty()) {
            options.putAll(sourceBoundaryOptions);
        }
        return options;
    }

    @Override
    public String genTableName() {
        return String.format("table_%s", super.getId());
    }

    @Override
    public String getPrimaryKey() {
        return primaryKey;
    }

    @Override
    public List<FieldInfo> getPartitionFields() {
        return super.getPartitionFields();
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
            case INLONG_PROPERTIES:
                metadataKey = INLONG_MSG_PROPERTIES;
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
        return EnumSet.of(MetaField.AUDIT_DATA_TIME, MetaField.INLONG_PROPERTIES);
    }

    @Override
    public void fillInBoundaries(Boundaries boundaries) {
        super.fillInBoundaries(boundaries);
        BoundaryType boundaryType = boundaries.getBoundaryType();
        String lowerBoundary = boundaries.getLowerBound();
        String upperBoundary = boundaries.getUpperBound();
        if (Objects.requireNonNull(boundaryType) == BoundaryType.TIME) {
            sourceBoundaryOptions.put("source.start.publish-time", lowerBoundary);
            sourceBoundaryOptions.put("source.stop.at-publish-time", upperBoundary);
            log.info("Filled in source boundaries options");
        } else {
            log.warn("Not supported boundary type: {}", boundaryType);
        }
    }
}
