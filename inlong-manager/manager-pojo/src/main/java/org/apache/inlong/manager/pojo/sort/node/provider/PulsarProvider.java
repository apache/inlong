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

package org.apache.inlong.manager.pojo.sort.node.provider;

import org.apache.inlong.common.enums.MessageWrapType;
import org.apache.inlong.common.enums.MetaField;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.LongFormatInfo;
import org.apache.inlong.manager.common.consts.SourceType;
import org.apache.inlong.manager.pojo.sort.node.base.ExtractNodeProvider;
import org.apache.inlong.manager.pojo.source.pulsar.PulsarSource;
import org.apache.inlong.manager.pojo.stream.StreamField;
import org.apache.inlong.manager.pojo.stream.StreamNode;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.enums.PulsarScanStartupMode;
import org.apache.inlong.sort.protocol.node.ExtractNode;
import org.apache.inlong.sort.protocol.node.extract.PulsarExtractNode;
import org.apache.inlong.sort.protocol.node.format.Format;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * The Provider for creating Pulsar extract nodes.
 */
@Service
public class PulsarProvider implements ExtractNodeProvider {

    @Override
    public Boolean accept(String sourceType) {
        return SourceType.PULSAR.equals(sourceType);
    }

    @Override
    public ExtractNode createExtractNode(StreamNode streamNodeInfo) {
        PulsarSource pulsarSource = (PulsarSource) streamNodeInfo;
        List<FieldInfo> fieldInfos = parseStreamFieldInfos(pulsarSource.getFieldList(), pulsarSource.getSourceName());
        Map<String, String> properties = parseProperties(pulsarSource.getProperties());

        String fullTopicName =
                pulsarSource.getPulsarTenant() + "/" + pulsarSource.getNamespace() + "/" + pulsarSource.getTopic();
        Format format = parsingFormat(pulsarSource.getSerializationType(),
                pulsarSource.getWrapType(),
                pulsarSource.getDataSeparator(),
                pulsarSource.getKvSeparator(),
                pulsarSource.getDataEscapeChar(),
                pulsarSource.getIgnoreParseError());

        String startupMode =
                StringUtils.isNotBlank(pulsarSource.getScanStartupMode()) ? PulsarScanStartupMode.forName(
                        pulsarSource.getScanStartupMode()).getValue() : null;
        final String primaryKey = pulsarSource.getPrimaryKey();
        final String serviceUrl = pulsarSource.getServiceUrl();
        final String adminUrl = pulsarSource.getAdminUrl();
        final String scanStartupSubStartOffset =
                StringUtils.isNotBlank(pulsarSource.getSubscription()) ? PulsarScanStartupMode.EARLIEST.getValue()
                        : null;

        return new PulsarExtractNode(pulsarSource.getSourceName(),
                pulsarSource.getSourceName(),
                fieldInfos,
                null,
                properties,
                fullTopicName,
                adminUrl,
                serviceUrl,
                format,
                startupMode,
                primaryKey,
                pulsarSource.getSubscription(),
                scanStartupSubStartOffset,
                pulsarSource.getClientAuthPluginClassName(),
                pulsarSource.getClientAuthParams());
    }

    @Override
    public List<StreamField> addStreamMetaFields(List<StreamField> streamFields) {
        List<String> fieldNames = streamFields.stream().map(StreamField::getFieldName).collect(Collectors.toList());
        if (!fieldNames.contains(MetaField.AUDIT_DATA_TIME.name())) {
            streamFields.add(0,
                    new StreamField(0, "long", MetaField.AUDIT_DATA_TIME.name(), "data_time", null, 1,
                            MetaField.AUDIT_DATA_TIME.name()));
        }
        return streamFields;
    }

    @Override
    public List<FieldInfo> getMetaFields() {
        List<FieldInfo> fieldInfos = new ArrayList<>();
        fieldInfos.add(0, new FieldInfo(MetaField.AUDIT_DATA_TIME.name(), new LongFormatInfo()));
        return fieldInfos;
    }

    @Override
    public boolean needInlongPropertiesField(StreamNode streamNode) {
        if (streamNode instanceof PulsarSource) {
            PulsarSource pulsarSource = (PulsarSource) streamNode;
            return !Objects.equals(pulsarSource.getWrapType(), MessageWrapType.RAW.getName());
        }
        return true;
    }

    @Override
    public List<StreamField> addInlongPropertiesFieldForStream(List<StreamField> streamFields) {
        List<String> fieldNames = streamFields.stream().map(StreamField::getFieldName).collect(Collectors.toList());
        if (!fieldNames.contains(MetaField.INLONG_PROPERTIES.name())) {
            streamFields.add(0,
                    new StreamField(0, "map", MetaField.INLONG_PROPERTIES.name(), "inlong properties", null, 1,
                            MetaField.INLONG_PROPERTIES.name()));
        }
        return streamFields;
    }
}