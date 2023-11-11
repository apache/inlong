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

import org.apache.inlong.common.enums.MetaField;
import org.apache.inlong.manager.common.consts.StreamType;
import org.apache.inlong.manager.pojo.sink.SinkField;
import org.apache.inlong.manager.pojo.sink.iceberg.IcebergSink;
import org.apache.inlong.manager.pojo.sort.node.base.ExtractNodeProvider;
import org.apache.inlong.manager.pojo.sort.node.base.LoadNodeProvider;
import org.apache.inlong.manager.pojo.source.iceberg.IcebergSource;
import org.apache.inlong.manager.pojo.stream.StreamField;
import org.apache.inlong.manager.pojo.stream.StreamNode;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.MetaFieldInfo;
import org.apache.inlong.sort.protocol.constant.IcebergConstant;
import org.apache.inlong.sort.protocol.constant.IcebergConstant.CatalogType;
import org.apache.inlong.sort.protocol.node.ExtractNode;
import org.apache.inlong.sort.protocol.node.LoadNode;
import org.apache.inlong.sort.protocol.node.extract.IcebergExtractNode;
import org.apache.inlong.sort.protocol.node.load.IcebergLoadNode;
import org.apache.inlong.sort.protocol.transformation.FieldRelation;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The Provider for creating Iceberg load nodes.
 */
@Slf4j
public class IcebergProvider implements ExtractNodeProvider, LoadNodeProvider {

    @Override
    public Boolean accept(String sinkType) {
        return StreamType.ICEBERG.equals(sinkType);
    }

    @Override
    public ExtractNode createExtractNode(StreamNode streamNodeInfo) {
        IcebergSource icebergSource = (IcebergSource) streamNodeInfo;
        List<FieldInfo> fieldInfos = parseStreamFieldInfos(icebergSource.getFieldList(), icebergSource.getSourceName());
        Map<String, String> properties = parseProperties(icebergSource.getProperties());

        return new IcebergExtractNode(icebergSource.getSourceName(),
                icebergSource.getSourceName(),
                fieldInfos,
                null,
                icebergSource.getUri(),
                icebergSource.getWarehouse(),
                icebergSource.getDatabase(),
                icebergSource.getTableName(),
                CatalogType.HIVE,
                "HIVE",
                icebergSource.getPrimaryKey(),
                null,
                properties);
    }

    @Override
    public LoadNode createLoadNode(StreamNode nodeInfo, Map<String, StreamField> constantFieldMap) {
        IcebergSink icebergSink = (IcebergSink) nodeInfo;
        Map<String, String> properties = parseProperties(icebergSink.getProperties());
        List<FieldInfo> fieldInfos = parseSinkFieldInfos(icebergSink.getSinkFieldList(), icebergSink.getSinkName());
        List<FieldRelation> fieldRelations = parseSinkFields(icebergSink.getSinkFieldList(), constantFieldMap);
        IcebergConstant.CatalogType catalogType = CatalogType.forName(icebergSink.getCatalogType());

        return new IcebergLoadNode(
                icebergSink.getSinkName(),
                icebergSink.getSinkName(),
                fieldInfos,
                fieldRelations,
                null,
                null,
                null,
                properties,
                icebergSink.getDbName(),
                icebergSink.getTableName(),
                icebergSink.getPrimaryKey(),
                catalogType,
                icebergSink.getCatalogUri(),
                icebergSink.getWarehouse());
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
    public List<SinkField> addSinkMetaFields(List<SinkField> sinkFields) {
        List<String> fieldNames = sinkFields.stream().map(SinkField::getFieldName).collect(Collectors.toList());
        if (!fieldNames.contains(MetaField.AUDIT_DATA_TIME.name())) {
            sinkFields.add(0, new SinkField(0, "long", MetaField.AUDIT_DATA_TIME.name(), "iceberg meta field",
                    MetaField.AUDIT_DATA_TIME.name(), "long", 1, MetaField.AUDIT_DATA_TIME.name(), null));
        }
        return sinkFields;
    }

    @Override
    public List<FieldInfo> getMetaFields() {
        List<FieldInfo> fieldInfos = new ArrayList<>();
        fieldInfos.add(0, new MetaFieldInfo(MetaField.AUDIT_DATA_TIME.name(), MetaField.AUDIT_DATA_TIME));
        return fieldInfos;
    }
}