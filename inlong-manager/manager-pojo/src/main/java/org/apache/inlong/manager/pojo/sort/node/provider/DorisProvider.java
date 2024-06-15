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
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.pojo.sink.SinkField;
import org.apache.inlong.manager.pojo.sink.doris.DorisSink;
import org.apache.inlong.manager.pojo.sort.node.base.LoadNodeProvider;
import org.apache.inlong.manager.pojo.stream.StreamField;
import org.apache.inlong.manager.pojo.stream.StreamNode;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.node.LoadNode;
import org.apache.inlong.sort.protocol.node.format.Format;
import org.apache.inlong.sort.protocol.node.load.DorisLoadNode;
import org.apache.inlong.sort.protocol.transformation.FieldRelation;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The Provider for creating Doris load nodes.
 */
@Slf4j
@Service
public class DorisProvider implements LoadNodeProvider {

    @Override
    public Boolean accept(String sinkType) {
        return SinkType.DORIS.equals(sinkType);
    }

    @Override
    public LoadNode createLoadNode(StreamNode nodeInfo, Map<String, StreamField> constantFieldMap) {
        DorisSink dorisSink = (DorisSink) nodeInfo;
        Map<String, String> properties = parseProperties(dorisSink.getProperties());
        List<FieldInfo> fieldInfos = parseSinkFieldInfos(dorisSink.getSinkFieldList(), dorisSink.getSinkName());
        List<FieldRelation> fieldRelations = parseSinkFields(dorisSink.getSinkFieldList(), constantFieldMap);
        Format format = parsingSinkMultipleFormat(dorisSink.getSinkMultipleEnable(), dorisSink.getSinkMultipleFormat());

        return new DorisLoadNode(
                dorisSink.getSinkName(),
                dorisSink.getSinkName(),
                fieldInfos,
                fieldRelations,
                null,
                null,
                null,
                properties,
                dorisSink.getFeNodes(),
                dorisSink.getUsername(),
                dorisSink.getPassword(),
                dorisSink.getTableIdentifier(),
                null,
                dorisSink.getSinkMultipleEnable(),
                format,
                dorisSink.getDatabasePattern(),
                dorisSink.getTablePattern());
    }

    @Override
    public Boolean isSinkMultiple(StreamNode nodeInfo) {
        DorisSink dorisSink = (DorisSink) nodeInfo;
        return dorisSink.getSinkMultipleEnable();
    }

    @Override
    public List<StreamField> addStreamFieldsForSinkMultiple(List<StreamField> streamFields) {
        if (CollectionUtils.isEmpty(streamFields)) {
            streamFields = new ArrayList<>();
        }
        streamFields.add(0,
                new StreamField(0, "varbinary", MetaField.DATA_BYTES_CANAL.name(), "meta.data_canal", null, 1,
                        MetaField.DATA_BYTES_CANAL.name()));
        return streamFields;
    }

    @Override
    public List<SinkField> addSinkFieldsForSinkMultiple(List<SinkField> sinkFields) {
        if (CollectionUtils.isEmpty(sinkFields)) {
            sinkFields = new ArrayList<>();
        }
        sinkFields.add(0, new SinkField(0, "varbinary", MetaField.DATA_BYTES_CANAL.name(), "meta.data_canal",
                MetaField.DATA_BYTES_CANAL.name(), "varbinary", 0, MetaField.DATA_BYTES_CANAL.name(), null));
        return sinkFields;
    }

}