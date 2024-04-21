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

import org.apache.inlong.manager.common.consts.StreamType;
import org.apache.inlong.manager.pojo.sink.paimon.PaimonSink;
import org.apache.inlong.manager.pojo.sort.node.base.ExtractNodeProvider;
import org.apache.inlong.manager.pojo.sort.node.base.LoadNodeProvider;
import org.apache.inlong.manager.pojo.source.paimon.PaimonSource;
import org.apache.inlong.manager.pojo.stream.StreamField;
import org.apache.inlong.manager.pojo.stream.StreamNode;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.constant.PaimonConstant.CatalogType;
import org.apache.inlong.sort.protocol.node.ExtractNode;
import org.apache.inlong.sort.protocol.node.LoadNode;
import org.apache.inlong.sort.protocol.node.extract.PaimonExtractNode;
import org.apache.inlong.sort.protocol.node.load.PaimonLoadNode;
import org.apache.inlong.sort.protocol.transformation.FieldRelation;

import java.util.List;
import java.util.Map;

/**
 * The Provider for creating Paimon extract or load nodes.
 */
public class PaimonProvider implements ExtractNodeProvider, LoadNodeProvider {

    @Override
    public Boolean accept(String streamType) {
        return StreamType.PAIMON.equals(streamType);
    }

    @Override
    public ExtractNode createExtractNode(StreamNode streamNodeInfo) {
        PaimonSource source = (PaimonSource) streamNodeInfo;
        List<FieldInfo> fieldInfos = parseStreamFieldInfos(source.getFieldList(), source.getSourceName());
        Map<String, String> properties = parseProperties(source.getProperties());

        return new PaimonExtractNode(
                source.getSourceName(),
                source.getSourceName(),
                fieldInfos,
                null,
                source.getCatalogUri(),
                source.getWarehouse(),
                source.getDbName(),
                source.getTableName(),
                CatalogType.HIVE,
                source.getCheckIntervalInMinus(),
                source.isReadStreamingSkipCompaction(),
                source.getReadStartCommit(),
                properties,
                source.getExtList());
    }

    @Override
    public LoadNode createLoadNode(StreamNode nodeInfo, Map<String, StreamField> constantFieldMap) {
        PaimonSink paimonSink = (PaimonSink) nodeInfo;
        Map<String, String> properties = parseProperties(paimonSink.getProperties());
        List<FieldInfo> fieldInfos = parseSinkFieldInfos(paimonSink.getSinkFieldList(), paimonSink.getSinkName());
        List<FieldRelation> fieldRelations = parseSinkFields(paimonSink.getSinkFieldList(), constantFieldMap);
        CatalogType catalogType = CatalogType.forName(paimonSink.getCatalogType());

        return new PaimonLoadNode(
                paimonSink.getSinkName(),
                paimonSink.getSinkName(),
                fieldInfos,
                fieldRelations,
                null,
                null,
                null,
                properties,
                paimonSink.getDbName(),
                paimonSink.getTableName(),
                paimonSink.getPrimaryKey(),
                catalogType,
                paimonSink.getCatalogUri(),
                paimonSink.getWarehouse(),
                paimonSink.getExtList(),
                paimonSink.getPartitionKey());
    }
}