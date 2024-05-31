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
import org.apache.inlong.manager.common.fieldtype.strategy.FieldTypeMappingStrategy;
import org.apache.inlong.manager.common.fieldtype.strategy.FieldTypeStrategyFactory;
import org.apache.inlong.manager.pojo.sink.postgresql.PostgreSQLSink;
import org.apache.inlong.manager.pojo.sort.node.base.ExtractNodeProvider;
import org.apache.inlong.manager.pojo.sort.node.base.LoadNodeProvider;
import org.apache.inlong.manager.pojo.source.postgresql.PostgreSQLSource;
import org.apache.inlong.manager.pojo.stream.StreamField;
import org.apache.inlong.manager.pojo.stream.StreamNode;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.node.ExtractNode;
import org.apache.inlong.sort.protocol.node.LoadNode;
import org.apache.inlong.sort.protocol.node.extract.PostgresExtractNode;
import org.apache.inlong.sort.protocol.node.load.PostgresLoadNode;
import org.apache.inlong.sort.protocol.transformation.FieldRelation;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * The Provider for creating PostgreSQL extract or load nodes.
 */
@Service
public class PostgreSQLProvider implements ExtractNodeProvider, LoadNodeProvider {

    @Autowired
    private FieldTypeStrategyFactory fieldTypeStrategyFactory;

    @Override
    public Boolean accept(String streamType) {
        return StreamType.POSTGRESQL.equals(streamType);
    }

    @Override
    public ExtractNode createExtractNode(StreamNode streamNodeInfo) {
        PostgreSQLSource postgreSQLSource = (PostgreSQLSource) streamNodeInfo;
        FieldTypeMappingStrategy fieldTypeMappingStrategy =
                fieldTypeStrategyFactory.getInstance(postgreSQLSource.getSourceType());
        List<FieldInfo> fieldInfos = parseStreamFieldInfos(postgreSQLSource.getFieldList(),
                postgreSQLSource.getSourceName(), fieldTypeMappingStrategy);
        Map<String, String> properties = parseProperties(postgreSQLSource.getProperties());

        return new PostgresExtractNode(postgreSQLSource.getSourceName(),
                postgreSQLSource.getSourceName(),
                fieldInfos,
                null,
                properties,
                postgreSQLSource.getPrimaryKey(),
                postgreSQLSource.getTableNameList(),
                postgreSQLSource.getHostname(),
                postgreSQLSource.getUsername(),
                postgreSQLSource.getPassword(),
                postgreSQLSource.getDatabase(),
                postgreSQLSource.getSchema(),
                postgreSQLSource.getPort(),
                postgreSQLSource.getDecodingPluginName(),
                postgreSQLSource.getServerTimeZone(),
                postgreSQLSource.getScanStartupMode());
    }

    @Override
    public LoadNode createLoadNode(StreamNode nodeInfo, Map<String, StreamField> constantFieldMap) {
        PostgreSQLSink postgreSQLSink = (PostgreSQLSink) nodeInfo;
        Map<String, String> properties = parseProperties(postgreSQLSink.getProperties());
        FieldTypeMappingStrategy fieldTypeMappingStrategy =
                fieldTypeStrategyFactory.getInstance(postgreSQLSink.getSinkType());
        List<FieldInfo> fieldInfos = parseSinkFieldInfos(postgreSQLSink.getSinkFieldList(),
                postgreSQLSink.getSinkName(), fieldTypeMappingStrategy);
        List<FieldRelation> fieldRelations = parseSinkFields(postgreSQLSink.getSinkFieldList(), constantFieldMap);

        return new PostgresLoadNode(
                postgreSQLSink.getSinkName(),
                postgreSQLSink.getSinkName(),
                fieldInfos,
                fieldRelations,
                null,
                null,
                null,
                properties,
                postgreSQLSink.getJdbcUrl(),
                postgreSQLSink.getUsername(),
                postgreSQLSink.getPassword(),
                postgreSQLSink.getDbName() + "." + postgreSQLSink.getTableName(),
                postgreSQLSink.getPrimaryKey());
    }
}