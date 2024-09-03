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
import org.apache.inlong.manager.common.consts.SourceType;
import org.apache.inlong.manager.common.fieldtype.strategy.FieldTypeMappingStrategy;
import org.apache.inlong.manager.common.fieldtype.strategy.FieldTypeStrategyFactory;
import org.apache.inlong.manager.pojo.sort.node.base.ExtractNodeProvider;
import org.apache.inlong.manager.pojo.source.mysql.MySQLBinlogSource;
import org.apache.inlong.manager.pojo.stream.StreamField;
import org.apache.inlong.manager.pojo.stream.StreamNode;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.MetaFieldInfo;
import org.apache.inlong.sort.protocol.node.ExtractNode;
import org.apache.inlong.sort.protocol.node.extract.MySqlExtractNode;

import com.google.common.base.Splitter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The Provider for creating MySQLBinlog extract nodes.
 */
@Service
public class MySQLBinlogProvider implements ExtractNodeProvider {

    @Autowired
    private FieldTypeStrategyFactory fieldTypeStrategyFactory;

    @Override
    public Boolean accept(String sourceType) {
        return SourceType.MYSQL_BINLOG.equals(sourceType);
    }

    @Override
    public ExtractNode createExtractNode(StreamNode streamNodeInfo) {
        MySQLBinlogSource binlogSource = (MySQLBinlogSource) streamNodeInfo;
        FieldTypeMappingStrategy fieldTypeMappingStrategy =
                fieldTypeStrategyFactory.getInstance(binlogSource.getSourceType());
        List<FieldInfo> fieldInfos = parseStreamFieldInfos(binlogSource.getFieldList(), binlogSource.getSourceName(),
                fieldTypeMappingStrategy);
        Map<String, String> properties = parseProperties(binlogSource.getProperties());

        final String database = binlogSource.getDatabaseWhiteList();
        final String primaryKey = binlogSource.getPrimaryKey();
        final String hostName = binlogSource.getHostname();
        final String username = binlogSource.getUser();
        final String password = binlogSource.getPassword();
        final Integer port = binlogSource.getPort();
        Integer serverId = null;
        if (binlogSource.getServerId() != null && binlogSource.getServerId() > 0) {
            serverId = binlogSource.getServerId();
        }
        String tables = binlogSource.getTableWhiteList();
        final List<String> tableNames = Splitter.on(",").splitToList(tables);
        final String serverTimeZone = binlogSource.getServerTimezone();

        // TODO Needs to be configurable for those parameters
        if (binlogSource.isAllMigration()) {
            // Unique properties when migrate all tables in database
            properties.put("migrate-all", "true");
        }
        if (binlogSource.isOnlyIncremental()) {
            properties.put("scan.startup.mode", "latest-offset");
        }

        return new MySqlExtractNode(binlogSource.getSourceName(),
                binlogSource.getSourceName(),
                fieldInfos,
                null,
                properties,
                primaryKey,
                tableNames,
                hostName,
                username,
                password,
                database,
                port,
                serverId,
                true,
                serverTimeZone);
    }

    @Override
    public List<FieldInfo> getMetaFields() {
        List<FieldInfo> fieldInfos = new ArrayList<>();
        fieldInfos.add(0, new MetaFieldInfo(MetaField.AUDIT_DATA_TIME.name(), MetaField.AUDIT_DATA_TIME));
        return fieldInfos;
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

}