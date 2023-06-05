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

package org.apache.inlong.manager.pojo.sort.node.extract;

import org.apache.inlong.manager.common.consts.SourceType;
import org.apache.inlong.manager.pojo.sort.node.ExtractNodeProvider;
import org.apache.inlong.manager.pojo.source.postgresql.PostgreSQLSource;
import org.apache.inlong.manager.pojo.stream.StreamNode;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.node.ExtractNode;
import org.apache.inlong.sort.protocol.node.extract.PostgresExtractNode;

import java.util.List;
import java.util.Map;

/**
 * The Provider for creating PostgreSql extract nodes.
 */
public class PostgreSqlProvider implements ExtractNodeProvider {

    @Override
    public Boolean accept(String sourceType) {
        return SourceType.POSTGRESQL.equals(sourceType);
    }

    @Override
    public ExtractNode createNode(StreamNode streamNodeInfo) {
        PostgreSQLSource postgreSQLSource = (PostgreSQLSource) streamNodeInfo;
        List<FieldInfo> fieldInfos = parseFieldInfos(postgreSQLSource.getFieldList(), postgreSQLSource.getSourceName());
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
}