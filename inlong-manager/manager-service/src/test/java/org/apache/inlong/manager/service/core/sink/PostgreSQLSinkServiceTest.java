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

package org.apache.inlong.manager.service.core.sink;

import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.SinkType;
import org.apache.inlong.manager.common.pojo.sink.SinkRequest;
import org.apache.inlong.manager.common.pojo.sink.StreamSink;
import org.apache.inlong.manager.common.pojo.sink.postgresql.PostgreSQLColumnInfo;
import org.apache.inlong.manager.common.pojo.sink.postgresql.PostgreSQLSink;
import org.apache.inlong.manager.common.pojo.sink.postgresql.PostgreSQLSinkRequest;
import org.apache.inlong.manager.common.pojo.sink.postgresql.PostgreSQLTableInfo;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.apache.inlong.manager.service.core.impl.InlongStreamServiceTest;
import org.apache.inlong.manager.service.resource.postgresql.PostgreSQLJdbcUtils;
import org.apache.inlong.manager.service.sink.StreamSinkService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * PostgreSQL sink service test
 */
public class PostgreSQLSinkServiceTest extends ServiceBaseTest {

    private static final String globalGroupId = "b_group1";
    private static final String globalStreamId = "stream1";
    private static final String globalOperator = "admin";

    @Autowired
    private StreamSinkService sinkService;
    @Autowired
    private InlongStreamServiceTest streamServiceTest;

    /**
     * Save sink info.
     */
    public Integer saveSink(String sinkName) {
        streamServiceTest.saveInlongStream(globalGroupId, globalStreamId, globalOperator);
        PostgreSQLSinkRequest sinkInfo = new PostgreSQLSinkRequest();
        sinkInfo.setInlongGroupId(globalGroupId);
        sinkInfo.setInlongStreamId(globalStreamId);
        sinkInfo.setSinkType(SinkType.SINK_POSTGRES);

        sinkInfo.setJdbcUrl("jdbc:postgresql://localhost:5432/postgre");
        sinkInfo.setUsername("postgresql");
        sinkInfo.setPassword("inlong");
        sinkInfo.setDbName("public");
        sinkInfo.setTableName("user");
        sinkInfo.setPrimaryKey("name,age");

        sinkInfo.setSinkName(sinkName);
        sinkInfo.setEnableCreateResource(InlongConstants.DISABLE_CREATE_RESOURCE);
        return sinkService.save(sinkInfo, globalOperator);
    }

    /**
     * Delete sink info by sink id.
     */
    public void deleteSink(Integer sinkId) {
        boolean result = sinkService.delete(sinkId, globalOperator);
        Assertions.assertTrue(result);
    }

    @Test
    public void testListByIdentifier() {
        Integer sinkId = this.saveSink("postgresql_default1");
        StreamSink sink = sinkService.get(sinkId);
        Assertions.assertEquals(globalGroupId, sink.getInlongGroupId());
        deleteSink(sinkId);
    }

    @Test
    public void testGetAndUpdate() {
        Integer sinkId = this.saveSink("postgresql_default2");
        StreamSink streamSink = sinkService.get(sinkId);
        Assertions.assertEquals(globalGroupId, streamSink.getInlongGroupId());

        PostgreSQLSink sink = (PostgreSQLSink) streamSink;
        sink.setEnableCreateResource(InlongConstants.ENABLE_CREATE_RESOURCE);
        SinkRequest request = sink.genSinkRequest();
        boolean result = sinkService.update(request, globalOperator);
        Assertions.assertTrue(result);

        deleteSink(sinkId);
    }

    /**
     * Just using in local test
     */
    @Disabled
    public void testDbResource() {
        String url = "jdbc:postgresql://localhost:5432/test";
        String user = "postgresql";
        String password = "123456";
        String dbName = "test";
        String tableName = "test_123";

        try {
            PostgreSQLJdbcUtils.createDb(url, user, password, dbName);
            List<PostgreSQLColumnInfo> columnInfoList = new ArrayList<>();
            PostgreSQLColumnInfo info = new PostgreSQLColumnInfo();
            info.setType("integer");
            info.setName("id");
            columnInfoList.add(info);
            PostgreSQLColumnInfo info2 = new PostgreSQLColumnInfo();
            info2.setType("integer");
            info2.setName("age");
            columnInfoList.add(info2);

            PostgreSQLColumnInfo info3 = new PostgreSQLColumnInfo();
            info3.setType("integer");
            info3.setName("high");
            columnInfoList.add(info3);

            PostgreSQLTableInfo tableInfo = new PostgreSQLTableInfo();
            tableInfo.setDbName(dbName);
            tableInfo.setColumns(columnInfoList);
            tableInfo.setTableName(tableName);

            boolean tableExists = PostgreSQLJdbcUtils.checkTablesExist(url, user, password, dbName, tableName);
            if (!tableExists) {
                PostgreSQLJdbcUtils.createTable(url, user, password, tableInfo);
            } else {
                List<PostgreSQLColumnInfo> existColumns = PostgreSQLJdbcUtils.getColumns(url, user, password,
                        tableName);
                List<String> columnNameList = new ArrayList<>();
                existColumns.forEach(columnInfo -> columnNameList.add(columnInfo.getName()));

                List<PostgreSQLColumnInfo> needAddColumns = tableInfo.getColumns().stream()
                        .filter((columnInfo) -> !columnNameList.contains(columnInfo.getName()))
                        .collect(Collectors.toList());
                if (CollectionUtils.isNotEmpty(needAddColumns)) {
                    PostgreSQLJdbcUtils.addColumns(url, user, password, tableName, needAddColumns);
                }
            }
        } catch (Exception e) {
            // print to local console
            e.printStackTrace();
        }
    }

}
