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

package org.apache.inlong.manager.service.resource.postgresql;

import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.SinkStatus;
import org.apache.inlong.manager.common.enums.SinkType;
import org.apache.inlong.manager.common.exceptions.WorkflowException;
import org.apache.inlong.manager.common.pojo.sink.SinkInfo;
import org.apache.inlong.manager.common.pojo.sink.postgresql.PostgreSQLColumnInfo;
import org.apache.inlong.manager.common.pojo.sink.postgresql.PostgreSQLSinkDTO;
import org.apache.inlong.manager.common.pojo.sink.postgresql.PostgreSQLTableInfo;
import org.apache.inlong.manager.dao.entity.StreamSinkFieldEntity;
import org.apache.inlong.manager.dao.mapper.StreamSinkFieldEntityMapper;
import org.apache.inlong.manager.service.resource.SinkResourceOperator;
import org.apache.inlong.manager.service.sink.StreamSinkService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * PostgreSQL's resource operator
 */
@Service
public class PostgreSQLResourceOperator implements SinkResourceOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgreSQLResourceOperator.class);

    @Autowired
    private StreamSinkService sinkService;

    @Autowired
    private StreamSinkFieldEntityMapper postgresFieldMapper;

    @Override
    public Boolean accept(SinkType sinkType) {
        return SinkType.POSTGRES == sinkType;
    }

    @Override
    public void createSinkResource(SinkInfo sinkInfo) {
        LOGGER.info("begin to create postgresql resources sinkId={}", sinkInfo.getId());
        if (SinkStatus.CONFIG_SUCCESSFUL.getCode().equals(sinkInfo.getStatus())) {
            LOGGER.warn("postgresql resource [" + sinkInfo.getId() + "] already success, skip to create");
            return;
        } else if (InlongConstants.DISABLE_CREATE_RESOURCE.equals(sinkInfo.getEnableCreateResource())) {
            LOGGER.warn("create resource was disabled, skip to create for [" + sinkInfo.getId() + "]");
            return;
        }
        this.createTable(sinkInfo);
    }

    /**
     * Create PostgreSQL table
     */
    private void createTable(SinkInfo sinkInfo) {
        LOGGER.info("begin to create postgresql table for sinkId={}", sinkInfo.getId());
        List<StreamSinkFieldEntity> fieldList = postgresFieldMapper.selectBySinkId(sinkInfo.getId());
        if (CollectionUtils.isEmpty(fieldList)) {
            LOGGER.warn("no postgresql fields found, skip to create table for sinkId={}", sinkInfo.getId());
        }

        // set columns
        List<PostgreSQLColumnInfo> columnList = new ArrayList<>();
        for (StreamSinkFieldEntity field : fieldList) {
            PostgreSQLColumnInfo columnInfo = new PostgreSQLColumnInfo();
            columnInfo.setName(field.getFieldName());
            columnInfo.setType(field.getFieldType());
            columnInfo.setDesc(field.getFieldComment());
            columnList.add(columnInfo);
        }

        try {
            PostgreSQLSinkDTO pgInfo = PostgreSQLSinkDTO.getFromJson(sinkInfo.getExtParams());
            PostgreSQLTableInfo tableInfo = PostgreSQLSinkDTO.getTableInfo(pgInfo, columnList);
            String url = pgInfo.getJdbcUrl();
            String user = pgInfo.getUsername();
            String password = pgInfo.getPassword();

            String dbName = tableInfo.getDbName();
            String tableName = tableInfo.getTableName();

            // 1. create database if not exists
            PostgreSQLJdbcUtils.createDb(url, user, password, dbName);

            // 2. check if the table exists
            boolean tableExists = PostgreSQLJdbcUtils.checkTablesExist(url, user, password, dbName, tableName);

            // 3. table not exists, create it
            if (!tableExists) {
                PostgreSQLJdbcUtils.createTable(url, user, password, tableInfo);
            } else {
                // 4. table exists, add columns - skip the exists columns
                List<PostgreSQLColumnInfo> existColumns = PostgreSQLJdbcUtils.getColumns(url, user, password,
                        tableName);
                List<String> columnNameList = new ArrayList<>();
                existColumns.forEach(e -> columnNameList.add(e.getName()));
                List<PostgreSQLColumnInfo> needAddColumns = tableInfo.getColumns().stream()
                        .filter((pgcInfo) -> !columnNameList.contains(pgcInfo.getName())).collect(Collectors.toList());
                if (CollectionUtils.isNotEmpty(needAddColumns)) {
                    PostgreSQLJdbcUtils.addColumns(url, user, password, tableName, needAddColumns);
                }
            }

            // 5. update the sink status to success
            String info = "success to create postgresql resource";
            sinkService.updateStatus(sinkInfo.getId(), SinkStatus.CONFIG_SUCCESSFUL.getCode(), info);
            LOGGER.info(info + " for sinkInfo={}", sinkInfo);
        } catch (Throwable e) {
            String errMsg = "create postgresql table failed: " + e.getMessage();
            LOGGER.error(errMsg, e);
            sinkService.updateStatus(sinkInfo.getId(), SinkStatus.CONFIG_FAILED.getCode(), errMsg);
            throw new WorkflowException(errMsg);
        }

        LOGGER.info("success create postgresql table for data sink [" + sinkInfo.getId() + "]");
    }

}
