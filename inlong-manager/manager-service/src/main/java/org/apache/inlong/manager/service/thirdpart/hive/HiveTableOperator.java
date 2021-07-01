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

package org.apache.inlong.manager.service.thirdpart.hive;

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.pojo.datastream.DataStreamInfoToHiveConfig;
import org.apache.inlong.manager.common.pojo.query.ColumnInfoBean;
import org.apache.inlong.manager.common.pojo.query.DatabaseQueryBean;
import org.apache.inlong.manager.common.pojo.query.hive.HiveColumnQueryBean;
import org.apache.inlong.manager.common.pojo.query.hive.HiveTableQueryBean;
import org.apache.inlong.manager.dao.entity.StorageHiveFieldEntity;
import org.apache.inlong.manager.dao.mapper.StorageHiveFieldEntityMapper;
import org.apache.inlong.manager.service.core.DataSourceService;
import org.apache.inlong.manager.service.core.StorageService;
import org.apache.inlong.manager.workflow.exception.WorkflowException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Create hive table operation
 */
@Slf4j
@Component
public class HiveTableOperator {

    @Autowired
    private StorageService storageService;
    @Autowired
    private DataSourceService<DatabaseQueryBean, HiveTableQueryBean> dataSourceService;

    @Autowired
    private StorageHiveFieldEntityMapper storageHiveFieldMapper;

    public void createHiveTable(String bid, DataStreamInfoToHiveConfig hiveConfig) {
        if (log.isDebugEnabled()) {
            log.debug("begin create hive table for business={}, hiveConfig={}", bid, hiveConfig);
        }

        HiveTableQueryBean tableBean = getTableQueryBean(hiveConfig);

        try {
            // create database if not exists
            dataSourceService.createDb(tableBean);

            // check if the table exists
            List<ColumnInfoBean> columns = dataSourceService.queryColumns(tableBean);
            if (columns.size() == 0) {
                // no such table, create one
                dataSourceService.createTable(tableBean);
            } else {
                // set columns, skip the first columns already exist in hive
                List<HiveColumnQueryBean> columnsSkipHistory = tableBean.getColumns().stream()
                        .skip(columns.size()).collect(toList());
                if (columnsSkipHistory.size() != 0) {
                    tableBean.setColumns(columnsSkipHistory);
                    dataSourceService.createColumn(tableBean);
                }
            }
        } catch (Exception e) {
            log.error("create hive table error, ", e);
            storageService.updateHiveStatusById(hiveConfig.getId(),
                    EntityStatus.DATA_STORAGE_CONFIG_FAILURE.getCode(), e.getMessage());
            throw new WorkflowException("create hive table failed, reason: " + e.getMessage());
        }

        log.info("finish create hive table for business {} ", bid);
    }

    protected HiveTableQueryBean getTableQueryBean(DataStreamInfoToHiveConfig hiveConfig) {
        List<StorageHiveFieldEntity> fieldEntities = storageHiveFieldMapper
                .queryHiveFieldsByStreamId(hiveConfig.getBusinessIdentifier(), hiveConfig.getDataStreamIdentifier());
        List<HiveColumnQueryBean> columnQueryBeans = new ArrayList<>();

        for (StorageHiveFieldEntity field : fieldEntities) {
            HiveColumnQueryBean columnBean = new HiveColumnQueryBean();
            columnBean.setColumnName(field.getFieldName());
            columnBean.setColumnType(field.getFieldType());
            columnBean.setColumnDesc(field.getFieldComment());
            columnQueryBeans.add(columnBean);
        }

        HiveTableQueryBean queryBean = new HiveTableQueryBean();
        queryBean.setColumns(columnQueryBeans);
        queryBean.setUsername(hiveConfig.getUserName());
        queryBean.setPassword(hiveConfig.getPassword());
        queryBean.setTableName(hiveConfig.getTableName());
        queryBean.setDbName(hiveConfig.getDbName());
        queryBean.setJdbcUrl(hiveConfig.getJdbcUrl());

        return queryBean;
    }
}
