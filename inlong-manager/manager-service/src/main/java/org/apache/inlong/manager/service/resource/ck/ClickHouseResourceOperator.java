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

package org.apache.inlong.manager.service.resource.ck;

import org.apache.inlong.manager.common.enums.GlobalConstants;
import org.apache.inlong.manager.common.enums.SinkStatus;
import org.apache.inlong.manager.common.enums.SinkType;
import org.apache.inlong.manager.common.exceptions.WorkflowException;
import org.apache.inlong.manager.common.pojo.query.ColumnInfoBean;
import org.apache.inlong.manager.common.pojo.query.DatabaseQueryBean;
import org.apache.inlong.manager.common.pojo.query.ck.ClickHouseColumnQueryBean;
import org.apache.inlong.manager.common.pojo.query.ck.ClickHouseTableQueryBean;
import org.apache.inlong.manager.common.pojo.sink.SinkInfo;
import org.apache.inlong.manager.common.pojo.sink.ck.ClickHouseSinkDTO;
import org.apache.inlong.manager.dao.entity.StreamSinkFieldEntity;
import org.apache.inlong.manager.dao.mapper.StreamSinkFieldEntityMapper;
import org.apache.inlong.manager.service.core.DataSourceService;
import org.apache.inlong.manager.service.resource.SinkResourceOperator;
import org.apache.inlong.manager.service.sink.StreamSinkService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.toList;

@Service
public class ClickHouseResourceOperator implements SinkResourceOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseResourceOperator.class);

    @Autowired
    private StreamSinkService sinkService;
    @Autowired
    private StreamSinkFieldEntityMapper clickHouseFieldMapper;
    @Autowired
    @Qualifier("clickHouseSourceServiceImpl")
    private DataSourceService<DatabaseQueryBean, ClickHouseTableQueryBean> dataSourceService;

    @Override
    public Boolean accept(SinkType sinkType) {
        return SinkType.CLICKHOUSE == sinkType;
    }

    /**
     * Create hive table according to the groupId and hive config
     */
    public void createSinkResource(String groupId, SinkInfo sinkInfo) {
        if (sinkInfo == null) {
            LOGGER.warn("sink info was null, skip to create resource");
            return;
        }

        if (SinkStatus.CONFIG_SUCCESSFUL.getCode().equals(sinkInfo.getStatus())) {
            LOGGER.warn("sink resource [" + sinkInfo.getId() + "] already success, skip to create");
            return;
        } else if (GlobalConstants.DISABLE_CREATE_RESOURCE.equals(sinkInfo.getEnableCreateResource())) {
            LOGGER.warn("create resource was disabled, skip to create for [" + sinkInfo.getId() + "]");
            return;
        }

        this.createTable(groupId, sinkInfo);
    }

    private void createTable(String groupId, SinkInfo config) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin create hive table for inlong group={}, config={}", groupId, config);
        }

        // Get all info from config
        ClickHouseSinkDTO clickHouseInfo = ClickHouseSinkDTO.getFromJson(config.getExtParams());
        ClickHouseTableQueryBean tableBean = getTableQueryBean(config, clickHouseInfo);
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
                List<ClickHouseColumnQueryBean> columnsSkipHistory = tableBean.getColumns().stream()
                        .skip(columns.size()).collect(toList());
                if (columnsSkipHistory.size() != 0) {
                    tableBean.setColumns(columnsSkipHistory);
                    dataSourceService.createColumn(tableBean);
                }
            }
            sinkService.updateStatus(config.getId(),
                    SinkStatus.CONFIG_SUCCESSFUL.getCode(), "create hive table success");
        } catch (Throwable e) {
            LOGGER.error("create hive table error, ", e);
            sinkService.updateStatus(config.getId(), SinkStatus.CONFIG_FAILED.getCode(), e.getMessage());
            throw new WorkflowException("create hive table failed, reason: " + e.getMessage());
        }

        LOGGER.info("success create hive table for data group [" + groupId + "]");
    }

    protected ClickHouseTableQueryBean getTableQueryBean(SinkInfo config, ClickHouseSinkDTO clickHouseInfo) {
        String groupId = config.getInlongGroupId();
        String streamId = config.getInlongStreamId();
        LOGGER.info("begin to get table query bean for groupId={}, streamId={}", groupId, streamId);

        List<StreamSinkFieldEntity> fieldEntities = clickHouseFieldMapper.selectFields(groupId, streamId);

        List<ClickHouseColumnQueryBean> columnQueryBeans = new ArrayList<>();
        for (StreamSinkFieldEntity field : fieldEntities) {
            ClickHouseColumnQueryBean columnBean = new ClickHouseColumnQueryBean();
            columnBean.setColumnName(field.getFieldName());
            columnBean.setColumnType(field.getFieldType());
            columnBean.setColumnDesc(field.getFieldComment());
            columnQueryBeans.add(columnBean);
        }

        ClickHouseTableQueryBean queryBean = new ClickHouseTableQueryBean();
        queryBean.setColumns(columnQueryBeans);
        if (clickHouseInfo.getTableEngine() != null) {
            queryBean.setTableEngine(clickHouseInfo.getTableEngine());
        }
        queryBean.setUsername(clickHouseInfo.getUsername());
        queryBean.setPassword(clickHouseInfo.getPassword());
        queryBean.setTableName(clickHouseInfo.getTableName());
        queryBean.setDbName(clickHouseInfo.getDbName());
        queryBean.setJdbcUrl(clickHouseInfo.getJdbcUrl());

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("success to get table query bean={}", queryBean);
        }
        return queryBean;
    }
}
