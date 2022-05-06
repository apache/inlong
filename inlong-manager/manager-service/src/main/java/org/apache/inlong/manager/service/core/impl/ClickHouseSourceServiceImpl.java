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

package org.apache.inlong.manager.service.core.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.query.ColumnInfoBean;
import org.apache.inlong.manager.common.pojo.query.ConnectionInfo;
import org.apache.inlong.manager.common.pojo.query.DatabaseDetail;
import org.apache.inlong.manager.common.pojo.query.DatabaseQueryBean;
import org.apache.inlong.manager.common.pojo.query.ck.ClickHouseTableQueryBean;
import org.apache.inlong.manager.dao.ClickHouseServerDao;
import org.apache.inlong.manager.service.core.DataSourceService;
import org.apache.inlong.manager.service.resource.ck.builder.ClickHouseAddColumnSqlBuilder;
import org.apache.inlong.manager.service.resource.ck.builder.ClickHouseCreateDbSqlBuilder;
import org.apache.inlong.manager.service.resource.ck.builder.ClickHouseCreateTableSqlBuilder;
import org.apache.inlong.manager.service.resource.ck.builder.ClickHouseDropColumnSqlBuilder;
import org.apache.inlong.manager.service.resource.ck.builder.ClickHouseDropDbSqlBuilder;
import org.apache.inlong.manager.service.resource.ck.builder.ClickHouseDropTableSqlBuilder;
import org.apache.inlong.manager.service.resource.ck.builder.ClickHouseModifyColumnSqlBuilder;
import org.apache.inlong.manager.service.resource.ck.builder.ClickHouseQueryTableSqlBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

@Service
public class ClickHouseSourceServiceImpl implements DataSourceService<DatabaseQueryBean, ClickHouseTableQueryBean> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseSourceServiceImpl.class);

    @Autowired
    ClickHouseServerDao clickHouseServerDao;

    @Override
    public boolean testConnection(ConnectionInfo connectionInfo) {
        return false;
    }

    @Override
    public void createDb(ClickHouseTableQueryBean bean) throws Exception {
        ClickHouseCreateDbSqlBuilder builder = new ClickHouseCreateDbSqlBuilder();
        String createDbSql = builder.buildDDL(bean);
        LOGGER.info("create database sql={}", createDbSql);
        clickHouseServerDao.executeDDL(createDbSql, bean.getJdbcUrl(),
                bean.getUsername(), bean.getPassword());
    }

    @Override
    public void dropDb(DatabaseQueryBean queryBean) throws Exception {
        ClickHouseDropDbSqlBuilder builder = new ClickHouseDropDbSqlBuilder();
        String dropDbSql = builder.buildDDL(queryBean);
        LOGGER.info("drop database sql={}", dropDbSql);
        clickHouseServerDao.executeDDL(dropDbSql, queryBean.getJdbcUrl(),
                queryBean.getUserName(), queryBean.getPassword());
    }

    @Override
    public void createTable(ClickHouseTableQueryBean bean) throws Exception {
        ClickHouseCreateTableSqlBuilder builder = new ClickHouseCreateTableSqlBuilder();
        String createTableSql = builder.buildDDL(bean);
        LOGGER.info("create table sql={}", createTableSql);
        clickHouseServerDao.executeDDL(createTableSql, bean.getJdbcUrl(),
                bean.getUsername(), bean.getPassword());
    }

    @Override
    public void dropTable(ClickHouseTableQueryBean queryBean) throws Exception {
        ClickHouseDropTableSqlBuilder builder = new ClickHouseDropTableSqlBuilder();
        String dropTableSql = builder.buildDDL(queryBean);
        LOGGER.info("drop table sql={}", dropTableSql);
        clickHouseServerDao.executeDDL(dropTableSql, queryBean.getJdbcUrl(),
                queryBean.getUsername(), queryBean.getPassword());
    }

    @Override
    public void createColumn(ClickHouseTableQueryBean queryBean) throws Exception {
        if (queryBean == null) {
            LOGGER.warn("add table column failed: change info is null");
            return;
        }
        if (queryBean.getColumns() == null) {
            LOGGER.warn("add table column failed: columns is empty");
            return;
        }
        ClickHouseAddColumnSqlBuilder builder = new ClickHouseAddColumnSqlBuilder();
        String addColumnSql = builder.buildDDL(queryBean);
        LOGGER.info("add table Column sql={}", addColumnSql);
        String[] addColumnInfo = addColumnSql.split("|");
        for (String columnInfo : addColumnInfo) {
            clickHouseServerDao.executeDDL(columnInfo, queryBean.getJdbcUrl(),
                    queryBean.getUsername(), queryBean.getPassword());
        }
    }

    @Override
    public void updateColumn(ClickHouseTableQueryBean queryBean) throws Exception {
        if (queryBean == null) {
            LOGGER.warn("update table column failed: change info is null");
            return;
        }
        if (StringUtils.isBlank(queryBean.getObjectId())) {
            LOGGER.warn("update table column failed: table objectId is null");
        }
        if (queryBean.getColumns() == null) {
            LOGGER.warn("update table column failed: columns is empty");
            return;
        }
        if (queryBean.getColumns().size() > 1) {
            LOGGER.warn("update table column failed: columns number is more than one, only support one column");
        }

        ClickHouseModifyColumnSqlBuilder builder = new ClickHouseModifyColumnSqlBuilder();
        String updateColumnSql = builder.buildDDL(queryBean);
        LOGGER.info("update table Column sql={}", updateColumnSql);
        String[] updateColumnInfo = updateColumnSql.split("|");
        for (String columnInfo : updateColumnInfo) {
            clickHouseServerDao.executeDDL(columnInfo, queryBean.getJdbcUrl(),
                    queryBean.getUsername(), queryBean.getPassword());
        }
    }

    @Override
    public void dropColumn(ClickHouseTableQueryBean queryBean) throws Exception {
        if (queryBean == null) {
            LOGGER.warn("update table column failed: change info is null");
            return;
        }

        ClickHouseDropColumnSqlBuilder builder = new ClickHouseDropColumnSqlBuilder();
        String dropColumnSql = builder.buildDDL(queryBean);
        LOGGER.info("update table Column sql={}", dropColumnSql);
        String[] dropColumnInfo = dropColumnSql.split("|");
        for (String columnInfo : dropColumnInfo) {
            clickHouseServerDao.executeDDL(columnInfo, queryBean.getJdbcUrl(),
                    queryBean.getUsername(), queryBean.getPassword());
        }
    }

    @Override
    public List<ColumnInfoBean> queryColumns(ClickHouseTableQueryBean queryBean) throws Exception {
        String jdbcUrl = queryBean.getJdbcUrl();
        String username = queryBean.getUsername();
        String password = queryBean.getPassword();

        ClickHouseQueryTableSqlBuilder tableBuilder = new ClickHouseQueryTableSqlBuilder();
        String querySql = tableBuilder.buildDDL(queryBean);
        LOGGER.info("query sql={}", querySql);

        if (querySql != null && clickHouseServerDao.isExistTable(jdbcUrl, username, password,
                queryBean.getDbName(), queryBean.getTableName())) {
            return clickHouseServerDao.queryStructure(querySql, jdbcUrl, username, password);
        }
        return Collections.emptyList();
    }

    @Override
    public DatabaseDetail queryDbDetail(ClickHouseTableQueryBean queryBean) throws Exception {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin to query database info for {}", queryBean);
        }

        // TODO if the database not exists, return null
        DatabaseDetail databaseDetail;
        try {
            String jdbcUrl = queryBean.getJdbcUrl();
            String username = queryBean.getUsername();
            String password = queryBean.getPassword();

            List<String> tableNames = clickHouseServerDao.getTables(jdbcUrl, username, password, queryBean.getDbName());
            databaseDetail = new DatabaseDetail(queryBean.getDbName(), tableNames);

            LOGGER.info("success to query db detail");
        } catch (Exception e) {
            LOGGER.error("query db detail error, ", e);
            throw new BusinessException(ErrorCodeEnum.CLICKHOUSE_OPERATION_FAILED);
        }

        LOGGER.info("success to query database");
        return databaseDetail;
    }
}
