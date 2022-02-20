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

import java.sql.Connection;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.query.ColumnInfoBean;
import org.apache.inlong.manager.common.pojo.query.ConnectionInfo;
import org.apache.inlong.manager.common.pojo.query.DatabaseDetail;
import org.apache.inlong.manager.common.pojo.query.DatabaseQueryBean;
import org.apache.inlong.manager.common.pojo.query.hive.HiveTableQueryBean;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.HiveServerDao;
import org.apache.inlong.manager.service.core.DataSourceService;
import org.apache.inlong.manager.service.thirdparty.hive.builder.HiveAddColumnSqlBuilder;
import org.apache.inlong.manager.service.thirdparty.hive.builder.HiveChangeColumnBuilder;
import org.apache.inlong.manager.service.thirdparty.hive.builder.HiveCreateDbSqlBuilder;
import org.apache.inlong.manager.service.thirdparty.hive.builder.HiveDropDbSqlBuilder;
import org.apache.inlong.manager.service.thirdparty.hive.builder.HiveDropTableSqlBuilder;
import org.apache.inlong.manager.service.thirdparty.hive.builder.HiveQueryTableSqlBuilder;
import org.apache.inlong.manager.service.thirdparty.hive.builder.HiveTableCreateSqlBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class HiveSourceServiceImpl implements DataSourceService<DatabaseQueryBean, HiveTableQueryBean> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HiveSourceServiceImpl.class);

    @Autowired
    HiveServerDao hiveServerDao;

    @Override
    public boolean testConnection(ConnectionInfo connectionInfo) {
        LOGGER.debug("begin test connection, connection info: {}", connectionInfo);
        Preconditions.checkNotNull(connectionInfo, "Connection info cannot be empty");
        Preconditions.checkNotNull(connectionInfo.getJdbcUrl(), "JDBC URL cannot be empty");

        try (Connection conn = hiveServerDao.getHiveConnection(connectionInfo.getJdbcUrl(),
                connectionInfo.getUsername(), connectionInfo.getPassword())) {

            LOGGER.info("test connection success");
            return conn != null;
        } catch (Exception e) {
            LOGGER.error("test connection error: {}", e.getMessage());
            return false;
        }
    }

    @Override
    public void createDb(HiveTableQueryBean bean) throws Exception {
        HiveCreateDbSqlBuilder builder = new HiveCreateDbSqlBuilder();
        String createDbSql = builder.buildDDL(bean);
        LOGGER.info("create database sql={}", createDbSql);
        hiveServerDao.executeDDL(createDbSql, bean.getJdbcUrl(), bean.getUsername(), bean.getPassword());
    }

    @Override
    public void dropDb(DatabaseQueryBean bean) throws Exception {
        HiveDropDbSqlBuilder builder = new HiveDropDbSqlBuilder();
        String dropDbSql = builder.buildDDL(bean);
        LOGGER.info("drop database sql={}", dropDbSql);
        hiveServerDao.executeDDL(dropDbSql, bean.getJdbcUrl(), bean.getUserName(), bean.getPassword());
    }

    @Override
    public void createTable(HiveTableQueryBean bean) throws Exception {
        HiveTableCreateSqlBuilder builder = new HiveTableCreateSqlBuilder();
        String createTableSql = builder.buildDDL(bean);
        LOGGER.info("create table sql={}", createTableSql);
        hiveServerDao.executeDDL(createTableSql, bean.getJdbcUrl(), bean.getUsername(), bean.getPassword());
    }

    @Override
    public void dropTable(HiveTableQueryBean bean) throws Exception {
        HiveDropTableSqlBuilder builder = new HiveDropTableSqlBuilder();
        String dropTableSql = builder.buildDDL(bean);
        LOGGER.info("drop table sql={}", dropTableSql);
        hiveServerDao.executeDDL(dropTableSql, bean.getJdbcUrl(),
                bean.getUsername(), bean.getPassword());
    }

    @Override
    public void createColumn(HiveTableQueryBean bean) throws Exception {
        if (bean == null) {
            LOGGER.warn("add table column failed: change info is null");
            return;
        }
        if (bean.getColumns() == null) {
            LOGGER.warn("add table column failed: columns is empty");
            return;
        }
        HiveAddColumnSqlBuilder builder = new HiveAddColumnSqlBuilder();
        String addColumnSql = builder.buildDDL(bean);
        LOGGER.info("add table Column sql={}", addColumnSql);
        hiveServerDao.executeDDL(addColumnSql, bean.getJdbcUrl(),
                bean.getUsername(), bean.getPassword());
    }

    @Override
    public void updateColumn(HiveTableQueryBean tableQueryBean) throws Exception {
        if (tableQueryBean == null) {
            LOGGER.warn("update table column failed: change info is null");
            return;
        }
        if (StringUtils.isBlank(tableQueryBean.getObjectId())) {
            LOGGER.warn("update table column failed: table objectId is null");
        }
        if (tableQueryBean.getColumns() == null) {
            LOGGER.warn("update table column failed: columns is empty");
            return;
        }
        if (tableQueryBean.getColumns().size() > 1) {
            LOGGER.warn("update table column failed: columns number is more than one, only support one column");
        }

        HiveChangeColumnBuilder builder = new HiveChangeColumnBuilder();
        String updateColumnSql = builder.buildDDL(tableQueryBean);
        LOGGER.info("update table Column sql={}", updateColumnSql);
        hiveServerDao.executeDDL(updateColumnSql, tableQueryBean.getJdbcUrl(),
                tableQueryBean.getUsername(), tableQueryBean.getPassword());
    }

    @Override
    public void dropColumn(HiveTableQueryBean queryBean) {
        // todo
    }

    @Override
    public List<ColumnInfoBean> queryColumns(HiveTableQueryBean queryBean) throws Exception {
        String jdbcUrl = queryBean.getJdbcUrl();
        String username = queryBean.getUsername();
        String password = queryBean.getPassword();

        HiveQueryTableSqlBuilder tableBuilder = new HiveQueryTableSqlBuilder();
        String querySql = tableBuilder.buildDDL(queryBean);
        LOGGER.info("query sql={}", querySql);

        if (querySql != null && hiveServerDao.isExistTable(jdbcUrl, username, password,
                queryBean.getDbName(), queryBean.getTableName())) {
            return hiveServerDao.queryStructure(querySql, jdbcUrl, username, password);
        }
        return Collections.emptyList();
    }

    @Override
    public DatabaseDetail queryDbDetail(HiveTableQueryBean queryBean) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin to query database info for {}", queryBean);
        }

        // TODO if the database not exists, return null
        DatabaseDetail databaseDetail;
        try {
            String jdbcUrl = queryBean.getJdbcUrl();
            String username = queryBean.getUsername();
            String password = queryBean.getPassword();

            // Query tables in DB
            List<String> tableNames = hiveServerDao.getTables(jdbcUrl, username, password, queryBean.getDbName());
            databaseDetail = new DatabaseDetail(queryBean.getDbName(), tableNames);

            LOGGER.info("success to query db detail");
        } catch (Exception e) {
            LOGGER.error("query db detail error, ", e);
            throw new BusinessException(ErrorCodeEnum.HIVE_OPERATION_FAILED);
        }

        LOGGER.info("success to query database");
        return databaseDetail;
    }
}
