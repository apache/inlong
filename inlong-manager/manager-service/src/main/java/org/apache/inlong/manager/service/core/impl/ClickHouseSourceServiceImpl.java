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

import org.apache.inlong.manager.common.pojo.query.ColumnInfoBean;
import org.apache.inlong.manager.common.pojo.query.ConnectionInfo;
import org.apache.inlong.manager.common.pojo.query.DatabaseDetail;
import org.apache.inlong.manager.common.pojo.query.DatabaseQueryBean;
import org.apache.inlong.manager.common.pojo.query.ck.ClickHouseTableQueryBean;
import org.apache.inlong.manager.dao.ClickHouseServerDao;
import org.apache.inlong.manager.service.core.DataSourceService;
import org.apache.inlong.manager.service.resource.ck.builder.ClickHouseCreateDbSqlBuilder;
import org.apache.inlong.manager.service.resource.ck.builder.ClickHouseCreateTableSqlBuilder;
import org.apache.inlong.manager.service.resource.ck.builder.ClickHouseQueryTableSqlBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

@Service
public class ClickHouseSourceServiceImpl implements DataSourceService<DatabaseQueryBean, ClickHouseTableQueryBean> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HiveSourceServiceImpl.class);

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
        clickHouseServerDao.executeDDL(createDbSql, bean.getJdbcUrl(), bean.getUsername(), bean.getPassword());
    }

    @Override
    public void dropDb(DatabaseQueryBean queryBean) throws Exception {

    }

    @Override
    public void createTable(ClickHouseTableQueryBean bean) throws Exception {
        ClickHouseCreateTableSqlBuilder builder = new ClickHouseCreateTableSqlBuilder();
        String createTableSql = builder.buildDDL(bean);
        LOGGER.info("create table sql={}", createTableSql);
        clickHouseServerDao.executeDDL(createTableSql, bean.getJdbcUrl(), bean.getUsername(), bean.getPassword());
    }

    @Override
    public void dropTable(ClickHouseTableQueryBean queryBean) throws Exception {

    }

    @Override
    public void createColumn(ClickHouseTableQueryBean queryBean) throws Exception {

    }

    @Override
    public void updateColumn(ClickHouseTableQueryBean queryBean) throws Exception {

    }

    @Override
    public void dropColumn(ClickHouseTableQueryBean queryBean) throws Exception {

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
        return null;
    }
}
