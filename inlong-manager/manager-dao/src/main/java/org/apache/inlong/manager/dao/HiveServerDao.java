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

package org.apache.inlong.manager.dao;

import org.apache.commons.lang3.StringUtils;
import org.apache.hive.jdbc.HiveDatabaseMetaData;
import org.apache.inlong.manager.common.pojo.query.ColumnInfoBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@Repository
public class HiveServerDao {

    private static final String HIVE_DRIVER_CLASS = "org.apache.hive.jdbc.HiveDriver";
    private static final Logger LOG = LoggerFactory.getLogger(HiveServerDao.class);

    public void executeDDL(String ddl, String hiveUrl, String user, String password) throws Exception {
        try (Connection conn = this.getHiveConnection(hiveUrl, user, password)) {
            Statement stmt = conn.createStatement();
            stmt.execute(ddl);
        }
    }

    /**
     * Query Hive column structure
     */
    public List<ColumnInfoBean> queryStructure(String querySql, String jdbcUrl,
            String user, String password) throws Exception {
        try (Connection conn = this.getHiveConnection(jdbcUrl, user, password)) {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(querySql);
            List<ColumnInfoBean> columnInfoBeans = new ArrayList<>();
            try {
                while (rs.next()) {
                    ColumnInfoBean columnInfoBean = new ColumnInfoBean();
                    columnInfoBean.setColumnName(rs.getString(1));
                    columnInfoBean.setColumnType(rs.getString(2));
                    columnInfoBean.setColumnDesc(rs.getString(3));
                    columnInfoBeans.add(columnInfoBean);
                }
            } catch (Exception e) {
                LOG.error("query table structure error", e);
            }
            return columnInfoBeans;
        }
    }

    /**
     * Get Hive tables
     */
    public List<String> getTables(String hiveUrl, String user, String password, String dbname) throws Exception {
        List<String> tables = new ArrayList<>();

        try (Connection conn = getHiveConnection(hiveUrl, user, password)) {
            // get DatabaseMetaData
            HiveDatabaseMetaData metaData = (HiveDatabaseMetaData) conn.getMetaData();
            // Get the table in the specified database
            LOG.info("dbname is {}", dbname);
            ResultSet rs = metaData.getTables(dbname, dbname, null, new String[]{"TABLE"});
            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                tables.add(tableName);
            }
        } catch (Exception e) {
            LOG.error("a database access error occurs or this method is called on a closed connection", e);
            throw e;
        }
        return tables;
    }

    /**
     * Get Hive connection from hive url and user
     */
    public Connection getHiveConnection(String hiveUrl, String user, String password) throws Exception {
        if (StringUtils.isBlank(hiveUrl) || !hiveUrl.startsWith("jdbc:hive2")) {
            throw new Exception("Hive server URL was invalid, it should start with jdbc:hive2");
        }
        Connection conn;
        try {
            Class.forName(HIVE_DRIVER_CLASS);
            conn = DriverManager.getConnection(hiveUrl, user, password);
        } catch (Exception e) {
            LOG.error("get hive connection error, please check hive jdbc url, username or password", e);
            throw new Exception("get hive connection error, please check jdbc url, username or password. "
                    + "other error msg: " + e.getMessage());
        }

        if (conn == null) {
            throw new Exception("get hive connection failed, please contact administrator");
        }

        LOG.info("get hive connection success, url={}", hiveUrl);
        return conn;
    }

    public boolean isExistTable(String jdbcUrl, String user, String password, String dbName, String tableName)
            throws Exception {
        List<String> tables = this.getTables(jdbcUrl, user, password, dbName);
        return tables.contains(tableName);
    }

}
