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

package org.apache.inlong.audit.service.node;

import org.apache.inlong.audit.service.config.Configuration;
import org.apache.inlong.audit.service.entities.JdbcConfig;
import org.apache.inlong.audit.service.entities.PartitionEntity;
import org.apache.inlong.audit.service.utils.JdbcUtils;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.audit.service.config.ConfigConstants.DEFAULT_AUDIT_DATA_TEMP_STORAGE_DAYS;
import static org.apache.inlong.audit.service.config.ConfigConstants.DEFAULT_CHECK_PARTITION_INTERVAL_HOURS;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_AUDIT_DATA_TEMP_STORAGE_DAYS;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_CHECK_PARTITION_INTERVAL_HOURS;
import static org.apache.inlong.audit.service.config.SqlConstants.DEFAULT_AUDIT_DATA_TEMP_ADD_PARTITION_SQL;
import static org.apache.inlong.audit.service.config.SqlConstants.DEFAULT_AUDIT_DATA_TEMP_DELETE_PARTITION_SQL;
import static org.apache.inlong.audit.service.config.SqlConstants.DEFAULT_TABLE_AUDIT_DATA_DAY_ADD_PARTITION_SQL;
import static org.apache.inlong.audit.service.config.SqlConstants.KEY_AUDIT_DATA_TEMP_ADD_PARTITION_SQL;
import static org.apache.inlong.audit.service.config.SqlConstants.KEY_AUDIT_DATA_TEMP_DELETE_PARTITION_SQL;
import static org.apache.inlong.audit.service.config.SqlConstants.KEY_TABLE_AUDIT_DATA_DAY_ADD_PARTITION_SQL;
import static org.apache.inlong.audit.service.config.SqlConstants.TABLE_AUDIT_DATA_DAY;
import static org.apache.inlong.audit.service.config.SqlConstants.TABLE_AUDIT_DATA_TEMP;

public class PartitionManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionManager.class);
    private static volatile PartitionManager partitionManager = null;
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
    private DataSource dataSource;
    private final PartitionEntity auditDayTable;
    private final PartitionEntity auditTempTable;
    private final Configuration configuration;

    public static PartitionManager getInstance() {
        if (partitionManager == null) {
            synchronized (PartitionManager.class) {
                if (partitionManager == null) {
                    partitionManager = new PartitionManager();
                }
            }
        }
        return partitionManager;
    }

    private PartitionManager() {
        configuration = Configuration.getInstance();
        createDataSource();
        auditDayTable = createAndAddPartition(TABLE_AUDIT_DATA_DAY,
                KEY_TABLE_AUDIT_DATA_DAY_ADD_PARTITION_SQL,
                DEFAULT_TABLE_AUDIT_DATA_DAY_ADD_PARTITION_SQL,
                null,
                null);
        auditTempTable = createAndAddPartition(TABLE_AUDIT_DATA_TEMP,
                KEY_AUDIT_DATA_TEMP_ADD_PARTITION_SQL,
                DEFAULT_AUDIT_DATA_TEMP_ADD_PARTITION_SQL,
                KEY_AUDIT_DATA_TEMP_DELETE_PARTITION_SQL,
                DEFAULT_AUDIT_DATA_TEMP_DELETE_PARTITION_SQL);
    }

    private PartitionEntity createAndAddPartition(String tableName,
            String addPartitionKey,
            String defaultAddPartitionSql,
            String deletePartitionKey,
            String defaultDeletePartitionSql) {
        String addPartitionSql = configuration.get(addPartitionKey, defaultAddPartitionSql);
        String deletePartitionSql =
                deletePartitionKey != null ? configuration.get(deletePartitionKey, defaultDeletePartitionSql) : null;
        PartitionEntity partitionEntity = new PartitionEntity(tableName, addPartitionSql, deletePartitionSql);
        addPartition(partitionEntity, 0);
        return partitionEntity;
    }

    public void start() {
        long intervalHours =
                configuration.get(KEY_CHECK_PARTITION_INTERVAL_HOURS, DEFAULT_CHECK_PARTITION_INTERVAL_HOURS);
        timer.scheduleWithFixedDelay(this::executePartitionManagement, 0, intervalHours, TimeUnit.HOURS);
    }

    private void executePartitionManagement() {
        try {
            managePartition(auditDayTable, false);
            managePartition(auditTempTable, true);
        } catch (Exception e) {
            LOGGER.error("Error occurred while managing partitions", e);
        }
    }

    private void managePartition(PartitionEntity partitionEntity, boolean delete) {
        addPartition(partitionEntity, 1);
        if (delete) {
            long storageDays =
                    configuration.get(KEY_AUDIT_DATA_TEMP_STORAGE_DAYS, DEFAULT_AUDIT_DATA_TEMP_STORAGE_DAYS);
            deletePartition(partitionEntity, storageDays);
        }
    }

    private void addPartition(PartitionEntity partitionEntity, long daysToAdd) {
        String partitionName = partitionEntity.getAddPartitionName(daysToAdd);
        if (isPartitionExist(partitionEntity.getCheckPartitionSql(daysToAdd, false))) {
            LOGGER.info("Partition [{}] of [{}] already exists. Don`t need to add.", partitionName,
                    partitionEntity.getTableName());
            return;
        }
        executeUpdate(partitionEntity.getAddPartitionSql(daysToAdd));
    }

    private void deletePartition(PartitionEntity partitionEntity, long daysToDelete) {
        String partitionName = partitionEntity.getDeletePartitionName(daysToDelete);
        if (!isPartitionExist(partitionEntity.getCheckPartitionSql(daysToDelete, true))) {
            LOGGER.info("Partition [{}] of [{}] does not exist. Don`t need to delete.", partitionName,
                    partitionEntity.getTableName());
            return;
        }
        executeUpdate(partitionEntity.getDeletePartitionSql(daysToDelete));
    }

    private boolean isPartitionExist(String querySql) {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(querySql)) {
            return isPartitionInResultSet(statement);
        } catch (SQLException exception) {
            LOGGER.error("An exception occurred while checking partition [{}]:", querySql, exception);
        }
        return false;
    }

    private boolean isPartitionInResultSet(PreparedStatement statement) {
        try (ResultSet resultSet = statement.executeQuery()) {
            if (resultSet.next()) {
                return resultSet.getInt("count") > 0;
            }
        } catch (SQLException sqlException) {
            LOGGER.error("An error occurred while processing the result set:", sqlException);
        }
        return false;
    }

    private void executeUpdate(String sql) {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.executeUpdate();
            LOGGER.info("Success to manage partition, execute SQL: {}", sql);
        } catch (SQLException e) {
            LOGGER.error("Failed to execute update: {}", sql, e);
        }
    }

    private void createDataSource() {
        JdbcConfig jdbcConfig = JdbcUtils.buildMysqlConfig();
        HikariConfig hikariConfig = JdbcUtils.buildHikariConfig(
                jdbcConfig.getDriverClass(),
                jdbcConfig.getJdbcUrl(),
                jdbcConfig.getUserName(),
                jdbcConfig.getPassword());
        dataSource = new HikariDataSource(hikariConfig);
    }
}
