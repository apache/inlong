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

package org.apache.inlong.sort.flink.hive.partition;

import com.google.common.base.Preconditions;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Partition commit policy to create partitions in hive table.
 */
public class JdbcHivePartitionCommitPolicy implements PartitionCommitPolicy {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcHivePartitionCommitPolicy.class);

    private static final String driverClass = "org.apache.hive.jdbc.HiveDriver";

    private final Configuration configuration;

    private final HiveSinkInfo hiveSinkInfo;

    private final Connection connection;

    public JdbcHivePartitionCommitPolicy(
            Configuration configuration,
            HiveSinkInfo hiveSinkInfo) throws SQLException, ClassNotFoundException {
        this.configuration = Preconditions.checkNotNull(configuration);
        this.hiveSinkInfo = Preconditions.checkNotNull(hiveSinkInfo);
        connection = getHiveConnection();
    }

    @Override
    public void commit(Context context) throws Exception {
        final String databaseName = context.databaseName();
        final String tableName = context.tableName();
        Statement statement = connection.createStatement();
        String sql = generateCreatePartitionSql(databaseName, tableName, context.partition());
        statement.execute(sql);
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    public static String getHiveConnStr(String hiveServerJdbcUrl, String databaseName) {
        return hiveServerJdbcUrl + "/" + databaseName;
    }

    public static String generateCreatePartitionSql(
            String databaseName,
            String tableName,
            HivePartition hivePartition) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder
                .append("ALTER TABLE ")
                .append(databaseName)
                .append(".")
                .append(tableName)
                .append(" ADD IF NOT EXISTS PARTITION (");

        for (Tuple2<String, String> partition: hivePartition.getPartitions()) {
            stringBuilder
                    .append(partition.f0)
                    .append(" = '")
                    .append(partition.f1)
                    .append("', ");
        }

        String result = stringBuilder.toString();
        result = result.trim();
        result = result.substring(0, result.length() - 1);

        return result + ")";
    }

    private Connection getHiveConnection() throws SQLException, ClassNotFoundException {
        Class.forName(driverClass);

        String hiveServerJdbcUrl = hiveSinkInfo.getHiveServerJdbcUrl();
        String username = hiveSinkInfo.getUsername();
        String password = hiveSinkInfo.getPassword();
        String databaseName = hiveSinkInfo.getDatabaseName();
        String connStr = getHiveConnStr(hiveServerJdbcUrl, databaseName);
        Connection connection = DriverManager.getConnection(connStr, username, password);

        LOG.info("Connect to hive {} successfully", connStr);
        return connection;
    }

    public static class Factory implements PartitionCommitPolicy.Factory {

        private static final long serialVersionUID = 1680762247741262988L;

        @Override
        public PartitionCommitPolicy create(Configuration configuration, HiveSinkInfo hiveSinkInfo) throws Exception {
            return new JdbcHivePartitionCommitPolicy(configuration, hiveSinkInfo);
        }
    }
}
