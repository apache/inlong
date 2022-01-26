/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.flink.clickhouse;

import org.apache.inlong.sort.protocol.sink.ClickHouseSinkInfo;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;

import java.io.Serializable;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ClickHouseConnectionProvider implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseConnectionProvider.class);

    private static final String CLICKHOUSE_DRIVER_NAME = "ru.yandex.clickhouse.ClickHouseDriver";

    private static final Pattern PATTERN = Pattern.compile("You must use port (?<port>[0-9]+) for HTTP.");

    private final ClickHouseSinkInfo clickHouseSinkInfo;

    private transient ClickHouseConnection connection;

    private transient List<ClickHouseConnection> shardConnections;

    public ClickHouseConnectionProvider(ClickHouseSinkInfo clickHouseSinkInfo) {
        this.clickHouseSinkInfo = clickHouseSinkInfo;
    }

    public synchronized ClickHouseConnection getConnection() throws Exception {
        if (connection == null) {
            connection = createConnection(clickHouseSinkInfo.getUrl(), clickHouseSinkInfo.getDatabaseName());
        }
        return connection;
    }

    private ClickHouseConnection createConnection(String url, String database) throws Exception {
        LOG.info("Try connect to {}", url);

        ClickHouseConnection conn;
        Class.forName(CLICKHOUSE_DRIVER_NAME);

        if (clickHouseSinkInfo.getUsername() != null
                && clickHouseSinkInfo.getPassword() != null) {
            conn = (ClickHouseConnection) DriverManager.getConnection(
                    getJdbcUrl(url, database),
                    clickHouseSinkInfo.getUsername(),
                    clickHouseSinkInfo.getPassword());
        } else {
            conn = (ClickHouseConnection) DriverManager.getConnection(getJdbcUrl(url, database));
        }

        return conn;
    }

    /**
     * Get connection for the specified shard.
     */
    public synchronized List<ClickHouseConnection> getShardConnections(
            String remoteCluster, String remoteDatabase) throws Exception {
        if (shardConnections == null) {
            ClickHouseConnection conn = getConnection();
            PreparedStatement stmt = conn.prepareStatement(
                    "SELECT shard_num, host_address, port FROM system.clusters WHERE cluster = ?");
            stmt.setString(1, remoteCluster);

            try (ResultSet rs = stmt.executeQuery()) {
                shardConnections = new ArrayList<>();
                while (rs.next()) {
                    String host = rs.getString("host_address");
                    int port = getActualHttpPort(host, rs.getInt("port"));
                    String url = "clickhouse://" + host + ":" + port;
                    shardConnections.add(createConnection(url, remoteDatabase));
                }
            }

            if (shardConnections.isEmpty()) {
                throw new SQLException("unable to query shards in system.clusters");
            }
        }
        return shardConnections;
    }

    private int getActualHttpPort(String host, int port) throws SQLException {
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            HttpGet request = new HttpGet((new URIBuilder()).setScheme("http").setHost(host).setPort(port).build());
            CloseableHttpResponse closeableHttpResponse = httpclient.execute(request);
            int statusCode = closeableHttpResponse.getStatusLine().getStatusCode();
            if (statusCode == 200) {
                return port;
            }

            String raw = EntityUtils.toString(closeableHttpResponse.getEntity());
            Matcher matcher = PATTERN.matcher(raw);
            if (matcher.find()) {
                return Integer.parseInt(matcher.group("port"));
            }

            throw new SQLException("Cannot query ClickHouse http port");
        } catch (Exception e) {
            throw new SQLException("Cannot connect to ClickHouse server using HTTP", e);
        }
    }

    public void closeConnections() throws SQLException {
        if (connection != null) {
            connection.close();
        }

        if (shardConnections != null) {
            for (ClickHouseConnection shardConnection : shardConnections) {
                shardConnection.close();
            }
        }
    }

    private String getJdbcUrl(String url, String database) throws SQLException {
        try {
            return "jdbc:" + (new URIBuilder(url)).setPath("/" + database).build().toString();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public String queryTableEngine(String databaseName, String tableName) throws Exception {
        ClickHouseConnection conn = getConnection();
        try (PreparedStatement stmt = conn.prepareStatement(
                "SELECT engine_full FROM system.tables WHERE database = ? AND name = ?")) {
            stmt.setString(1, databaseName);
            stmt.setString(2, tableName);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("engine_full");
                }
            }
        }
        throw new SQLException("table `" + databaseName + "`.`" + tableName + "` does not exist");
    }
}

