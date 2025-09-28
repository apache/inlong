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

package org.apache.inlong.audit.store.service;

import org.apache.inlong.audit.protocol.AuditData;
import org.apache.inlong.audit.store.config.JdbcConfig;
import org.apache.inlong.audit.store.entities.JdbcDataPo;
import org.apache.inlong.audit.store.metric.MetricsManager;
import org.apache.inlong.audit.store.route.AuditRouteManager;
import org.apache.inlong.audit.store.utils.PulsarUtils;
import org.apache.inlong.audit.utils.DataUtils;
import org.apache.inlong.audit.utils.RouteUtils;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This is a general jdbc sink service. As long as it meets the jdbc protocol, you can use this service.
 */
public class JdbcService implements InsertData, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcService.class);
    private static final String INSERT_SQL = "insert into audit_data (ip, docker_id, thread_id, \r\n"
            + "      sdk_ts, packet_id, log_ts, \r\n"
            + "      inlong_group_id, inlong_stream_id, audit_id, audit_tag, audit_version, \r\n"
            + "      count, size, delay, \r\n"
            + "      update_time)\r\n"
            + "    values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    private final JdbcConfig jdbcConfig;

    private final ScheduledExecutorService timerService = Executors.newSingleThreadScheduledExecutor();
    private LinkedBlockingQueue<JdbcDataPo> receiveQueue;
    private long lastCheckTime = System.currentTimeMillis();
    private Connection connection;
    private final List<JdbcDataPo> writeDataList = new LinkedList<>();

    public JdbcService(JdbcConfig jdbcConfig) {
        this.jdbcConfig = jdbcConfig;
    }

    /**
     * start
     */
    public void start() {
        receiveQueue = new LinkedBlockingQueue<>(jdbcConfig.getDataQueueSize());
        try {
            Class.forName(jdbcConfig.getDriver());
            reconnect();
        } catch (Exception e) {
            LOG.error("Start failure!", e);
        }
        timerService.scheduleWithFixedDelay(this::process,
                jdbcConfig.getProcessIntervalMs(),
                jdbcConfig.getProcessIntervalMs(), TimeUnit.MILLISECONDS);
    }

    private void process() {
        if (receiveQueue.size() < jdbcConfig.getBatchThreshold()
                && (System.currentTimeMillis() - lastCheckTime < jdbcConfig.getBatchIntervalMs())) {
            return;
        }
        lastCheckTime = System.currentTimeMillis();

        if (writeDataList.size() > 0) {
            if (executeBatch(writeDataList)) {
                acknowledge(writeDataList);
                writeDataList.clear();
            } else {
                return;
            }
        }

        JdbcDataPo data = receiveQueue.poll();
        while (data != null) {
            writeDataList.add(data);
            if (writeDataList.size() > jdbcConfig.getBatchThreshold()) {
                if (executeBatch(writeDataList)) {
                    acknowledge(writeDataList);
                    writeDataList.clear();
                } else {
                    break;
                }
            }
            data = receiveQueue.poll();
        }
    }

    private boolean executeBatch(List<JdbcDataPo> dataList) {
        boolean result = false;

        long currentTimestamp = System.currentTimeMillis();

        try (PreparedStatement statement = connection.prepareStatement(INSERT_SQL)) {
            for (JdbcDataPo data : dataList) {
                statement.setString(1, data.getIp());
                statement.setString(2, data.getDockerId());
                statement.setString(3, data.getThreadId());
                statement.setTimestamp(4, data.getSdkTs());
                statement.setLong(5, data.getPacketId());
                statement.setTimestamp(6, data.getLogTs());
                statement.setString(7, data.getInLongGroupId());
                statement.setString(8, data.getInLongStreamId());
                statement.setString(9, data.getAuditId());
                statement.setString(10, data.getAuditTag());
                statement.setLong(11, data.getAuditVersion());
                statement.setLong(12, data.getCount());
                statement.setLong(13, data.getSize());
                statement.setLong(14, data.getDelay());
                statement.setTimestamp(15, data.getUpdateTime());
                statement.addBatch();
            }
            statement.executeBatch();
            connection.commit();
            result = true;

            MetricsManager.getInstance().addSendSuccess(dataList.size(), System.currentTimeMillis() - currentTimestamp);

        } catch (Exception exception) {

            MetricsManager.getInstance().addSendFailed(dataList.size(), System.currentTimeMillis() - currentTimestamp);
            LOG.error("Execute batch has failure!", exception);
            try {
                reconnect();
            } catch (SQLException sqlException) {
                LOG.error("Re-connect has  failure!", sqlException);
            }
        }
        return result;
    }

    /**
     * reconnect
     *
     * @throws SQLException Exception when creating connection.
     */
    private void reconnect() throws SQLException {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                LOG.error("Reconnect has exception!", e);
            }
            connection = null;
        }
        connection = DriverManager.getConnection(jdbcConfig.getUrl(), jdbcConfig.getUserName(),
                jdbcConfig.getPassword());
        connection.setAutoCommit(false);
    }

    /**
     * insert
     *
     * @param msgBody audit data reading from Pulsar or other MessageQueue.
     */
    @Override
    public void insert(AuditData msgBody) {
    }

    @Override
    public void insert(AuditData msgBody, Consumer<byte[]> consumer, MessageId messageId) {
        if (!isAuditDataValid(msgBody)) {
            MetricsManager.getInstance().addInvalidData();
            PulsarUtils.acknowledge(consumer, messageId);
            LOG.error("Invalid audit data: {} ", msgBody);
            return;
        }

        // Filter out audit data that does not match the routing rules
        if (!RouteUtils.matchesAuditRoute(msgBody.getAuditId(), msgBody.getInlongGroupId(),
                AuditRouteManager.getInstance().getAuditRoutes())) {
            MetricsManager.getInstance().filterSuccess();
            PulsarUtils.acknowledge(consumer, messageId);
            LOG.warn("The audit data does not match the routing rules and is filtered out: {} ", msgBody);
            return;
        }

        JdbcDataPo data = new JdbcDataPo();
        data.setConsumer(consumer);
        data.setMessageId(messageId);
        data.setIp(msgBody.getIp());
        data.setThreadId(msgBody.getThreadId());
        data.setDockerId(msgBody.getDockerId());
        data.setPacketId(msgBody.getPacketId());
        data.setSdkTs(new Timestamp(msgBody.getSdkTs()));
        data.setLogTs(new Timestamp(msgBody.getLogTs()));
        data.setAuditId(msgBody.getAuditId());
        data.setAuditTag(msgBody.getAuditTag());
        data.setAuditVersion(msgBody.getAuditVersion());
        data.setCount(msgBody.getCount());
        data.setDelay(msgBody.getDelay());
        data.setInLongGroupId(msgBody.getInlongGroupId());
        data.setInLongStreamId(msgBody.getInlongStreamId());
        data.setSize(msgBody.getSize());
        data.setUpdateTime(new Timestamp(System.currentTimeMillis()));
        try {
            receiveQueue.offer(data, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException exception) {
            LOG.error("Insert data has InterruptedException ", exception);
        }
    }

    @Override
    public void close() throws Exception {
        this.connection.close();
        this.timerService.shutdown();
    }

    private void acknowledge(List<JdbcDataPo> dataList) {
        Iterator<JdbcDataPo> iterator = dataList.iterator();
        while (iterator.hasNext()) {
            JdbcDataPo jdbcData = iterator.next();
            try {
                if (jdbcData.getConsumer() != null && jdbcData.getMessageId() != null) {
                    jdbcData.getConsumer().acknowledge(jdbcData.getMessageId());
                }
                iterator.remove();
            } catch (Exception exception) {
                LOG.error("Acknowledge has exception!", exception);
            }
        }
    }

    private boolean isAuditDataValid(AuditData auditData) {
        // Check if any of the timestamp fields are within the valid range
        if (!isDataTimeValid(auditData)) {
            return false;
        }
        // Check if any of the audit items are valid
        return isAuditItemValid(auditData);
    }

    private boolean isDataTimeValid(AuditData auditData) {
        long validDataTimeRangeMs = jdbcConfig.getValidDataTimeRangeMs();
        return DataUtils.isDataTimeValid(auditData.getLogTs(), validDataTimeRangeMs) &&
                DataUtils.isDataTimeValid(auditData.getSdkTs(), validDataTimeRangeMs);
    }

    private boolean isAuditItemValid(AuditData auditData) {
        return DataUtils.isAuditItemValid(auditData.getInlongGroupId()) &&
                DataUtils.isAuditItemValid(auditData.getInlongStreamId()) &&
                DataUtils.isAuditItemValid(auditData.getAuditId()) &&
                DataUtils.isAuditItemValid(auditData.getAuditTag()) &&
                DataUtils.isAuditItemValid(auditData.getIp()) &&
                DataUtils.isAuditItemValid(auditData.getDockerId()) &&
                DataUtils.isAuditItemValid(auditData.getThreadId());
    }
}
