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

package org.apache.inlong.agent.plugin.sources;

import org.apache.inlong.agent.common.AgentThreadFactory;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.InstanceProfile;
import org.apache.inlong.agent.constant.AgentConstants;
import org.apache.inlong.agent.constant.SqlServerConstants;
import org.apache.inlong.agent.constant.TaskConstants;
import org.apache.inlong.agent.except.FileException;
import org.apache.inlong.agent.plugin.sources.file.AbstractSource;

import io.debezium.connector.sqlserver.SqlServerConnector;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.engine.spi.OffsetCommitPolicy;
import io.debezium.relational.history.FileDatabaseHistory;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * SQLServer source
 */
public class SQLServerSource extends AbstractSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(SQLServerSource.class);
    private static final Integer DEBEZIUM_QUEUE_SIZE = 100;
    private ExecutorService executor;
    public InstanceProfile profile;
    private BlockingQueue<SourceData> debeziumQueue;
    private final Properties props = new Properties();
    private String dbName;
    private String schemaName;
    private String tableName;

    public SQLServerSource() {
    }

    protected void initSource(InstanceProfile profile) {
        try {
            LOGGER.info("SQLServerSource init: {}", profile.toJsonStr());
            debeziumQueue = new LinkedBlockingQueue<>(DEBEZIUM_QUEUE_SIZE);

            dbName = profile.get(TaskConstants.TASK_SQLSERVER_DB_NAME);
            schemaName = profile.get(TaskConstants.TASK_SQLSERVER_SCHEMA_NAME);
            tableName = profile.get(TaskConstants.TASK_SQLSERVER_TABLE_NAME);

            props.setProperty("name", "SQLServer-" + instanceId);
            props.setProperty("connector.class", SqlServerConnector.class.getName());
            props.setProperty("offset.storage", FileOffsetBackingStore.class.getName());
            String agentPath = AgentConfiguration.getAgentConf()
                    .get(AgentConstants.AGENT_HOME, AgentConstants.DEFAULT_AGENT_HOME);
            String offsetPath = agentPath + "/" + getThreadName() + "/offset.dat";
            props.setProperty("offset.storage.file.filename", offsetPath);
            props.setProperty("offset.flush.interval.ms", "10000");
            props.setProperty("database.history", FileDatabaseHistory.class.getCanonicalName());
            props.setProperty("database.history.file.filename", agentPath + "/" + getThreadName() + "/history.dat");
            // ignore "schema" and extract data from "payload"
            props.setProperty("key.converter.schemas.enable", "false");
            props.setProperty("value.converter.schemas.enable", "false");
            // ignore ddl
            props.setProperty("include.schema.changes", "false");
            if (Boolean.parseBoolean(profile.get(TaskConstants.TASK_SQLSERVER_UNIX_TIMESTAMP_FORMAT_ENABLE, "true"))) {
                // convert time to formatted string
                props.setProperty("converters", "datetime");
                props.setProperty("datetime.type", "org.apache.inlong.agent.plugin.utils.SQLServerTimeConverter");
                props.setProperty("datetime.format.date", "yyyy-MM-dd");
                props.setProperty("datetime.format.time", "HH:mm:ss");
                props.setProperty("datetime.format.datetime", "yyyy-MM-dd HH:mm:ss");
                props.setProperty("datetime.format.timestamp", "yyyy-MM-dd HH:mm:ss");
            }

            props.setProperty(String.valueOf(SqlServerConnectorConfig.HOSTNAME),
                    profile.get(TaskConstants.TASK_SQLSERVER_HOSTNAME));
            props.setProperty(String.valueOf(SqlServerConnectorConfig.PORT),
                    profile.get(TaskConstants.TASK_SQLSERVER_PORT));
            props.setProperty(String.valueOf(SqlServerConnectorConfig.USER),
                    profile.get(TaskConstants.TASK_SQLSERVER_USER));
            props.setProperty(String.valueOf(SqlServerConnectorConfig.PASSWORD),
                    profile.get(TaskConstants.TASK_SQLSERVER_PASSWORD));
            props.setProperty(String.valueOf(SqlServerConnectorConfig.DATABASE_NAME),
                    profile.get(TaskConstants.TASK_SQLSERVER_DB_NAME));
            props.setProperty(String.valueOf(SqlServerConnectorConfig.SNAPSHOT_MODE),
                    profile.get(TaskConstants.TASK_SQLSERVER_SNAPSHOT_MODE, SqlServerConstants.INITIAL));
            props.setProperty(String.valueOf(SqlServerConnectorConfig.SERVER_NAME),
                    profile.get(TaskConstants.TASK_SQLSERVER_SERVER_NAME));
            props.setProperty(String.valueOf(SqlServerConnectorConfig.SCHEMA_INCLUDE_LIST),
                    profile.get(TaskConstants.TASK_SQLSERVER_SCHEMA_NAME));
            props.setProperty(String.valueOf(SqlServerConnectorConfig.TABLE_INCLUDE_LIST),
                    profile.get(TaskConstants.TASK_SQLSERVER_TABLE_NAME));

            executor = Executors.newSingleThreadExecutor();
            executor.execute(startDebeziumEngine());
        } catch (Exception ex) {
            stopRunning();
            throw new FileException("error init stream for " + instanceId, ex);
        }
    }

    private Runnable startDebeziumEngine() {
        return () -> {
            AgentThreadFactory.nameThread(getThreadName() + "debezium");
            try (DebeziumEngine<ChangeEvent<String, String>> debeziumEngine = DebeziumEngine.create(Json.class)
                    .using(props)
                    .using(OffsetCommitPolicy.always())
                    .notifying(this::handleConsumerEvent)
                    .build()) {

                debeziumEngine.run();
            } catch (Throwable e) {
                LOGGER.error("do run error in SQLServer debezium: ", e);
            }
        };
    }

    private void handleConsumerEvent(List<ChangeEvent<String, String>> records,
            DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> committer) throws InterruptedException {
        for (ChangeEvent<String, String> record : records) {
            boolean offerSuc = false;
            SourceData sourceData = new SourceData(record.value().getBytes(StandardCharsets.UTF_8), "0");
            while (isRunnable() && !offerSuc) {
                offerSuc = debeziumQueue.offer(sourceData, 1, TimeUnit.SECONDS);
            }
            committer.markProcessed(record);
        }
        committer.markBatchFinished();
    }

    @Override
    protected String getThreadName() {
        return "SQLServer-source-" + taskId + "-" + instanceId;
    }

    @Override
    protected void printCurrentState() {
        LOGGER.info("sqlserver databases is {} and schema is {} and table is {}", dbName, schemaName, tableName);
    }

    @Override
    protected boolean doPrepareToRead() {
        return true;
    }

    @Override
    protected List<SourceData> readFromSource() {
        List<SourceData> dataList = new ArrayList<>();
        try {
            int size = 0;
            while (size < BATCH_READ_LINE_TOTAL_LEN) {
                SourceData sourceData = debeziumQueue.poll(1, TimeUnit.SECONDS);
                if (sourceData != null) {
                    LOGGER.info("readFromSource: {}", sourceData.getData());
                    size += sourceData.getData().length;
                    dataList.add(sourceData);
                } else {
                    break;
                }
            }

        } catch (InterruptedException e) {
            LOGGER.error("poll {} data from debezium queue interrupted.", instanceId);
        }
        return dataList;
    }

    @Override
    protected boolean isRunnable() {
        return runnable;
    }

    @Override
    protected void releaseSource() {
        LOGGER.info("release sqlserver source");
        executor.shutdownNow();
    }

    @Override
    public boolean sourceExist() {
        return true;
    }
}