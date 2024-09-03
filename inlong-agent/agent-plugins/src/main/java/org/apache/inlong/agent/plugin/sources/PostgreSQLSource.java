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
import org.apache.inlong.agent.constant.TaskConstants;
import org.apache.inlong.agent.except.FileException;
import org.apache.inlong.agent.plugin.sources.file.AbstractSource;

import io.debezium.connector.postgresql.PostgresConnector;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.engine.format.Json;
import io.debezium.engine.spi.OffsetCommitPolicy;
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

import static org.apache.inlong.agent.constant.TaskConstants.TASK_POSTGRES_DBNAME;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_POSTGRES_HOSTNAME;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_POSTGRES_PASSWORD;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_POSTGRES_PLUGIN_NAME;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_POSTGRES_PORT;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_POSTGRES_SCHEMA_INCLUDE_LIST;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_POSTGRES_TABLE_INCLUDE_LIST;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_POSTGRES_USER;

public class PostgreSQLSource extends AbstractSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgreSQLSource.class);
    private static final Integer DEBEZIUM_QUEUE_SIZE = 100;
    private ExecutorService executor;
    public InstanceProfile profile;
    private BlockingQueue<SourceData> debeziumQueue;
    private final Properties props = new Properties();
    private String snapshotMode;
    private String pluginName;
    private String dbName;
    private String tableName;

    private boolean isRestoreFromDB = false;

    public PostgreSQLSource() {

    }

    @Override
    protected void initSource(InstanceProfile profile) {
        try {
            LOGGER.info("PostgreSQLSource init: {}", profile.toJsonStr());
            debeziumQueue = new LinkedBlockingQueue<>(DEBEZIUM_QUEUE_SIZE);
            pluginName = profile.get(TASK_POSTGRES_PLUGIN_NAME);
            dbName = profile.get(TASK_POSTGRES_DBNAME);
            tableName = profile.get(TASK_POSTGRES_TABLE_INCLUDE_LIST);
            snapshotMode = profile.get(TaskConstants.TASK_POSTGRES_SNAPSHOT_MODE, "initial");

            props.setProperty("name", "PostgreSQL-" + instanceId);
            props.setProperty("connector.class", PostgresConnector.class.getName());
            props.setProperty("offset.storage", FileOffsetBackingStore.class.getName());
            String agentPath = AgentConfiguration.getAgentConf()
                    .get(AgentConstants.AGENT_HOME, AgentConstants.DEFAULT_AGENT_HOME);
            String offsetPath = agentPath + "/" + getThreadName() + "offset.dat";
            props.setProperty("offset.storage.file.filename", offsetPath);

            props.setProperty(String.valueOf(PostgresConnectorConfig.HOSTNAME), profile.get(TASK_POSTGRES_HOSTNAME));
            props.setProperty(String.valueOf(PostgresConnectorConfig.PORT), profile.get(TASK_POSTGRES_PORT));
            props.setProperty(String.valueOf(PostgresConnectorConfig.USER), profile.get(TASK_POSTGRES_USER));
            props.setProperty(String.valueOf(PostgresConnectorConfig.PASSWORD), profile.get(TASK_POSTGRES_PASSWORD));
            props.setProperty(String.valueOf(PostgresConnectorConfig.DATABASE_NAME), profile.get(TASK_POSTGRES_DBNAME));
            props.setProperty(String.valueOf(PostgresConnectorConfig.SERVER_NAME), getThreadName());
            props.setProperty(String.valueOf(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST),
                    profile.get(TASK_POSTGRES_SCHEMA_INCLUDE_LIST));
            props.setProperty(String.valueOf(PostgresConnectorConfig.TABLE_INCLUDE_LIST),
                    profile.get(TASK_POSTGRES_TABLE_INCLUDE_LIST));
            props.setProperty(String.valueOf(PostgresConnectorConfig.PLUGIN_NAME), pluginName);
            props.setProperty(String.valueOf(PostgresConnectorConfig.SNAPSHOT_MODE), snapshotMode);

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
                LOGGER.error("do run error in postgres debezium: ", e);
            }
        };
    }

    private void handleConsumerEvent(List<ChangeEvent<String, String>> records,
            RecordCommitter<ChangeEvent<String, String>> committer) throws InterruptedException {
        boolean offerSuc = false;
        for (ChangeEvent<String, String> record : records) {
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
        return "postgres-source-" + taskId + "-" + instanceId;
    }

    @Override
    protected void printCurrentState() {
        LOGGER.info("postgres databases is {} and table is {}", dbName, tableName);
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
        LOGGER.info("release postgres source");
        executor.shutdownNow();
    }

    @Override
    public boolean sourceExist() {
        return true;
    }
}
