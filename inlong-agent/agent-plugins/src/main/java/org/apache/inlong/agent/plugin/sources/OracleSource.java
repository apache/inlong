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
import org.apache.inlong.agent.except.FileException;
import org.apache.inlong.agent.plugin.Message;
import org.apache.inlong.agent.plugin.sources.file.AbstractSource;
import org.apache.inlong.agent.plugin.sources.file.extend.DefaultExtendedHandler;

import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.OracleConnectorConfig;
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

import static org.apache.inlong.agent.constant.TaskConstants.TASK_ORACLE_DBNAME;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_ORACLE_HOSTNAME;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_ORACLE_PASSWORD;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_ORACLE_PORT;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_ORACLE_SCHEMA_INCLUDE_LIST;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_ORACLE_SNAPSHOT_MODE;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_ORACLE_TABLE_INCLUDE_LIST;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_ORACLE_USER;

/**
 * Oracle SQL source
 */
public class OracleSource extends AbstractSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleSource.class);
    private static final Integer DEBEZIUM_QUEUE_SIZE = 100;
    private ExecutorService executor;
    public InstanceProfile profile;
    private BlockingQueue<SourceData> debeziumQueue;
    private Properties props = new Properties();

    private String snapshotMode;
    private String dbName;
    private String tableName;
    private String schema;

    public OracleSource() {
    }

    @Override
    protected String getThreadName() {
        return "oracle-source-" + taskId + "-" + instanceId;
    }

    @Override
    protected void initExtendClass() {
        extendClass = DefaultExtendedHandler.class.getCanonicalName();
    }

    @Override
    protected void initSource(InstanceProfile profile) {
        try {
            LOGGER.info("OracleSource init: {}", profile.toJsonStr());
            debeziumQueue = new LinkedBlockingQueue<>(DEBEZIUM_QUEUE_SIZE);

            dbName = profile.get(TASK_ORACLE_DBNAME);
            tableName = profile.get(TASK_ORACLE_TABLE_INCLUDE_LIST);
            schema = profile.get(TASK_ORACLE_SCHEMA_INCLUDE_LIST);
            snapshotMode = profile.get(TASK_ORACLE_SNAPSHOT_MODE, "initial");

            props.setProperty("name", "Oracle-" + instanceId);
            props.setProperty("connector.class", OracleConnector.class.getName());

            // Unified storage in "[agentPath]/data/"
            String agentPath =
                    AgentConfiguration.getAgentConf().get(AgentConstants.AGENT_HOME, AgentConstants.DEFAULT_AGENT_HOME);
            String offsetPath = agentPath + "/data/" + getThreadName() + "/" + "offset.dat";
            String historyPath = agentPath + "/data/" + getThreadName() + "/" + "history.dat";
            props.setProperty("offset.storage", FileOffsetBackingStore.class.getName());
            props.setProperty("offset.storage.file.filename", offsetPath);
            props.setProperty("database.history", FileDatabaseHistory.class.getCanonicalName());
            props.setProperty("database.history.file.filename", historyPath);

            props.setProperty(String.valueOf(OracleConnectorConfig.HOSTNAME), profile.get(TASK_ORACLE_HOSTNAME));
            props.setProperty(String.valueOf(OracleConnectorConfig.PORT), profile.get(TASK_ORACLE_PORT));
            props.setProperty(String.valueOf(OracleConnectorConfig.USER), profile.get(TASK_ORACLE_USER));
            props.setProperty(String.valueOf(OracleConnectorConfig.PASSWORD), profile.get(TASK_ORACLE_PASSWORD));
            props.setProperty(String.valueOf(OracleConnectorConfig.TABLE_INCLUDE_LIST), schema + "." + tableName);
            props.setProperty(String.valueOf(OracleConnectorConfig.SERVER_NAME), getThreadName());
            props.setProperty(String.valueOf(OracleConnectorConfig.DATABASE_NAME), profile.get(TASK_ORACLE_DBNAME));
            props.setProperty(String.valueOf(OracleConnectorConfig.SCHEMA_INCLUDE_LIST), schema);
            props.setProperty(String.valueOf(OracleConnectorConfig.SNAPSHOT_MODE), snapshotMode);

            // Prevent Base64 encoding of Oracle NUMBER type fields
            props.setProperty(String.valueOf(OracleConnectorConfig.DECIMAL_HANDLING_MODE), "string");

            props.setProperty("key.converter.schemas.enable", "false");
            props.setProperty("value.converter.schemas.enable", "false");

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
            DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> committer) throws InterruptedException {
        for (ChangeEvent<String, String> record : records) {
            boolean offerSuc = false;
            SourceData sourceData = new SourceData(record.value().getBytes(StandardCharsets.UTF_8), "0L");
            while (isRunnable() && !offerSuc) {
                offerSuc = debeziumQueue.offer(sourceData, 1, TimeUnit.SECONDS);
            }
            committer.markProcessed(record);
        }
        committer.markBatchFinished();
    }

    @Override
    protected void printCurrentState() {
        LOGGER.info("oracle table is {}", tableName);
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
    public Message read() {
        return super.read();
    }

    @Override
    protected boolean isRunnable() {
        return runnable;
    }

    @Override
    protected void releaseSource() {
        LOGGER.info("release oracle source");
        executor.shutdownNow();
    }

    @Override
    public boolean sourceFinish() {
        return super.sourceFinish();
    }

    @Override
    public boolean sourceExist() {
        return true;
    }
}
