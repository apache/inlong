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

package org.apache.inlong.agent.plugin.sources.reader;

import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.relational.history.FileDatabaseHistory;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.message.DefaultMessage;
import org.apache.inlong.agent.plugin.Message;
import org.apache.inlong.agent.plugin.Reader;
import org.apache.inlong.agent.plugin.sources.snapshot.BinlogSnapshotBase;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.slf4j.Logger;

import org.slf4j.LoggerFactory;

public class BinlogReader implements Reader {

    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogReader.class);

    private static final String JOB_DATABASE_USER = "job.binlogJob.user";
    private static final String JOB_DATABASE_PASSWORD = "job.binlogJob.password";
    private static final String JOB_DATABASE_HOSTNAME = "job.binlogJob.hostname";
    private static final String JOB_TABLE_WHITELIST = "job.binlogJob.tableWhiteList";
    private static final String JOB_DATABASE_WHITELIST = "job.binlogJob.databaseWhiteList";

    private static final String JOB_DATABASE_SNAPSHOT = "job.binlogJob.offset";
    private static final String JOB_DATABASE_OFFSET_FILENAME = "job.binlogJob.offset.filename";

    private static final String JOB_DATABASE_SERVER_TIME_ZONE = "job.binlogJob.serverTimezone";
    private static final String JOB_DATABASE_STORE_OFFSET_INTERVAL_MS = "job.binlogJob.offset.intervalMs";

    private static final String JOB_DATABASE_STORE_HISTORY_FILENAME = "job.binlogJob.history.filename";
    private static final String JOB_DATABASE_INCLUDE_SCHEMA_CHANGES = "job.binlogJob.schema";
    private static final String JOB_DATABASE_SNAPSHOT_MODE = "job.binlogJob.snapshot.mode";
    private static final String JOB_DATABASE_HISTORY_MONITOR_DDL = "job.binlogJob.ddl";
    private static final String JOB_DATABASE_PORT = "job.binlogJob.port";

    private static LinkedBlockingQueue<String> messageQueue = new LinkedBlockingQueue<>();

    private boolean finished = false;
    private String userName;
    private String password;
    private String hostName;
    private String port;
    private String tableWhiteList;
    private String databaseWhiteList;
    private String serverTimeZone;
    private String offsetStoreFileName;
    private String offsetFlushIntervalMs;
    private String databaseStoreHistoryName;
    private String includeSchemaChanges;
    private String snapshotMode;
    private String historyMonitorDdl;
    private String instanceId;
    private ExecutorService executor;
    private String offset;
    private BinlogSnapshotBase binlogSnapshot;
    private JobProfile jobProfile;

    public BinlogReader() {
    }

    @Override
    public Message read() {
        if (!messageQueue.isEmpty()) {
            return new DefaultMessage(messageQueue.poll().getBytes(StandardCharsets.UTF_8));
        } else {
            return null;
        }
    }

    @Override
    public void init(JobProfile jobConf) {
        jobProfile = jobConf;
        LOGGER.info("init binlog reader with jobConf {}", jobConf.toJsonStr());
        userName = jobConf.get(JOB_DATABASE_USER);
        password = jobConf.get(JOB_DATABASE_PASSWORD);
        hostName = jobConf.get(JOB_DATABASE_HOSTNAME);
        port = jobConf.get(JOB_DATABASE_PORT);
        tableWhiteList = jobConf.get(JOB_TABLE_WHITELIST, "");
        databaseWhiteList = jobConf.get(JOB_DATABASE_WHITELIST, "");
        serverTimeZone = jobConf.get(JOB_DATABASE_SERVER_TIME_ZONE, "");
        offsetFlushIntervalMs = jobConf.get(JOB_DATABASE_STORE_OFFSET_INTERVAL_MS, "1000");
        databaseStoreHistoryName = jobConf.get(JOB_DATABASE_STORE_HISTORY_FILENAME)
            + "history.dat" + jobConf.getInstanceId();
        offsetStoreFileName = jobConf.get(JOB_DATABASE_STORE_HISTORY_FILENAME)
            + "offset.dat" + jobConf.getInstanceId();
        snapshotMode = jobConf.get(JOB_DATABASE_SNAPSHOT_MODE, "");
        includeSchemaChanges = jobConf.get(JOB_DATABASE_INCLUDE_SCHEMA_CHANGES, "false");
        historyMonitorDdl = jobConf.get(JOB_DATABASE_HISTORY_MONITOR_DDL, "false");
        instanceId = jobConf.getInstanceId();
        finished = false;

        offset = jobConf.get(JOB_DATABASE_SNAPSHOT, "");
        binlogSnapshot = new BinlogSnapshotBase(offsetStoreFileName);
        binlogSnapshot.save(offset);

        Properties props = getEngineProps();

        DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(
            io.debezium.engine.format.Json.class)
            .using(props)
            .notifying((records, committer) -> {
                for (ChangeEvent<String, String> record : records) {
                    messageQueue.add(record.value());
                    committer.markProcessed(record);
                }
                committer.markBatchFinished();
            })
            .using((success, message, error) -> {
            }).build();

        executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);
    }

    private Properties getEngineProps() {
        Properties props = new Properties();

        props.setProperty("name", "engine" + instanceId);
        props.setProperty("connector.class", MySqlConnector.class.getCanonicalName());

        props.setProperty("database.server.name", instanceId);
        props.setProperty("database.hostname", hostName);
        props.setProperty("database.port", port);
        props.setProperty("database.user", userName);
        props.setProperty("database.password", password);
        props.setProperty("database.serverTimezone", serverTimeZone);
        props.setProperty("table.whitelist", tableWhiteList);
        props.setProperty("database.whitelist", databaseWhiteList);

        props.setProperty("offset.storage", FileOffsetBackingStore.class.getCanonicalName());
        props.setProperty("offset.storage.file.filename", offsetStoreFileName);
        props.setProperty("offset.flush.interval.ms", offsetFlushIntervalMs);
        props.setProperty("database.history", FileDatabaseHistory.class.getCanonicalName());
        props.setProperty("database.history.file.filename", databaseStoreHistoryName);
        props.setProperty("database.snapshot.mode", snapshotMode);
        props.setProperty("database.history.store.only.monitored.tables.ddl", historyMonitorDdl);
        props.setProperty("key.converter.schemas.enable", "false");
        props.setProperty("value.converter.schemas.enable", "false");
        props.setProperty("include.schema.changes", includeSchemaChanges);
        props.setProperty("snapshot.mode", snapshotMode);
        props.setProperty("tombstones.on.delete", "false");
        LOGGER.info("binlog job {} start with props {}", jobProfile.getInstanceId(), props);
        return props;
    }

    @Override
    public void destroy() {
        finished = true;
        executor.shutdownNow();
        binlogSnapshot.close();
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public String getReadSource() {
        return instanceId;
    }

    @Override
    public void setReadTimeout(long mill) {
        return;
    }

    @Override
    public void setWaitMillisecs(long millis) {
        return;
    }

    @Override
    public String getSnapshot() {
        return binlogSnapshot.getSnapshot();
    }

}
