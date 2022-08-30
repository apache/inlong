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

import com.alibaba.fastjson.JSONPath;
import com.google.common.base.Preconditions;
import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.MongoDbConnector;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.message.DefaultMessage;
import org.apache.inlong.agent.metrics.audit.AuditUtils;
import org.apache.inlong.agent.plugin.Message;
import org.apache.inlong.agent.plugin.sources.snapshot.MongoDBSnapshotBase;
import org.apache.inlong.agent.plugin.utils.InLongFileOffsetBackingStore;
import org.apache.inlong.agent.pojo.DebeziumFormat;
import org.apache.inlong.agent.pojo.DebeziumOffset;
import org.apache.inlong.agent.utils.DebeziumOffsetSerializer;
import org.apache.inlong.agent.utils.GsonUtil;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.inlong.agent.constant.CommonConstants.DEFAULT_MAP_CAPACITY;
import static org.apache.inlong.agent.constant.CommonConstants.PROXY_KEY_DATA;

/**
 * MongoDBReader : mongo source, split mongo source job into multi readers
 */
public class MongoDBReader extends AbstractReader {

    /**
     * job conf prefix: <br/>
     * job.mongoJob.mongodb.hosts<br/>
     * job.mongoJob.mongodb.name<br/>
     * job.mongoJob.mongodb.user<br/>
     * job.mongoJob.mongodb.password<br/>
     * {@link MongoDbConnectorConfig} <br/>
     * mongodb.hosts<br/>
     * mongodb.name<br/>
     */
    private static final String JOB_PARAM_PREFIX = "job.mongoJob.";
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDBReader.class);

    private String instanceId;
    private String offsetStoreFileName;
    private String specificOffsetFile;
    private String specificOffsetPos;
    private boolean finished = false;
    private boolean destroyed = false;

    private ExecutorService executor;
    /**
     * mongo snapshot info <br/>
     * Currently, there is no usage scenario
     */
    private MongoDBSnapshotBase snapshot;
    /**
     * mongo job queue queue size
     */
    private static final String JOB_DATABASE_QUEUE_SIZE = "job.mongoJob.queueSize";
    /**
     * mongo job history file name
     */
    public static final String JOB_DATABASE_STORE_HISTORY_FILENAME = "job.mongoJob.history.filename";
    public static final String JOB_DATABASE_OFFSET_SPECIFIC_OFFSET_FILE = "job.mongoJob.offset.specificOffsetFile";
    public static final String JOB_DATABASE_OFFSET_SPECIFIC_OFFSET_POS = "job.mongoJob.offset.specificOffsetPos";
    /**
     * mongo job offset
     */
    public static final String JOB_DATABASE_OFFSETS = "job.mongoJob.offsets";
    /**
     * message buffer queue
     */
    private LinkedBlockingQueue<Pair<String, DebeziumFormat>> bufferPool;

    @Override
    public Message read() {
        if (!bufferPool.isEmpty()) {
            super.readerMetric.pluginReadCount.incrementAndGet();
            return this.pollMessage();
        } else {
            return null;
        }
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
    public void setReadTimeout(long mills) {
    }

    @Override
    public void setWaitMillisecond(long millis) {
    }

    @Override
    public String getSnapshot() {
        if (snapshot != null) {
            return snapshot.getSnapshot();
        } else {
            return "";
        }
    }

    @Override
    public void finishRead() {
        this.finished = true;
    }

    @Override
    public boolean isSourceExist() {
        return true;
    }

    @Override
    public void destroy() {
        synchronized (this) {
            if (!destroyed) {
                this.executor.shutdownNow();
                this.snapshot.close();
                this.destroyed = true;
            }
        }
    }

    @Override
    public void init(JobProfile jobConf) {
        super.init(jobConf);
        this.setGlobalParamsValue(jobConf);
        this.startEmbeddedDebeziumEngine(jobConf);
    }

    /**
     * poll message from buffer pool
     *
     * @return org.apache.inlong.agent.plugin.Message
     */
    private Message pollMessage() {
        // Retrieves and removes the head of this queue,
        // or returns null if this queue is empty.
        Pair<String, DebeziumFormat> message = bufferPool.poll();
        if (message == null) {
            return null;
        }
        Map<String, String> header = new HashMap<>(DEFAULT_MAP_CAPACITY);
        header.put(PROXY_KEY_DATA, message.getKey());
        return new DefaultMessage(GsonUtil.toJson(message.getValue()).getBytes(StandardCharsets.UTF_8), header);
    }

    /**
     * set global parameters value
     *
     * @param jobConf job conf
     */
    private void setGlobalParamsValue(JobProfile jobConf) {
        bufferPool = new LinkedBlockingQueue<>(jobConf.getInt(JOB_DATABASE_QUEUE_SIZE, 1000));
        instanceId = jobConf.getInstanceId();
        // offset file absolute path
        offsetStoreFileName = jobConf.get(JOB_DATABASE_STORE_HISTORY_FILENAME,
                MongoDBSnapshotBase.getSnapshotFilePath()) + "/mongo-" + instanceId + "-offset.dat";
        // snapshot info
        snapshot = new MongoDBSnapshotBase(offsetStoreFileName);
        String offset = jobConf.get(JOB_DATABASE_OFFSETS, "");
        snapshot.save(offset, new File(offsetStoreFileName));
        // offset info
        specificOffsetFile = jobConf.get(JOB_DATABASE_OFFSET_SPECIFIC_OFFSET_FILE, "");
        specificOffsetPos = jobConf.get(JOB_DATABASE_OFFSET_SPECIFIC_OFFSET_POS, "-1");
    }

    /**
     * start the embedded debezium engine
     *
     * @param jobConf job conf
     */
    private void startEmbeddedDebeziumEngine(JobProfile jobConf) {
        DebeziumEngine<ChangeEvent<String, String>> debeziumEngine = DebeziumEngine.create(Json.class)
                .using(this.buildMongoConnectorConfig(jobConf))
                .notifying(this::handleChangeEvent)
                .using(this::handle)
                .build();
        this.executor = Executors.newSingleThreadExecutor();
        this.executor.execute(debeziumEngine);
    }

    /**
     * Handle the completion of the embedded connector engine.
     *
     * @param success {@code true} if the connector completed normally,
     *                or {@code false} if the connector produced an error
     *                that prevented startup or premature termination.
     * @param message the completion message; never null
     * @param error   the error, or null if there was no exception
     */
    private void handle(boolean success, String message, Throwable error) {
        //jobConf.getInstanceId()
        if (!success) {
            LOGGER.error("{}, {}", message, error);
        }
    }

    /**
     * A Configuration object is basically a decorator around a {@link Properties} object.
     *
     * @return Configuration
     */
    private Properties buildMongoConnectorConfig(JobProfile jobConf) {
        Configuration.Builder builder = Configuration.create();
        MongoDbConnectorConfig.ALL_FIELDS
                .forEach(field -> {
                            String defaultValue = field.defaultValueAsString();
                            String value = jobConf.get(JOB_PARAM_PREFIX + field.name(), defaultValue);
                            // Configuration parameters are not set and there is no default value
                            if (StringUtils.isBlank(defaultValue) && StringUtils.isBlank(value)) {
                                return;
                            }
                            builder.with(field, value);
                        }
                );
        Properties props = builder.build().asProperties();
        props.setProperty("offset.storage.file.filename", offsetStoreFileName);
        props.setProperty("connector.class", MongoDbConnector.class.getCanonicalName());
        props.setProperty("name", instanceId);

        String snapshotMode = props.getOrDefault(MongoDbConnectorConfig.SNAPSHOT_MODE.name(), "").toString();
        if (Objects.equals(MongoDbConnectorConfig.SnapshotMode.INITIAL.getValue(), snapshotMode)) {
            props.setProperty("offset.storage", InLongFileOffsetBackingStore.class.getCanonicalName());
            props.setProperty(InLongFileOffsetBackingStore.OFFSET_STATE_VALUE, serializeOffset());
        } else {
            props.setProperty("offset.storage", FileOffsetBackingStore.class.getCanonicalName());
        }
        LOGGER.info("mongo job {} start with props {}",
                jobConf.getInstanceId(),
                GsonUtil.toJson(props));
        return props;
    }

    private String serializeOffset() {
        Map<String, Object> sourceOffset = new HashMap<>();
        Preconditions.checkNotNull(JOB_DATABASE_OFFSET_SPECIFIC_OFFSET_FILE,
                JOB_DATABASE_OFFSET_SPECIFIC_OFFSET_FILE + " cannot be null");
        sourceOffset.put("file", specificOffsetFile);
        Preconditions.checkNotNull(JOB_DATABASE_OFFSET_SPECIFIC_OFFSET_POS,
                JOB_DATABASE_OFFSET_SPECIFIC_OFFSET_POS + " cannot be null");
        sourceOffset.put("pos", specificOffsetPos);
        DebeziumOffset specificOffset = new DebeziumOffset();
        specificOffset.setSourceOffset(sourceOffset);
        Map<String, String> sourcePartition = new HashMap<>();
        sourcePartition.put("server", instanceId);
        specificOffset.setSourcePartition(sourcePartition);
        byte[] serializedOffset = new byte[0];
        try {
            serializedOffset = DebeziumOffsetSerializer.INSTANCE.serialize(specificOffset);
        } catch (IOException e) {
            LOGGER.error("serialize offset message error", e);
        }
        return new String(serializedOffset, StandardCharsets.UTF_8);
    }

    /**
     * Handles a batch of records, calling the {@link DebeziumEngine.RecordCommitter#markProcessed(Object)}
     * for each record and {@link DebeziumEngine.RecordCommitter#markBatchFinished()} when this batch is finished.
     *
     * @param records   the records to be processed
     * @param committer the committer that indicates to the system that we are finished
     */
    private void handleChangeEvent(List<ChangeEvent<String, String>> records,
                                   DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> committer) {
        try {
            for (ChangeEvent<String, String> record : records) {
                DebeziumFormat debeziumFormat = JSONPath.read(record.value(), "$.payload", DebeziumFormat.class);
                bufferPool.put(Pair.of(debeziumFormat.getSource().getCollection(), debeziumFormat));
                committer.markProcessed(record);
            }
            committer.markBatchFinished();
            AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_READ_SUCCESS, super.inlongGroupId, super.inlongStreamId,
                    System.currentTimeMillis(), records.size());
            readerMetric.pluginReadCount.addAndGet(records.size());
        } catch (InterruptedException e) {
            e.printStackTrace();
            LOGGER.error("parse mongo message error", e);

            readerMetric.pluginReadFailCount.addAndGet(records.size());
        }
    }
}
