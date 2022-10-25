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

package org.apache.inlong.sort.doris.table;

import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.exception.DorisException;
import org.apache.doris.flink.exception.StreamLoadException;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.inlong.sort.base.format.DynamicSchemaFormatFactory;
import org.apache.inlong.sort.base.format.JsonDynamicSchemaFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * DorisDynamicSchemaOutputFormat, copy from {@link org.apache.doris.flink.table.DorisDynamicOutputFormat}
 * It is used in the multiple sink scenario, in this scenario, we directly convert the data format by
 * 'sink.multiple.format' in the data stream to doris json that is used to load
 */
public class DorisDynamicSchemaOutputFormat<T> extends RichOutputFormat<T> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DorisDynamicSchemaOutputFormat.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String DORIS_DELETE_SIGN = "__DORIS_DELETE_SIGN__";
    /**
     * Mark the record for delete
     */
    private static final String DORIS_DELETE_TRUE = "1";
    /**
     * Mark the record for not delete
     */
    private static final String DORIS_DELETE_FALSE = "0";
    @SuppressWarnings({"rawtypes"})
    private final Map<String, List> batchMap = new HashMap<>();
    private final DorisOptions options;
    private final DorisReadOptions readOptions;
    private final DorisExecutionOptions executionOptions;
    private final String databasePattern;
    private final String tablePattern;
    private final String dynamicSchemaFormat;
    private long batchBytes = 0L;
    private int size;
    private DorisStreamLoad dorisStreamLoad;
    private transient volatile boolean closed = false;
    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;
    private transient volatile Exception flushException;
    private transient JsonDynamicSchemaFormat jsonDynamicSchemaFormat;

    public DorisDynamicSchemaOutputFormat(DorisOptions option,
            DorisReadOptions readOptions,
            DorisExecutionOptions executionOptions,
            String dynamicSchemaFormat,
            String databasePattern,
            String tablePattern) {
        this.options = option;
        this.readOptions = readOptions;
        this.executionOptions = executionOptions;
        this.dynamicSchemaFormat = dynamicSchemaFormat;
        this.databasePattern = databasePattern;
        this.tablePattern = tablePattern;
    }

    /**
     * A builder used to set parameters to the output format's configuration in a fluent way.
     *
     * @return builder
     */
    public static DorisDynamicSchemaOutputFormat.Builder builder() {
        return new DorisDynamicSchemaOutputFormat.Builder();
    }

    private boolean enableBatchDelete() {
        return executionOptions.getEnableDelete();
    }

    @Override
    public void configure(Configuration configuration) {
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        dorisStreamLoad = new DorisStreamLoad(
                getBackend(),
                options.getUsername(),
                options.getPassword(),
                executionOptions.getStreamLoadProp());
        jsonDynamicSchemaFormat = (JsonDynamicSchemaFormat)
                DynamicSchemaFormatFactory.getFormat(dynamicSchemaFormat);
        if (executionOptions.getBatchIntervalMs() != 0 && executionOptions.getBatchSize() != 1) {
            this.scheduler = new ScheduledThreadPoolExecutor(1,
                    new ExecutorThreadFactory("doris-streamload-output-format"));
            this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(() -> {
                synchronized (DorisDynamicSchemaOutputFormat.this) {
                    if (!closed) {
                        try {
                            flush();
                        } catch (Exception e) {
                            flushException = e;
                        }
                    }
                }
            }, executionOptions.getBatchIntervalMs(), executionOptions.getBatchIntervalMs(), TimeUnit.MILLISECONDS);
        }
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new RuntimeException("Writing records to streamload failed.", flushException);
        }
    }

    @Override
    public synchronized void writeRecord(T row) throws IOException {
        checkFlushException();
        addBatch(row);
        boolean valid = (executionOptions.getBatchSize() > 0 && size >= executionOptions.getBatchSize())
                || batchBytes >= executionOptions.getMaxBatchBytes();
        if (valid) {
            flush();
        }
    }

    @SuppressWarnings({"unchecked"})
    private void addBatch(T row) throws IOException {
        if (row instanceof RowData) {
            RowData rowData = (RowData) row;
            JsonNode rootNode = jsonDynamicSchemaFormat.deserialize(rowData.getBinary(0));
            boolean isDDL = jsonDynamicSchemaFormat.extractDDLFlag(rootNode);
            if (isDDL) {
                // Ignore ddl change for now
                return;
            }
            String tableIdentifier = StringUtils.join(
                    jsonDynamicSchemaFormat.parse(rowData.getBinary(0), databasePattern),
                    ".",
                    jsonDynamicSchemaFormat.parse(rowData.getBinary(0), tablePattern));
            List<RowKind> rowKinds = jsonDynamicSchemaFormat
                    .opType2RowKind(jsonDynamicSchemaFormat.getOpType(rootNode));
            List<Map<String, String>> physicalDataList = jsonDynamicSchemaFormat.jsonNode2Map(
                    jsonDynamicSchemaFormat.getPhysicalData(rootNode));
            JsonNode updateBeforeNode = jsonDynamicSchemaFormat.getUpdateBefore(rootNode);
            List<Map<String, String>> updateBeforeList = null;
            if (updateBeforeNode != null) {
                updateBeforeList = jsonDynamicSchemaFormat.jsonNode2Map(updateBeforeNode);
            }
            for (int i = 0; i < physicalDataList.size(); i++) {
                for (RowKind rowKind : rowKinds) {
                    switch (rowKind) {
                        case INSERT:
                        case UPDATE_AFTER:
                            batchBytes += physicalDataList.get(i).toString().getBytes(StandardCharsets.UTF_8).length;
                            size++;
                            // add doris delete sign
                            if (enableBatchDelete()) {
                                physicalDataList.get(i).put(DORIS_DELETE_SIGN, DORIS_DELETE_FALSE);
                            }
                            batchMap.computeIfAbsent(tableIdentifier, k -> new ArrayList<>())
                                    .add(physicalDataList.get(i));
                            break;
                        case DELETE:
                            batchBytes += physicalDataList.get(i).toString().getBytes(StandardCharsets.UTF_8).length;
                            size++;
                            // add doris delete sign
                            if (enableBatchDelete()) {
                                physicalDataList.get(i).put(DORIS_DELETE_SIGN, DORIS_DELETE_TRUE);
                            }
                            batchMap.computeIfAbsent(tableIdentifier, k -> new ArrayList<>())
                                    .add(physicalDataList.get(i));
                            break;
                        case UPDATE_BEFORE:
                            if (updateBeforeList != null && updateBeforeList.size() > i) {
                                batchBytes += physicalDataList.get(i).toString()
                                        .getBytes(StandardCharsets.UTF_8).length;
                                size++;
                                // add doris delete sign
                                if (enableBatchDelete()) {
                                    updateBeforeList.get(i).put(DORIS_DELETE_SIGN, DORIS_DELETE_TRUE);
                                }
                                batchMap.computeIfAbsent(tableIdentifier, k -> new ArrayList<>())
                                        .add(updateBeforeList.get(i));
                            }
                            break;
                        default:
                            throw new RuntimeException("Unrecognized row kind:" + rowKind.toString());
                    }
                }
            }
        } else {
            throw new RuntimeException("The type of element should be 'RowData' only.");
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (!closed) {
            closed = true;

            if (this.scheduledFuture != null) {
                scheduledFuture.cancel(false);
                this.scheduler.shutdown();
            }

            try {
                flush();
            } catch (Exception e) {
                LOG.warn("Writing records to doris failed.", e);
                throw new RuntimeException("Writing records to doris failed.", e);
            } finally {
                this.dorisStreamLoad.close();
            }
        }
        checkFlushException();
    }

    @SuppressWarnings({"rawtypes"})
    public synchronized void flush() throws IOException {
        checkFlushException();
        if (batchMap.isEmpty()) {
            return;
        }
        for (Entry<String, List> kvs : batchMap.entrySet()) {
            load(kvs.getKey(), OBJECT_MAPPER.writeValueAsString(kvs.getValue()));
        }
    }

    private void load(String tableIdentifier, String result) throws IOException {
        String[] tableWithDb = tableIdentifier.split("\\.");
        for (int i = 0; i <= executionOptions.getMaxRetries(); i++) {
            try {
                dorisStreamLoad.load(tableWithDb[0], tableWithDb[1], result);
                batchMap.remove(tableIdentifier);
                break;
            } catch (StreamLoadException e) {
                LOG.error("doris sink error, retry times = {}", i, e);
                if (i >= executionOptions.getMaxRetries()) {
                    throw new IOException(e);
                }
                try {
                    dorisStreamLoad.setHostPort(getBackend());
                    LOG.warn("streamload error,switch be: {}",
                            dorisStreamLoad.getLoadUrlStr(tableWithDb[0], tableWithDb[1]), e);
                    Thread.sleep(1000 * i);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException("unable to flush; interrupted while doing another attempt", e);
                }
            }
        }
        batchBytes = 0;
        size = 0;
    }

    private String getBackend() throws IOException {
        try {
            //get be url from fe
            return RestService.randomBackend(options, readOptions, LOG);
        } catch (IOException | DorisException e) {
            LOG.error("get backends info fail");
            throw new IOException(e);
        }
    }

    /**
     * Builder for {@link DorisDynamicSchemaOutputFormat}.
     */
    public static class Builder {

        private final DorisOptions.Builder optionsBuilder;
        private DorisReadOptions readOptions;
        private DorisExecutionOptions executionOptions;
        private String dynamicSchemaFormat;
        private String databasePattern;
        private String tablePattern;

        public Builder() {
            this.optionsBuilder = DorisOptions.builder().setTableIdentifier("");
        }

        public DorisDynamicSchemaOutputFormat.Builder setFenodes(String fenodes) {
            this.optionsBuilder.setFenodes(fenodes);
            return this;
        }

        public DorisDynamicSchemaOutputFormat.Builder setUsername(String username) {
            this.optionsBuilder.setUsername(username);
            return this;
        }

        public DorisDynamicSchemaOutputFormat.Builder setPassword(String password) {
            this.optionsBuilder.setPassword(password);
            return this;
        }

        public DorisDynamicSchemaOutputFormat.Builder setReadOptions(DorisReadOptions readOptions) {
            this.readOptions = readOptions;
            return this;
        }

        public DorisDynamicSchemaOutputFormat.Builder setExecutionOptions(DorisExecutionOptions executionOptions) {
            this.executionOptions = executionOptions;
            return this;
        }

        public DorisDynamicSchemaOutputFormat.Builder setDynamicSchemaFormat(
                String dynamicSchemaFormat) {
            this.dynamicSchemaFormat = dynamicSchemaFormat;
            return this;
        }

        public DorisDynamicSchemaOutputFormat.Builder setDatabasePattern(String databasePattern) {
            this.databasePattern = databasePattern;
            return this;
        }

        public DorisDynamicSchemaOutputFormat.Builder setTablePattern(String tablePattern) {
            this.tablePattern = tablePattern;
            return this;
        }

        @SuppressWarnings({"rawtypes"})
        public DorisDynamicSchemaOutputFormat build() {
            return new DorisDynamicSchemaOutputFormat(
                    optionsBuilder.build(), readOptions, executionOptions,
                    dynamicSchemaFormat, databasePattern, tablePattern);
        }
    }
}