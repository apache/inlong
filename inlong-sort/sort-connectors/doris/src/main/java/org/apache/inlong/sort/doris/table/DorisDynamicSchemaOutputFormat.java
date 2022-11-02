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
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.inlong.sort.base.format.DynamicSchemaFormatFactory;
import org.apache.inlong.sort.base.format.JsonDynamicSchemaFormat;
import org.apache.inlong.sort.base.metric.MetricOption;
import org.apache.inlong.sort.base.metric.MetricState;
import org.apache.inlong.sort.base.metric.SinkMetricData;
import org.apache.inlong.sort.base.util.MetricStateUtils;
import org.apache.inlong.sort.doris.model.RespContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import static org.apache.inlong.sort.base.Constants.INLONG_METRIC_STATE_NAME;
import static org.apache.inlong.sort.base.Constants.NUM_BYTES_OUT;
import static org.apache.inlong.sort.base.Constants.NUM_RECORDS_OUT;

/**
 * DorisDynamicSchemaOutputFormat, copy from {@link org.apache.doris.flink.table.DorisDynamicOutputFormat}
 * It is used in the multiple sink scenario, in this scenario, we directly convert the data format by
 * 'sink.multiple.format' in the data stream to doris json that is used to load
 */
public class DorisDynamicSchemaOutputFormat<T> extends RichOutputFormat<T> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DorisDynamicSchemaOutputFormat.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String COLUMNS_KEY = "columns";
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
    private final Map<String, String> columnsMap = new HashMap<>();
    private final List<String> errorTables = new ArrayList<>();
    private final DorisOptions options;
    private final DorisReadOptions readOptions;
    private final DorisExecutionOptions executionOptions;
    private final String databasePattern;
    private final String tablePattern;
    private final String dynamicSchemaFormat;
    private final boolean ignoreSingleTableErrors;
    private final Map<String, Exception> flushExceptionMap = new HashMap<>();
    private final AtomicLong readInNum = new AtomicLong(0);
    private final AtomicLong writeOutNum = new AtomicLong(0);
    private final AtomicLong errorNum = new AtomicLong(0);
    private final AtomicLong ddlNum = new AtomicLong(0);
    private final String inlongMetric;
    private final String auditHostAndPorts;
    private long batchBytes = 0L;
    private int size;
    private DorisStreamLoad dorisStreamLoad;
    private transient volatile boolean closed = false;
    private transient volatile boolean flushing = false;
    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;
    private transient JsonDynamicSchemaFormat jsonDynamicSchemaFormat;
    private transient SinkMetricData metricData;
    private transient ListState<MetricState> metricStateListState;
    private transient MetricState metricState;

    public DorisDynamicSchemaOutputFormat(DorisOptions option,
            DorisReadOptions readOptions,
            DorisExecutionOptions executionOptions,
            String dynamicSchemaFormat,
            String databasePattern,
            String tablePattern,
            boolean ignoreSingleTableErrors,
            String inlongMetric,
            String auditHostAndPorts) {
        this.options = option;
        this.readOptions = readOptions;
        this.executionOptions = executionOptions;
        this.dynamicSchemaFormat = dynamicSchemaFormat;
        this.databasePattern = databasePattern;
        this.tablePattern = tablePattern;
        this.ignoreSingleTableErrors = ignoreSingleTableErrors;
        this.inlongMetric = inlongMetric;
        this.auditHostAndPorts = auditHostAndPorts;
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
        MetricOption metricOption = MetricOption.builder()
                .withInlongLabels(inlongMetric)
                .withInlongAudit(auditHostAndPorts)
                .withInitRecords(metricState != null ? metricState.getMetricValue(NUM_RECORDS_OUT) : 0L)
                .withInitBytes(metricState != null ? metricState.getMetricValue(NUM_BYTES_OUT) : 0L)
                .withRegisterMetric(MetricOption.RegisteredMetric.ALL)
                .build();
        if (metricOption != null) {
            metricData = new SinkMetricData(metricOption, getRuntimeContext().getMetricGroup());
        }
        if (executionOptions.getBatchIntervalMs() != 0 && executionOptions.getBatchSize() != 1) {
            this.scheduler = new ScheduledThreadPoolExecutor(1,
                    new ExecutorThreadFactory("doris-streamload-output-format"));
            this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(() -> {
                if (!closed && !flushing) {
                    flush();
                }
            }, executionOptions.getBatchIntervalMs(), executionOptions.getBatchIntervalMs(), TimeUnit.MILLISECONDS);
        }
    }

    private boolean checkFlushException(String tableIdentifier) {
        Exception flushException = flushExceptionMap.get(tableIdentifier);
        if (flushException == null) {
            return false;
        }
        if (!ignoreSingleTableErrors) {
            throw new RuntimeException(
                    String.format("Writing records to streamload of tableIdentifier:%s failed.", tableIdentifier),
                    flushException);
        }
        return true;
    }

    @Override
    public synchronized void writeRecord(T row) throws IOException {
        addBatch(row);
        boolean valid = (executionOptions.getBatchSize() > 0 && size >= executionOptions.getBatchSize())
                || batchBytes >= executionOptions.getMaxBatchBytes();
        if (valid && !flushing) {
            flush();
        }
    }

    @SuppressWarnings({"unchecked"})
    private void addBatch(T row) throws IOException {
        if (row instanceof RowData) {
            RowData rowData = (RowData) row;
            JsonNode rootNode = jsonDynamicSchemaFormat.deserialize(rowData.getBinary(0));
            readInNum.incrementAndGet();
            boolean isDDL = jsonDynamicSchemaFormat.extractDDLFlag(rootNode);
            if (isDDL) {
                ddlNum.incrementAndGet();
                // Ignore ddl change for now
                return;
            }
            String tableIdentifier = StringUtils.join(
                    jsonDynamicSchemaFormat.parse(rootNode, databasePattern), ".",
                    jsonDynamicSchemaFormat.parse(rootNode, tablePattern));
            if (checkFlushException(tableIdentifier)) {
                return;
            }
            List<RowKind> rowKinds = jsonDynamicSchemaFormat
                    .opType2RowKind(jsonDynamicSchemaFormat.getOpType(rootNode));
            JsonNode physicalData = jsonDynamicSchemaFormat.getPhysicalData(rootNode);
            List<Map<String, String>> physicalDataList = jsonDynamicSchemaFormat.jsonNode2Map(physicalData);
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
                            handleColumnsChange(tableIdentifier, rootNode, physicalData);
                            batchBytes += physicalDataList.get(i).toString().getBytes(StandardCharsets.UTF_8).length;
                            size++;
                            batchMap.computeIfAbsent(tableIdentifier, k -> new ArrayList<>())
                                    .add(physicalDataList.get(i));
                            if (enableBatchDelete()) {
                                physicalDataList.get(i).put(DORIS_DELETE_SIGN, DORIS_DELETE_FALSE);
                            }
                            break;
                        case DELETE:
                            handleColumnsChange(tableIdentifier, rootNode, physicalData);
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
                                handleColumnsChange(tableIdentifier, rootNode, updateBeforeNode);
                                batchBytes += updateBeforeList.get(i).toString()
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
                            errorNum.incrementAndGet();
                            throw new RuntimeException("Unrecognized row kind:" + rowKind.toString());
                    }
                }
            }
        } else {
            throw new RuntimeException("The type of element should be 'RowData' only.");
        }
    }

    private void handleColumnsChange(String tableIdentifier, JsonNode rootNode, JsonNode physicalData) {
        String columns = parseColumns(rootNode, physicalData);
        String oldColumns = columnsMap.get(tableIdentifier);
        if (columns == null && oldColumns != null || (columns != null && !columns.equals(oldColumns))) {
            flushSingleTable(tableIdentifier, batchMap.get(tableIdentifier));
            if (!errorTables.contains(tableIdentifier)) {
                columnsMap.put(tableIdentifier, columns);
            } else {
                batchMap.remove(tableIdentifier);
                columnsMap.remove(tableIdentifier);
                errorTables.remove(tableIdentifier);
            }
        }
    }

    private String parseColumns(JsonNode rootNode, JsonNode physicalData) {
        // Add column key when fieldNames is not empty
        Iterator<String> fieldNames = null;
        try {
            RowType rowType = jsonDynamicSchemaFormat.extractSchema(rootNode);
            if (rowType != null) {
                fieldNames = rowType.getFieldNames().listIterator();
            }
        } catch (IllegalArgumentException e) {
            LOG.warn("extract schema failed", e);
            // Extract schema from physicalData
            JsonNode first = physicalData.isArray() ? physicalData.get(0) : physicalData;
            // Add column key when fieldNames is not empty
            fieldNames = first.fieldNames();
        }
        return genColumns(fieldNames);
    }

    private String genColumns(Iterator<String> fieldNames) {
        if (fieldNames != null && fieldNames.hasNext()) {
            StringBuilder sb = new StringBuilder();
            while (fieldNames.hasNext()) {
                String item = fieldNames.next();
                sb.append("`").append(item.trim()
                        .replace("`", "")).append("`,");
            }
            if (enableBatchDelete()) {
                sb.append(DORIS_DELETE_SIGN);
            } else {
                sb.deleteCharAt(sb.lastIndexOf(","));
            }
            return sb.toString();
        }
        return null;
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
    }

    @SuppressWarnings({"rawtypes"})
    public synchronized void flush() {
        flushing = true;
        if (!hasRecords()) {
            flushing = false;
            return;
        }
        for (Entry<String, List> kvs : batchMap.entrySet()) {
            flushSingleTable(kvs.getKey(), kvs.getValue());
        }
        if (!errorTables.isEmpty()) {
            // Clean the key that has errors
            errorTables.forEach(batchMap::remove);
            errorTables.clear();
        }
        batchBytes = 0;
        size = 0;
        LOG.info("Doris sink statistics: readInNum: {}, writeOutNum: {}, errorNum: {}, ddlNum: {}",
                readInNum.get(), writeOutNum.get(), errorNum.get(), ddlNum.get());
        flushing = false;
    }

    @SuppressWarnings({"rawtypes"})
    private void flushSingleTable(String tableIdentifier, List values) {
        if (checkFlushException(tableIdentifier) || values == null || values.isEmpty()) {
            return;
        }
        String loadValue = null;
        try {
            loadValue = OBJECT_MAPPER.writeValueAsString(values);
            RespContent respContent = load(tableIdentifier, loadValue);
            try {
                if (null != metricData && null != respContent) {
                    metricData.invoke(respContent.getNumberLoadedRows(), respContent.getLoadBytes());
                }
            } catch (Exception e) {
                LOG.warn("metricData invoke get err:", e);
            }
            LOG.info("load {} records to tableIdentifier: {}", values.size(), tableIdentifier);
            writeOutNum.addAndGet(values.size());
            // Clean the data that has been loaded.
            values.clear();
        } catch (Exception e) {
            flushExceptionMap.put(tableIdentifier, e);
            errorNum.getAndAdd(values.size());
            if (!ignoreSingleTableErrors) {
                throw new RuntimeException(
                        String.format("Writing records to streamload of tableIdentifier:%s failed, the value: %s.",
                                tableIdentifier, loadValue), e);
            }
            errorTables.add(tableIdentifier);
            LOG.warn("The tableIdentifier: {} load failed and the data will be throw away in the future"
                    + " because the option 'sink.multiple.ignore-single-table-errors' is 'true'", tableIdentifier);
        }
    }

    @SuppressWarnings("rawtypes")
    private boolean hasRecords() {
        if (batchMap.isEmpty()) {
            return false;
        }
        boolean hasRecords = false;
        for (List value : batchMap.values()) {
            if (!value.isEmpty()) {
                hasRecords = true;
                break;
            }
        }
        return hasRecords;
    }

    private RespContent load(String tableIdentifier, String result) throws IOException {
        String[] tableWithDb = tableIdentifier.split("\\.");
        RespContent respContent = null;
        // Dynamic set COLUMNS_KEY for tableIdentifier every time
        executionOptions.getStreamLoadProp().put(COLUMNS_KEY, columnsMap.get(tableIdentifier));
        for (int i = 0; i <= executionOptions.getMaxRetries(); i++) {
            try {
                respContent = dorisStreamLoad.load(tableWithDb[0], tableWithDb[1], result);
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
        return respContent;
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

    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (metricData != null && metricStateListState != null) {
            MetricStateUtils.snapshotMetricStateForSinkMetricData(metricStateListState, metricData,
                    getRuntimeContext().getIndexOfThisSubtask());
        }
    }

    public void initializeState(FunctionInitializationContext context) throws Exception {
        if (this.inlongMetric != null) {
            this.metricStateListState = context.getOperatorStateStore().getUnionListState(
                    new ListStateDescriptor<>(
                            INLONG_METRIC_STATE_NAME, TypeInformation.of(new TypeHint<MetricState>() {
                    })));
        }
        if (context.isRestored()) {
            metricState = MetricStateUtils.restoreMetricState(metricStateListState,
                    getRuntimeContext().getIndexOfThisSubtask(), getRuntimeContext().getNumberOfParallelSubtasks());
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
        private boolean ignoreSingleTableErrors;
        private String inlongMetric;
        private String auditHostAndPorts;

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

        public DorisDynamicSchemaOutputFormat.Builder setIgnoreSingleTableErrors(boolean ignoreSingleTableErrors) {
            this.ignoreSingleTableErrors = ignoreSingleTableErrors;
            return this;
        }

        public DorisDynamicSchemaOutputFormat.Builder setInlongMetric(String inlongMetric) {
            this.inlongMetric = inlongMetric;
            return this;
        }

        public DorisDynamicSchemaOutputFormat.Builder setAuditHostAndPorts(String auditHostAndPorts) {
            this.auditHostAndPorts = auditHostAndPorts;
            return this;
        }

        @SuppressWarnings({"rawtypes"})
        public DorisDynamicSchemaOutputFormat build() {
            return new DorisDynamicSchemaOutputFormat(
                    optionsBuilder.build(), readOptions, executionOptions,
                    dynamicSchemaFormat, databasePattern, tablePattern,
                    ignoreSingleTableErrors, inlongMetric, auditHostAndPorts);
        }
    }
}