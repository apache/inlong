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
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.rest.models.Schema;
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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.inlong.sort.base.dirty.DirtyData;
import org.apache.inlong.sort.base.dirty.DirtyOptions;
import org.apache.inlong.sort.base.dirty.DirtyType;
import org.apache.inlong.sort.base.dirty.sink.DirtySink;
import org.apache.inlong.sort.base.metric.MetricOption;
import org.apache.inlong.sort.base.metric.MetricState;
import org.apache.inlong.sort.base.metric.sub.SinkTableMetricData;
import org.apache.inlong.sort.base.util.MetricStateUtils;
import org.apache.inlong.sort.doris.internal.DorisOutputFormat;
import org.apache.inlong.sort.doris.model.RespContent;
import org.apache.inlong.sort.doris.util.DorisParseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.inlong.sort.base.Constants.DIRTY_BYTES_OUT;
import static org.apache.inlong.sort.base.Constants.DIRTY_RECORDS_OUT;
import static org.apache.inlong.sort.base.Constants.INLONG_METRIC_STATE_NAME;
import static org.apache.inlong.sort.base.Constants.NUM_BYTES_OUT;
import static org.apache.inlong.sort.base.Constants.NUM_RECORDS_OUT;
import static org.apache.inlong.sort.doris.util.DorisParseUtils.parseDeleteSign;

/**
 * DorisDynamicSchemaOutputFormat, copy from {@link org.apache.doris.flink.table.DorisDynamicOutputFormat}
 * It is used in the multiple sink scenario, in this scenario, we directly convert the data format by
 * 'sink.multiple.format' in the data stream to doris json that is used to load
 */
public class DorisSingleOutputFormat<T> extends RichOutputFormat<T> implements DorisOutputFormat<T> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DorisDynamicSchemaOutputFormat.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String COLUMNS_KEY = "columns";
    private static final String DORIS_DELETE_SIGN = "__DORIS_DELETE_SIGN__";
    private static final String FIELD_DELIMITER_KEY = "column_separator";
    private static final String FIELD_DELIMITER_DEFAULT = "\t";
    private static final String LINE_DELIMITER_KEY = "line_delimiter";
    private static final String LINE_DELIMITER_DEFAULT = "\n";
    private static final String NULL_VALUE = "\\N";
    private static final String ESCAPE_DELIMITERS_KEY = "escape_delimiters";
    private static final String ESCAPE_DELIMITERS_DEFAULT = "false";
    private static final String UNIQUE_KEYS_TYPE = "UNIQUE_KEYS";
    private static final String FORMAT_JSON_VALUE = "json";
    private static final String FORMAT_KEY = "format";
    @SuppressWarnings({"rawtypes"})
    private final List batch = new ArrayList<>();
    private final Map<String, String> columnsMap = new HashMap<>();
    private final List<String> errorTables = new ArrayList<>();
    private final DorisOptions options;
    private final DorisReadOptions readOptions;
    private final DorisExecutionOptions executionOptions;
    private final AtomicLong readInNum = new AtomicLong(0);
    private final AtomicLong writeOutNum = new AtomicLong(0);
    private final AtomicLong errorNum = new AtomicLong(0);
    private final AtomicLong ddlNum = new AtomicLong(0);
    private final String inlongMetric;
    private final String auditHostAndPorts;
    private transient volatile Exception flushException;
    private final boolean ignoreSingleTableErrors;
    private long batchBytes = 0L;
    private DorisStreamLoad dorisStreamLoad;
    private transient volatile boolean closed = false;
    private transient volatile boolean flushing = false;
    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;
    private transient SinkTableMetricData metricData;
    private transient ListState<MetricState> metricStateListState;
    private transient MetricState metricState;
    private final String[] fieldNames;
    private volatile boolean jsonFormat;
    private volatile RowData.FieldGetter[] fieldGetters;
    private String fieldDelimiter;
    private String lineDelimiter;
    private final String tableIdentifier;
    private final LogicalType[] logicalTypes;
    private final DirtyOptions dirtyOptions;
    private @Nullable final DirtySink<Object> dirtySink;

    public DorisSingleOutputFormat(DorisOptions option,
            DorisReadOptions readOptions,
            DorisExecutionOptions executionOptions,
            String tableIdentifier,
            LogicalType[] logicalTypes,
            String[] fieldNames,
            boolean ignoreSingleTableErrors,
            String inlongMetric,
            String auditHostAndPorts,
            DirtyOptions dirtyOptions,
            @Nullable DirtySink<Object> dirtySink) {
        this.options = option;
        this.readOptions = readOptions;
        this.executionOptions = executionOptions;
        this.tableIdentifier = tableIdentifier;
        this.fieldNames = fieldNames;
        this.inlongMetric = inlongMetric;
        this.auditHostAndPorts = auditHostAndPorts;
        this.logicalTypes = logicalTypes;
        this.ignoreSingleTableErrors = ignoreSingleTableErrors;
        this.dirtyOptions = dirtyOptions;
        this.dirtySink = dirtySink;
    }

    public static DorisSingleOutputFormat.Builder builder() {
        return new DorisSingleOutputFormat.Builder();
    }

    /**
     * A builder used to set parameters to the output format's configuration in a fluent way.
     *
     * @return builder
     */

    private void handleStreamLoadProp() {
        Properties props = executionOptions.getStreamLoadProp();
        boolean ifEscape = Boolean.parseBoolean(props.getProperty(ESCAPE_DELIMITERS_KEY, ESCAPE_DELIMITERS_DEFAULT));
        this.fieldDelimiter = props.getProperty(FIELD_DELIMITER_KEY, FIELD_DELIMITER_DEFAULT);
        this.lineDelimiter = props.getProperty(LINE_DELIMITER_KEY, LINE_DELIMITER_DEFAULT);
        if (ifEscape) {
            this.fieldDelimiter = DorisParseUtils.escapeString(fieldDelimiter);
            this.lineDelimiter = DorisParseUtils.escapeString(lineDelimiter);
            props.remove(ESCAPE_DELIMITERS_KEY);
        }

        // add column key when fieldNames is not empty
        if (!props.containsKey(COLUMNS_KEY) && fieldNames != null && fieldNames.length > 0) {
            String columns = Arrays.stream(fieldNames)
                    .map(item -> String.format("`%s`", item.trim().replace("`", "")))
                    .collect(Collectors.joining(","));
            props.put(COLUMNS_KEY, columns);
        }

        // if enable batch delete, the columns must add tag '__DORIS_DELETE_SIGN__'
        String columns = (String) props.get(COLUMNS_KEY);
        if (!columns.contains(DORIS_DELETE_SIGN) && enableBatchDelete()) {
            props.put(COLUMNS_KEY, String.format("%s,%s", columns, DORIS_DELETE_SIGN));
        }
    }

    private boolean enableBatchDelete() {
        try {
            Schema schema = RestService.getSchema(options, readOptions, LOG);
            return executionOptions.getEnableDelete() || UNIQUE_KEYS_TYPE.equals(schema.getKeysType());
        } catch (DorisException e) {
            throw new RuntimeException("Failed fetch doris single table schema: " + options.getTableIdentifier(), e);
        }
    }

    @Override
    public void configure(Configuration configuration) {
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        Properties loadProps = executionOptions.getStreamLoadProp();
        dorisStreamLoad = new DorisStreamLoad(getBackend(), options.getUsername(), options.getPassword(), loadProps);
        this.jsonFormat = FORMAT_JSON_VALUE.equals(executionOptions.getStreamLoadProp().getProperty(FORMAT_KEY));
        handleStreamLoadProp();
        initializeInlongStates();

        if (executionOptions.getBatchIntervalMs() != 0 && executionOptions.getBatchSize() != 1) {
            this.scheduler = new ScheduledThreadPoolExecutor(1,
                    new ExecutorThreadFactory("doris-streamload-output-format"));
            this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(() -> {
                if (!closed && !flushing) {
                    try {
                        flush();
                    } catch (Exception e) {
                        flushException = e;
                    }
                }
            }, executionOptions.getBatchIntervalMs(), executionOptions.getBatchIntervalMs(), TimeUnit.MILLISECONDS);
        }
    }

    private void initializeInlongStates() throws IOException {
        this.fieldGetters = new RowData.FieldGetter[logicalTypes.length];
        for (int i = 0; i < logicalTypes.length; i++) {
            fieldGetters[i] = RowData.createFieldGetter(logicalTypes[i], i);
            if ("DATE".equalsIgnoreCase(logicalTypes[i].toString())) {
                int finalI = i;
                fieldGetters[i] = row -> {
                    if (row.isNullAt(finalI)) {
                        return null;
                    }
                    return DorisParseUtils.epochToDate(row.getInt(finalI));
                };
            }
        }

        MetricOption metricOption = MetricOption.builder()
                .withInlongLabels(inlongMetric)
                .withInlongAudit(auditHostAndPorts)
                .withInitRecords(metricState != null ? metricState.getMetricValue(NUM_RECORDS_OUT) : 0L)
                .withInitBytes(metricState != null ? metricState.getMetricValue(NUM_BYTES_OUT) : 0L)
                .withInitDirtyRecords(metricState != null ? metricState.getMetricValue(DIRTY_RECORDS_OUT) : 0L)
                .withInitDirtyBytes(metricState != null ? metricState.getMetricValue(DIRTY_BYTES_OUT) : 0L)
                .withRegisterMetric(MetricOption.RegisteredMetric.ALL)
                .build();
        if (metricOption != null) {
            metricData = new SinkTableMetricData(metricOption, getRuntimeContext().getMetricGroup());
        }

        if (dirtySink != null) {
            try {
                dirtySink.open(new Configuration());
            } catch (Exception e) {
                throw new IOException(e);
            }
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
        boolean valid = (executionOptions.getBatchSize() > 0 && batch.size() >= executionOptions.getBatchSize())
                || batchBytes >= executionOptions.getMaxBatchBytes();
        if (valid && !flushing) {
            flush();
        }
    }

    @Override
    public synchronized void flush() {
        flushing = true;
        checkFlushException();
        if (batch.isEmpty()) {
            return;
        }
        String result = null;
        if (jsonFormat) {
            if (batch.get(0) instanceof String) {
                result = batch.toString();
            } else {
                try {
                    result = OBJECT_MAPPER.writeValueAsString(batch);
                } catch (Exception e) {
                    handleDirtyData(batch.toString(), DirtyType.DESERIALIZE_ERROR, e);
                }
            }
        } else {
            result = String.join(this.lineDelimiter, batch);
        }
        RespContent respContent = null;
        for (int i = 0; i <= executionOptions.getMaxRetries(); i++) {
            try {
                String[] identifiers = tableIdentifier.split("\\.");
                respContent = dorisStreamLoad.load(identifiers[0], identifiers[1], result);
                try {
                    if (null != metricData && null != respContent) {
                        metricData.invoke(respContent.getNumberLoadedRows(), respContent.getLoadBytes());
                    }
                } catch (Exception e) {
                    LOG.warn("metricData invoke get err:", e);
                }
                writeOutNum.addAndGet(batch.size());
                batch.clear();
                batchBytes = 0;
                break;
            } catch (Exception e) {
                LOG.error(String.format("Flush table: %s error", tableIdentifier), e);
                handleFlushException(respContent, result, e);
            }
        }
        batchBytes = 0;
        LOG.info("Doris sink statistics: readInNum: {}, writeOutNum: {}, errorNum: {}, ddlNum: {}",
                readInNum.get(), writeOutNum.get(), errorNum.get(), ddlNum.get());
        flushing = false;
    }

    private void handleFlushException(RespContent respContent, String result, Exception e) {
        // Makesure it is a dirty data
        if (respContent == null || StringUtils.isNotBlank(respContent.getErrorURL())) {
            flushException = e;
            errorNum.getAndAdd(batch.size());
            for (Object value : batch) {
                try {
                    handleDirtyData(OBJECT_MAPPER.readTree(OBJECT_MAPPER.writeValueAsString(value)),
                            DirtyType.BATCH_LOAD_ERROR, e);
                } catch (IOException ex) {
                    if (!dirtyOptions.ignoreSideOutputErrors()) {
                        throw new RuntimeException(ex);
                    }
                    LOG.warn("Dirty sink failed", ex);
                }
            }
            if (!ignoreSingleTableErrors) {
                throw new RuntimeException(
                        String.format("Writing records to streamload of tableIdentifier:%s failed, the value: %s.",
                                tableIdentifier, result),
                        e);
            }
            errorTables.add(tableIdentifier);
            LOG.warn("The tableIdentifier: {} load failed and the data will be throw away in the future"
                    + " because the option 'sink.multiple.ignore-single-table-errors' is 'true'", tableIdentifier);
        } else {
            throw new RuntimeException(e);
        }
    }

    private void addBatch(T row) {
        if (row instanceof RowData) {
            RowData rowData = (RowData) row;
            Map<String, String> valueMap = new HashMap<>();
            StringJoiner value = new StringJoiner(this.fieldDelimiter);
            for (int i = 0; i < rowData.getArity() && i < fieldGetters.length; ++i) {
                Object field = fieldGetters[i].getFieldOrNull(rowData);
                if (jsonFormat) {
                    String data = field != null ? field.toString() : null;
                    valueMap.put(this.fieldNames[i], data);
                    batchBytes += this.fieldNames[i].getBytes(StandardCharsets.UTF_8).length;
                    if (data != null) {
                        batchBytes += data.getBytes(StandardCharsets.UTF_8).length;
                    }
                } else {
                    String data = field != null ? field.toString() : NULL_VALUE;
                    value.add(data);
                    batchBytes += data.getBytes(StandardCharsets.UTF_8).length;
                }
            }
            // add doris delete sign
            if (enableBatchDelete()) {
                if (jsonFormat) {
                    valueMap.put(DORIS_DELETE_SIGN, parseDeleteSign(rowData.getRowKind()));
                } else {
                    value.add(parseDeleteSign(rowData.getRowKind()));
                }
            }
            Object data = jsonFormat ? valueMap : value.toString();
            batch.add(data);
        } else if (row instanceof String) {
            batchBytes += ((String) row).getBytes(StandardCharsets.UTF_8).length;
            batch.add(row);
        } else {
            LOG.error(String.format("The type of element should be 'RowData' only, raw data: %s", row));
            handleDirtyData(row, DirtyType.UNSUPPORTED_DATA_TYPE,
                    new RuntimeException("The type of element should be 'RowData' only."));
        }
    }

    private void handleDirtyData(Object dirtyData, DirtyType dirtyType, Exception e) {
        errorNum.incrementAndGet();
        if (!dirtyOptions.ignoreDirty()) {
            RuntimeException ex;
            if (e instanceof RuntimeException) {
                ex = (RuntimeException) e;
            } else {
                ex = new RuntimeException(e);
            }
            throw ex;
        }

        if (dirtySink != null) {
            DirtyData.Builder<Object> builder = DirtyData.builder();
            try {
                builder.setData(dirtyData)
                        .setDirtyType(dirtyType)
                        .setLabels(dirtyOptions.getLabels())
                        .setLogTag(dirtyOptions.getLogTag())
                        .setDirtyMessage(e.getMessage())
                        .setIdentifier(dirtyOptions.getIdentifier());
                dirtySink.invoke(builder.build());
            } catch (Exception ex) {
                if (!dirtyOptions.ignoreSideOutputErrors()) {
                    throw new RuntimeException(ex);
                }
                LOG.warn("Dirty sink failed", ex);
            }
        }
        metricData.invokeDirty(1, dirtyData.toString().getBytes(StandardCharsets.UTF_8).length);
    }

    private String getBackend() throws IOException {
        try {
            // get be url from fe
            return RestService.randomBackend(options, readOptions, LOG);
        } catch (IOException | DorisException e) {
            LOG.error("get backends info fail");
            throw new IOException(e);
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
     * Builder for {@link DorisSingleOutputFormat}.
     */
    public static class Builder {

        private final DorisOptions.Builder optionsBuilder;
        private DorisReadOptions readOptions;
        private DorisExecutionOptions executionOptions;
        private boolean ignoreSingleTableErrors;
        private String inlongMetric;
        private String auditHostAndPorts;
        private DataType[] fieldDataTypes;
        private String[] fieldNames;
        private String tableIdentifier;
        private DirtyOptions dirtyOptions;
        private DirtySink<Object> dirtySink;

        public Builder() {
            this.optionsBuilder = DorisOptions.builder().setTableIdentifier("");
        }

        public DorisSingleOutputFormat.Builder setTableIdentifier(String tableIdentifier) {
            this.tableIdentifier = tableIdentifier;
            this.optionsBuilder.setTableIdentifier(tableIdentifier);
            return this;
        }

        public DorisSingleOutputFormat.Builder setFenodes(String fenodes) {
            LOG.info("printing fenodes", fenodes);
            this.optionsBuilder.setFenodes(fenodes);
            return this;
        }

        public DorisSingleOutputFormat.Builder setUsername(String username) {
            this.optionsBuilder.setUsername(username);
            return this;
        }

        public DorisSingleOutputFormat.Builder setPassword(String password) {
            this.optionsBuilder.setPassword(password);
            return this;
        }

        public DorisSingleOutputFormat.Builder setFieldDataTypes(DataType[] fieldDataTypes) {
            this.fieldDataTypes = fieldDataTypes;
            return this;
        }

        public DorisSingleOutputFormat.Builder setFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public DorisSingleOutputFormat.Builder setReadOptions(DorisReadOptions readOptions) {
            this.readOptions = readOptions;
            return this;
        }

        public DorisSingleOutputFormat.Builder setExecutionOptions(DorisExecutionOptions executionOptions) {
            this.executionOptions = executionOptions;
            return this;
        }

        public DorisSingleOutputFormat.Builder setIgnoreSingleTableErrors(boolean ignoreSingleTableErrors) {
            this.ignoreSingleTableErrors = ignoreSingleTableErrors;
            return this;
        }

        public DorisSingleOutputFormat.Builder setInlongMetric(String inlongMetric) {
            this.inlongMetric = inlongMetric;
            return this;
        }

        public DorisSingleOutputFormat.Builder setAuditHostAndPorts(String auditHostAndPorts) {
            this.auditHostAndPorts = auditHostAndPorts;
            return this;
        }

        public DorisSingleOutputFormat.Builder setDirtyOptions(DirtyOptions dirtyOptions) {
            this.dirtyOptions = dirtyOptions;
            return this;
        }

        public DorisSingleOutputFormat.Builder setDirtySink(DirtySink<Object> dirtySink) {
            this.dirtySink = dirtySink;
            return this;
        }

        @SuppressWarnings({"rawtypes"})
        public DorisSingleOutputFormat build() {
            LogicalType[] logicalTypes = Arrays.stream(fieldDataTypes)
                    .map(DataType::getLogicalType).toArray(LogicalType[]::new);

            return new DorisSingleOutputFormat(
                    optionsBuilder.setTableIdentifier(tableIdentifier).build(), readOptions, executionOptions,
                    tableIdentifier, logicalTypes, fieldNames,
                    ignoreSingleTableErrors, inlongMetric, auditHostAndPorts, dirtyOptions, dirtySink);
        }
    }
}
