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

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.table.DorisDynamicOutputFormat;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.inlong.sort.base.metric.MetricOption;
import org.apache.inlong.sort.base.metric.MetricState;
import org.apache.inlong.sort.base.metric.SinkMetricData;
import org.apache.inlong.sort.base.util.MetricStateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.apache.inlong.sort.base.Constants.INLONG_METRIC_STATE_NAME;
import static org.apache.inlong.sort.base.Constants.NUM_BYTES_OUT;
import static org.apache.inlong.sort.base.Constants.NUM_RECORDS_OUT;

/**
 * DorisSingleTableFormat extends {org.apache.doris.flink.table.DorisDynamicOutputFormat}
 * It is used for single table doris jobs, enhanced the original class to support metric and
 * snapshot
 */
public class DorisSingleTableFormat<T> extends DorisDynamicOutputFormat<T> implements DorisOutputFormat<T> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DorisSingleTableFormat.class);
    private final String inlongMetric;
    private final String auditHostAndPorts;
    private transient SinkMetricData metricData;
    private transient ListState<MetricState> metricStateListState;
    private transient MetricState metricState;
    private volatile long datasize;

    public DorisSingleTableFormat(DorisOptions option,
            DorisReadOptions readOptions,
            DorisExecutionOptions executionOptions,
            LogicalType[] logicalTypes,
            String[] fieldNames,
            String inlongMetric,
            String auditHostAndPorts) {
        super(option, readOptions, executionOptions, logicalTypes, fieldNames);
        this.inlongMetric = inlongMetric;
        this.auditHostAndPorts = auditHostAndPorts;
    }

    public static SingleTableFormatBuilder builder() {
        return new SingleTableFormatBuilder();
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        super.open(taskNumber, numTasks);
        MetricOption metricOption;
        metricOption = MetricOption.builder()
                .withInlongLabels(inlongMetric)
                .withInlongAudit(auditHostAndPorts)
                .withInitRecords(metricState != null ? metricState.getMetricValue(NUM_RECORDS_OUT) : 0L)
                .withInitBytes(metricState != null ? metricState.getMetricValue(NUM_BYTES_OUT) : 0L)
                .withRegisterMetric(MetricOption.RegisteredMetric.ALL)
                .build();

        if (metricOption != null) {
            metricData = new SinkMetricData(metricOption, getRuntimeContext().getMetricGroup());
        }
    }

    @Override
    public synchronized void writeRecord(T row) throws IOException {
        super.writeRecord(row);
        try {
            if (metricData != null) {
                metricData.invokeWithEstimate(row);
                datasize++;
                LOG.info("written {} rows with {} bites", datasize,
                        row.toString().getBytes(StandardCharsets.UTF_8).length);
            }
        } catch (Exception e) {
            LOG.warn("metricData invoke get err:", e);
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

    public static class SingleTableFormatBuilder extends DorisDynamicOutputFormat.Builder {

        private final org.apache.doris.flink.cfg.DorisOptions.Builder optionsBuilder = DorisOptions.builder();
        private DorisReadOptions readOptions;
        private DorisExecutionOptions executionOptions;
        private DataType[] fieldDataTypes;
        private String[] fieldNames;
        private String inlongMetric;
        private String auditHostAndPorts;

        public SingleTableFormatBuilder() {
        }

        public SingleTableFormatBuilder setFenodes(String fenodes) {
            this.optionsBuilder.setFenodes(fenodes);
            return this;
        }

        public SingleTableFormatBuilder setUsername(String username) {
            this.optionsBuilder.setUsername(username);
            return this;
        }

        public SingleTableFormatBuilder setPassword(String password) {
            this.optionsBuilder.setPassword(password);
            return this;
        }

        public SingleTableFormatBuilder setTableIdentifier(String tableIdentifier) {
            this.optionsBuilder.setTableIdentifier(tableIdentifier);
            return this;
        }

        public SingleTableFormatBuilder setReadOptions(DorisReadOptions readOptions) {
            this.readOptions = readOptions;
            return this;
        }

        public SingleTableFormatBuilder setExecutionOptions(DorisExecutionOptions executionOptions) {
            this.executionOptions = executionOptions;
            return this;
        }

        public SingleTableFormatBuilder setFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public SingleTableFormatBuilder setFieldDataTypes(DataType[] fieldDataTypes) {
            this.fieldDataTypes = fieldDataTypes;
            return this;
        }

        public SingleTableFormatBuilder setInlongMetric(String inlongMetric) {
            this.inlongMetric = inlongMetric;
            return this;
        }

        public SingleTableFormatBuilder setAuditHostAndPorts(String auditHostAndPorts) {
            this.auditHostAndPorts = auditHostAndPorts;
            return this;
        }

        @SuppressWarnings({"rawtypes"})
        public DorisSingleTableFormat build() {
            LogicalType[] logicalTypes = Arrays.stream(this.fieldDataTypes)
                    .map(DataType::getLogicalType).toArray((x$0) -> new LogicalType[x$0]);
            return new DorisSingleTableFormat(
                    this.optionsBuilder.build(),
                    this.readOptions,
                    this.executionOptions,
                    logicalTypes,
                    this.fieldNames,
                    this.inlongMetric,
                    this.auditHostAndPorts);
        }
    }
}