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

package org.apache.inlong.sort.iceberg.sink.multiple;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.flink.sink.TaskWriterFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.inlong.sort.base.dirty.DirtyOptions;
import org.apache.inlong.sort.base.dirty.sink.DirtySink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;

public class IcebergSingleStreamWriter<T> extends IcebergProcessFunction<T, WriteResult>
        implements
            CheckpointedFunction,
            SchemaEvolutionFunction<TaskWriterFactory<T>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(IcebergSingleStreamWriter.class);

    private static final long serialVersionUID = 1L;

    private final String fullTableName;
    private TaskWriterFactory<T> taskWriterFactory;

    private transient TaskWriter<T> writer;
    private transient int subTaskId;
    private transient int attemptId;
    private @Nullable RowType flinkRowType;

    public IcebergSingleStreamWriter(
            String fullTableName,
            TaskWriterFactory<T> taskWriterFactory,
            String inlongMetric,
            String auditHostAndPorts,
            @Nullable RowType flinkRowType,
            DirtyOptions dirtyOptions,
            @Nullable DirtySink<Object> dirtySink) {
        this.fullTableName = fullTableName;
        this.taskWriterFactory = taskWriterFactory;
        this.flinkRowType = flinkRowType;
    }

    public RowType getFlinkRowType() {
        return flinkRowType;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.subTaskId = getRuntimeContext().getIndexOfThisSubtask();
        this.attemptId = getRuntimeContext().getAttemptNumber();

        // Initialize the task writer factory.
        this.taskWriterFactory.initialize(subTaskId, attemptId);
        // Initialize the task writer.
        this.writer = taskWriterFactory.create();
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        // close all open files and emit files to downstream committer operator
        emit(writer.complete());
        this.writer = taskWriterFactory.create();
    }

    @Override
    public void processElement(T value) throws Exception {
        writer.write(value);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
    }

    public void setFlinkRowType(@Nullable RowType flinkRowType) {
        this.flinkRowType = flinkRowType;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
    }

    @Override
    public void dispose() throws Exception {
        if (writer != null) {
            writer.close();
            writer = null;
        }
    }

    @Override
    public void endInput() throws IOException {
        // For bounded stream, it may don't enable the checkpoint mechanism so we'd better to emit the remaining
        // completed files to downstream before closing the writer so that we won't miss any of them.
        emit(writer.complete());
    }

    @Override
    public void schemaEvolution(TaskWriterFactory<T> schema) throws IOException {
        emit(writer.complete());

        taskWriterFactory = schema;
        taskWriterFactory.initialize(subTaskId, attemptId);
        writer = taskWriterFactory.create();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("table_name", fullTableName)
                .add("subtask_id", subTaskId)
                .add("attempt_id", attemptId)
                .toString();
    }

    private void emit(WriteResult result) {
        collector.collect(result);
    }
}
