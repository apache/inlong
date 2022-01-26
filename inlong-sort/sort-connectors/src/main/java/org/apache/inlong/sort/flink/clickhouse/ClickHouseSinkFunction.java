/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.flink.clickhouse;

import com.google.common.base.Preconditions;
import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.inlong.sort.flink.clickhouse.output.AbstractClickHouseOutputFormat;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.sink.ClickHouseSinkInfo;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.flink.clickhouse.output.ClickHouseOutputFormatFactory;

public class ClickHouseSinkFunction extends RichSinkFunction<Tuple2<Boolean, Row>> implements CheckpointedFunction {

    private static final long serialVersionUID = 2738357054183678956L;

    private final ClickHouseSinkInfo clickHouseSinkInfo;

    private transient AbstractClickHouseOutputFormat outputFormat;

    public ClickHouseSinkFunction(
            ClickHouseSinkInfo clickHouseSinkInfo) {
        this.clickHouseSinkInfo = Preconditions.checkNotNull(clickHouseSinkInfo);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        outputFormat.flush();
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) {
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        FieldInfo[] fields = clickHouseSinkInfo.getFields();
        int fieldsLength = fields.length;
        String[] fieldNames = new String[fieldsLength];
        FormatInfo[] formatInfos = new FormatInfo[fieldsLength];
        for (int i = 0; i < fieldsLength; ++i) {
            FieldInfo field = fields[i];
            fieldNames[i] = field.getName();
            formatInfos[i] = field.getFormatInfo();
        }

        outputFormat = ClickHouseOutputFormatFactory
                               .generateClickHouseOutputFormat(fieldNames, formatInfos, clickHouseSinkInfo);
        RuntimeContext ctx = getRuntimeContext();
        outputFormat.setRuntimeContext(ctx);
        outputFormat.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());
    }

    @Override
    public void close() throws Exception {
        outputFormat.close();
    }

    @Override
    public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
        outputFormat.writeRecord(value);
    }
}
