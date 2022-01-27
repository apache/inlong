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

package org.apache.inlong.sort.singletenant.flink.clickhouse;

import com.google.common.base.Preconditions;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.flink.clickhouse.ClickHouseSinkFunction;
import org.apache.inlong.sort.protocol.sink.ClickHouseSinkInfo;

public class ClickhouseRowSinkFunction extends RichSinkFunction<Row> implements CheckpointedFunction {

    private final ClickHouseSinkFunction clickHouseSinkFunction;

    public ClickhouseRowSinkFunction(ClickHouseSinkInfo clickHouseSinkInfo) {
        this.clickHouseSinkFunction = new ClickHouseSinkFunction(Preconditions.checkNotNull(clickHouseSinkInfo));
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        clickHouseSinkFunction.snapshotState(functionSnapshotContext);
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        clickHouseSinkFunction.initializeState(functionInitializationContext);
    }

    @Override
    public void invoke(Row value, Context context) throws Exception {
        // Sort doesn't support retraction currently, so just set the flag to false.
        clickHouseSinkFunction.invoke(Tuple2.of(false, value), context);
    }
}
