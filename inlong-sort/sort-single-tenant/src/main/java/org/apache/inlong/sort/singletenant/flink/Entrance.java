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

package org.apache.inlong.sort.singletenant.flink;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.configuration.Constants;
import org.apache.inlong.sort.protocol.DataFlowInfo;
import org.apache.inlong.sort.protocol.sink.IcebergSinkInfo;
import org.apache.inlong.sort.protocol.sink.SinkInfo;
import org.apache.inlong.sort.protocol.source.PulsarSourceInfo;
import org.apache.inlong.sort.protocol.source.SourceInfo;
import org.apache.inlong.sort.singletenant.flink.utils.CommonUtils;
import org.apache.inlong.sort.util.ParameterTool;

public class Entrance {

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        final Configuration config = parameterTool.getConfiguration();
        final String clusterId = checkNotNull(config.getString(Constants.CLUSTER_ID));
        final DataFlowInfo dataFlowInfo = getDataflowInfoFromFile(config.getString(Constants.DATAFLOW_INFO_FILE));
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checkpoint related
        env.enableCheckpointing(config.getInteger(Constants.CHECKPOINT_INTERVAL_MS));
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(
                config.getInteger(Constants.MIN_PAUSE_BETWEEN_CHECKPOINTS_MS));
        env.getCheckpointConfig().setCheckpointTimeout(config.getInteger(Constants.CHECKPOINT_TIMEOUT_MS));
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        DataStream<Row> sourceStream = buildSourceStream(
                env,
                config,
                dataFlowInfo.getSourceInfo()
        );

        buildSinkStream(
                sourceStream,
                config,
                dataFlowInfo.getSinkInfo());

        env.execute(clusterId);
    }

    private static DataFlowInfo getDataflowInfoFromFile(String fileName) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(new File(fileName), DataFlowInfo.class);
    }

    private static DataStream<Row> buildSourceStream(
            StreamExecutionEnvironment env,
            Configuration config,
            SourceInfo sourceInfo) {
        final String sourceType = checkNotNull(config.getString(Constants.SOURCE_TYPE));
        final int sourceParallelism = config.getInteger(Constants.SOURCE_PARALLELISM);

        DataStream<Row> sourceStream = null;
        if (sourceType.equals(Constants.SOURCE_TYPE_PULSAR)) {
            Preconditions.checkState(sourceInfo instanceof PulsarSourceInfo);
            PulsarSourceInfo pulsarSourceInfo = (PulsarSourceInfo) sourceInfo;

            // TODO : implement pulsar source function
        } else {
            throw new IllegalArgumentException("Unsupported source type " + sourceType);
        }

        return sourceStream;
    }

    private static void buildSinkStream(
            DataStream<Row> sourceStream,
            Configuration config,
            SinkInfo sinkInfo) {
        final String sinkType = checkNotNull(config.getString(Constants.SINK_TYPE));
        final int sinkParallelism = config.getInteger(Constants.SINK_PARALLELISM);

        // TODO : implement sink functions below
        switch (sinkType) {
            case Constants.SINK_TYPE_CLICKHOUSE:
                break;
            case Constants.SINK_TYPE_HIVE:
                break;
            case Constants.SINK_TYPE_ICEBERG:
                Preconditions.checkState(sinkInfo instanceof IcebergSinkInfo);
                IcebergSinkInfo icebergSinkInfo = (IcebergSinkInfo) sinkInfo;
                TableLoader tableLoader = TableLoader.fromHadoopTable(
                        icebergSinkInfo.getTableLocation(),
                        new org.apache.hadoop.conf.Configuration());

                FlinkSink.forRow(sourceStream, CommonUtils.getTableSchema(sinkInfo))
                        .tableLoader(tableLoader)
                        .writeParallelism(sinkParallelism)
                        .build();
                break;
            case Constants.SINK_TYPE_KAFKA:
                break;
            default:
                throw new IllegalArgumentException("Unsupported sink type " + sinkType);
        }

    }

}
