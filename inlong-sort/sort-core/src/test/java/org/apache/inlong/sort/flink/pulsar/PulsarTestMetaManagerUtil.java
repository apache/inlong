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

package org.apache.inlong.sort.flink.pulsar;

import java.util.HashMap;
import java.util.Map;
import org.apache.inlong.sort.ZkTools;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.protocol.DataFlowInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.deserialization.CsvDeserializationInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.HivePartitionInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.TextFileFormat;
import org.apache.inlong.sort.protocol.source.PulsarSourceInfo;
import org.apache.inlong.sort.util.TestMetaManagerUtil;

public class PulsarTestMetaManagerUtil extends TestMetaManagerUtil {

    public PulsarTestMetaManagerUtil() throws Exception {
        super();
    }

    @Override
    public void addOrUpdateDataFlowInfo(long dataFlowId, String... args) throws Exception {
        ZkTools.addDataFlowToCluster(cluster, dataFlowId, ZOOKEEPER.getConnectString(), zkRoot);
        ZkTools.updateDataFlowInfo(
                prepareDataFlowInfo(dataFlowId, args[0], args[1], args[2], args[3]),
                cluster,
                dataFlowId,
                ZOOKEEPER.getConnectString(),
                zkRoot);
    }

    @Override
    public void initDataFlowInfo(String... args) throws Exception {
        addOrUpdateDataFlowInfo(1L, args[0], args[1], args[2], args[3]);
    }

    @Override
    public DataFlowInfo prepareDataFlowInfo(long dataFlowId, String... args) {

        FieldInfo[] pulsarFields = new FieldInfo[] {
                new FieldInfo("f1", StringFormatInfo.INSTANCE),
                new FieldInfo("f2", StringFormatInfo.INSTANCE)
        };

        Map<String, Object> config = new HashMap<>();
        config.put("pulsar.source.consumer.bootstrap-mode", "earliest");

        return new DataFlowInfo(
                dataFlowId,
                new PulsarSourceInfo(
                        args[0],
                        args[1],
                        args[2],
                        args[3],
                        new CsvDeserializationInfo(','),
                        pulsarFields),
                new HiveSinkInfo(
                        new FieldInfo[0],
                        "testServerJdbcUrl",
                        "testDatabaseName",
                        "testTableName",
                        "testUsername",
                        "testPassword",
                        "testDataPath",
                        new HivePartitionInfo[0],
                        new TextFileFormat(',')),
                config
        );
    }
}
