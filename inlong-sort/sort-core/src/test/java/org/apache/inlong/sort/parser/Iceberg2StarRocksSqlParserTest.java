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

package org.apache.inlong.sort.parser;

import org.apache.inlong.common.pojo.sort.dataflow.field.format.LongFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.StringFormatInfo;
import org.apache.inlong.sort.parser.impl.FlinkSqlParser;
import org.apache.inlong.sort.parser.result.FlinkSqlParseResult;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.GroupInfo;
import org.apache.inlong.sort.protocol.StreamInfo;
import org.apache.inlong.sort.protocol.constant.IcebergConstant;
import org.apache.inlong.sort.protocol.node.Node;
import org.apache.inlong.sort.protocol.node.extract.IcebergExtractNode;
import org.apache.inlong.sort.protocol.node.load.StarRocksLoadNode;
import org.apache.inlong.sort.protocol.transformation.FieldRelation;
import org.apache.inlong.sort.protocol.transformation.relation.NodeRelation;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Iceberg2StarRocksSqlParserTest extends AbstractTestBase {

    private String groupId = "b_test_wk_0801";
    private String streamId = "b_test_wkstream_0801";

    // iceberg
    private String uri = "";
    private String icDatabase = "";
    private String icTable = "";
    private String catalogName = "HIVE";
    private String warehouse = "";

    // starrocks
    private String user = "";
    private String password = "";
    private String jdbc = "";
    private String srDatabase = "";
    private String srTable = "";
    private String primaryKey = "id";
    private String loadUrl = "";

    private List<FieldInfo> fields() {
        return Arrays.asList(
                new FieldInfo("id", new LongFormatInfo()),
                new FieldInfo("name", new StringFormatInfo()),
                new FieldInfo("source", new StringFormatInfo()),
                new FieldInfo("count", new LongFormatInfo()),
                new FieldInfo("remark", new StringFormatInfo()),
                new FieldInfo("send_time", new StringFormatInfo()));
    }

    private List<FieldRelation> relations() {
        return fields().stream()
                .map(info -> new FieldRelation(info, info))
                .collect(Collectors.toList());
    }

    private IcebergExtractNode buildIcebergExtracNode(String id) {

        return new IcebergExtractNode(id, "iceberg-source", fields(), null, uri,
                warehouse, icDatabase, icTable, IcebergConstant.CatalogType.HIVE, catalogName,
                null, null, null);

    }

    private StarRocksLoadNode buildStarRocksLoadNode(String id) {

        Map<String, String> properties = new HashMap<>();
        properties.put("sink.properties.format", "json");
        properties.put("sink.properties.strip_outer_array", "true");
        return new StarRocksLoadNode(id, "sink", fields(), relations(), null, null,
                1, properties, jdbc, loadUrl, user, password, srDatabase,
                srTable, primaryKey, null, null, null, null);
    }

    private NodeRelation buildNodeRelation(List<Node> inputs, List<Node> outputs) {
        List<String> inputIds = inputs.stream().map(Node::getId).collect(Collectors.toList());
        List<String> outputIds = outputs.stream().map(Node::getId).collect(Collectors.toList());
        return new NodeRelation(inputIds, outputIds);
    }

    @Test
    public void testIceberg() throws Exception {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        Node inputNode = buildIcebergExtracNode("1");
        Node outputNode = buildStarRocksLoadNode("2");
        StreamInfo streamInfo = new StreamInfo(streamId, Arrays.asList(inputNode, outputNode),
                Arrays.asList(buildNodeRelation(Collections.singletonList(inputNode),
                        Collections.singletonList(outputNode))));
        GroupInfo groupInfo = new GroupInfo(groupId, Collections.singletonList(streamInfo));

        ObjectMapper objectMapper = new ObjectMapper();
        System.out.println(objectMapper.writeValueAsString(groupInfo));
        FlinkSqlParser parser = FlinkSqlParser.getInstance(tableEnv, groupInfo);
        FlinkSqlParseResult result = (FlinkSqlParseResult) parser.parse();
        Assert.assertTrue(!result.getLoadSqls().isEmpty() && !result.getCreateTableSqls().isEmpty());
    }
}
