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

import org.apache.inlong.common.pojo.sort.dataflow.field.format.IntFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.StringFormatInfo;
import org.apache.inlong.sort.parser.impl.FlinkSqlParser;
import org.apache.inlong.sort.parser.result.ParseResult;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.GroupInfo;
import org.apache.inlong.sort.protocol.StreamInfo;
import org.apache.inlong.sort.protocol.node.Node;
import org.apache.inlong.sort.protocol.node.extract.MySqlExtractNode;
import org.apache.inlong.sort.protocol.node.load.OceanBaseLoadNode;
import org.apache.inlong.sort.protocol.transformation.FieldRelation;
import org.apache.inlong.sort.protocol.transformation.relation.NodeRelation;

import lombok.extern.slf4j.Slf4j;
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

/**
 * Test mysql cdc for {@link OceanBaseLoadNode}
 */
@Slf4j
public class MySqlExtractNodeToOceanBaseLoadNodeTest extends AbstractTestBase {

    private MySqlExtractNode buildMysqlExtractNode() {
        List<FieldInfo> fields = Arrays.asList(
                new FieldInfo("id", new IntFormatInfo()),
                new FieldInfo("user_name", new StringFormatInfo()),
                new FieldInfo("phone", new StringFormatInfo()));
        Map<String, String> map = new HashMap<>();
        return new MySqlExtractNode("1", "mysql_input", fields,
                null, map, "id",
                Collections.singletonList("t_ds_user"), "localhost", "root", "root",
                "dolphinscheduler", 3308, 10011,
                true, null);
    }

    private OceanBaseLoadNode buildMysqlLoadNode() {
        final List<FieldInfo> fields = Arrays.asList(
                new FieldInfo("id", new IntFormatInfo()),
                new FieldInfo("user_name", new StringFormatInfo()),
                new FieldInfo("phone", new StringFormatInfo()));

        final List<FieldRelation> fieldRelations = Arrays
                .asList(new FieldRelation(new FieldInfo("id", new IntFormatInfo()),
                        new FieldInfo("id", new IntFormatInfo())),
                        new FieldRelation(new FieldInfo("user_name", new StringFormatInfo()),
                                new FieldInfo("user_name", new StringFormatInfo())),
                        new FieldRelation(new FieldInfo("phone", new IntFormatInfo()),
                                new FieldInfo("phone", new IntFormatInfo())));

        Map<String, String> properties = new HashMap<>();
        properties.put("dirty.side-output.connector", "log");
        properties.put("dirty.ignore", "true");
        properties.put("dirty.side-output.enable", "true");
        properties.put("dirty.side-output.format", "csv");
        properties.put("dirty.side-output.labels",
                "SYSTEM_TIME=${SYSTEM_TIME}&DIRTY_TYPE=${DIRTY_TYPE}&database=inlong&table=inlong_oceanbase");
        return new OceanBaseLoadNode("2", "mysql_output", fields, fieldRelations, null,
                null, null, properties, "jdbc:mysql://localhost:2883/test",
                "root", "123456", "t_ds_user", "id");
    }

    /**
     * build node relation
     *
     * @param inputs  extract node
     * @param outputs load node
     * @return node relation
     */
    private NodeRelation buildNodeRelation(List<Node> inputs, List<Node> outputs) {
        List<String> inputIds = inputs.stream().map(Node::getId).collect(Collectors.toList());
        List<String> outputIds = outputs.stream().map(Node::getId).collect(Collectors.toList());
        return new NodeRelation(inputIds, outputIds);
    }

    /**
     * Test flink sql task for extract is mysql {@link MySqlExtractNode} and load is oceanbase {@link OceanBaseLoadNode}
     */
    @Test
    public void testMySqlExtractNodeToOceanBaseLoadNodeSqlParse() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);
        env.disableOperatorChaining();
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        Node inputNode = buildMysqlExtractNode();
        Node outputNode = buildMysqlLoadNode();
        StreamInfo streamInfo = new StreamInfo("1", Arrays.asList(inputNode, outputNode),
                Collections.singletonList(buildNodeRelation(Collections.singletonList(inputNode),
                        Collections.singletonList(outputNode))));
        GroupInfo groupInfo = new GroupInfo("1", Collections.singletonList(streamInfo));
        FlinkSqlParser parser = FlinkSqlParser.getInstance(tableEnv, groupInfo);
        ParseResult result = parser.parse();
        try {
            Assert.assertTrue(result.tryExecute());
        } catch (Exception e) {
            log.error("An exception occurred: {}", e.getMessage());
        }
    }
}