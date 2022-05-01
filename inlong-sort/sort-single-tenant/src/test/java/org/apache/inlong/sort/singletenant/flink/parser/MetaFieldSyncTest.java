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

package org.apache.inlong.sort.singletenant.flink.parser;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.inlong.sort.formats.common.ArrayFormatInfo;
import org.apache.inlong.sort.formats.common.BooleanFormatInfo;
import org.apache.inlong.sort.formats.common.FloatFormatInfo;
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.LongFormatInfo;
import org.apache.inlong.sort.formats.common.MapFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.formats.common.TimestampFormatInfo;
import org.apache.inlong.sort.protocol.BuiltInFieldInfo;
import org.apache.inlong.sort.protocol.BuiltInFieldInfo.BuiltInField;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.GroupInfo;
import org.apache.inlong.sort.protocol.StreamInfo;
import org.apache.inlong.sort.protocol.node.Node;
import org.apache.inlong.sort.protocol.node.extract.MySqlExtractNode;
import org.apache.inlong.sort.protocol.node.format.JsonFormat;
import org.apache.inlong.sort.protocol.node.load.KafkaLoadNode;
import org.apache.inlong.sort.protocol.transformation.FieldRelationShip;
import org.apache.inlong.sort.protocol.transformation.FilterFunction;
import org.apache.inlong.sort.protocol.transformation.StringConstantParam;
import org.apache.inlong.sort.protocol.transformation.function.SingleValueFilterFunction;
import org.apache.inlong.sort.protocol.transformation.operator.EmptyOperator;
import org.apache.inlong.sort.protocol.transformation.operator.EqualOperator;
import org.apache.inlong.sort.protocol.transformation.relation.NodeRelationShip;
import org.apache.inlong.sort.singletenant.flink.parser.impl.FlinkSqlParser;
import org.apache.inlong.sort.singletenant.flink.parser.result.FlinkSqlParseResult;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Test for mysql meta field
 */
public class MetaFieldSyncTest extends AbstractTestBase {

    private MySqlExtractNode buildMySQLExtractNode() {
        List<FieldInfo> fields = Arrays.asList(new FieldInfo("id", new LongFormatInfo()),
                new FieldInfo("name", new StringFormatInfo()),
                new FieldInfo("age", new IntFormatInfo()),
                new FieldInfo("salary", new FloatFormatInfo()),
                new FieldInfo("ts", new TimestampFormatInfo()),
                new BuiltInFieldInfo("database", new TimestampFormatInfo(), BuiltInField.MYSQL_METADATA_DATABASE),
                new BuiltInFieldInfo("table", new TimestampFormatInfo(), BuiltInField.MYSQL_METADATA_TABLE),
                new BuiltInFieldInfo("pk_names", new TimestampFormatInfo(), BuiltInField.METADATA_PK_NAMES),
                new BuiltInFieldInfo("event_time", new TimestampFormatInfo(), BuiltInField.MYSQL_METADATA_EVENT_TIME),
                new BuiltInFieldInfo("event_type", new TimestampFormatInfo(), BuiltInField.MYSQL_METADATA_EVENT_TYPE),
                new BuiltInFieldInfo("isddl", new TimestampFormatInfo(), BuiltInField.MYSQL_METADATA_IS_DDL),
                new BuiltInFieldInfo("batch_id", new TimestampFormatInfo(), BuiltInField.METADATA_BATCH_ID),
                new BuiltInFieldInfo("mysql_type", new TimestampFormatInfo(), BuiltInField.METADATA_MYSQL_TYPE),
                new BuiltInFieldInfo("sql_type", new TimestampFormatInfo(), BuiltInField.METADATA_SQL_TYPE),
                new BuiltInFieldInfo("meta_ts", new TimestampFormatInfo(), BuiltInField.METADATA_TS),
                new BuiltInFieldInfo("up_before", new TimestampFormatInfo(), BuiltInField.METADATA_UPDATE_BEFORE)
        );
        return new MySqlExtractNode("1", "mysql_input", fields, null, null, "id",
                Arrays.asList("sort"), "localhost", "inlong", "password",
                "test", null, null, null, null);
    }

    private KafkaLoadNode buildKafkaNode() {
        List<FieldInfo> fields = Arrays.asList(new FieldInfo("id", new LongFormatInfo()),
                new FieldInfo("name", new StringFormatInfo()),
                new FieldInfo("age", new IntFormatInfo()),
                new FieldInfo("salary", new FloatFormatInfo()),
                new FieldInfo("ts", new TimestampFormatInfo()),
                new FieldInfo("database", new StringFormatInfo()),
                new FieldInfo("table", new StringFormatInfo()),
                new FieldInfo("pk_names", new ArrayFormatInfo(new StringFormatInfo())),
                new FieldInfo("event_time", new TimestampFormatInfo()),
                new FieldInfo("event_type", new StringFormatInfo()),
                new FieldInfo("isddl", new BooleanFormatInfo()),
                new FieldInfo("batch_id", new LongFormatInfo()),
                new FieldInfo("mysql_type", new MapFormatInfo(new StringFormatInfo(), new StringFormatInfo())),
                new FieldInfo("sql_type", new MapFormatInfo(new StringFormatInfo(), new IntFormatInfo())),
                new FieldInfo("meta_ts", new TimestampFormatInfo()),
                new FieldInfo("up_before",
                        new ArrayFormatInfo(new MapFormatInfo(new StringFormatInfo(), new StringFormatInfo())))
        );
        List<FieldRelationShip> relations = Arrays
                .asList(new FieldRelationShip(new FieldInfo("id", new LongFormatInfo()),
                                new FieldInfo("id", new LongFormatInfo())),
                        new FieldRelationShip(new FieldInfo("name", new StringFormatInfo()),
                                new FieldInfo("name", new StringFormatInfo())),
                        new FieldRelationShip(new FieldInfo("age", new IntFormatInfo()),
                                new FieldInfo("age", new IntFormatInfo())),
                        new FieldRelationShip(new FieldInfo("ts", new TimestampFormatInfo()),
                                new FieldInfo("ts", new TimestampFormatInfo())),
                        new FieldRelationShip(new FieldInfo("database", new TimestampFormatInfo()),
                                new FieldInfo("database", new TimestampFormatInfo())),
                        new FieldRelationShip(new FieldInfo("table", new TimestampFormatInfo()),
                                new FieldInfo("table", new TimestampFormatInfo())),
                        new FieldRelationShip(new FieldInfo("pk_names", new TimestampFormatInfo()),
                                new FieldInfo("pk_names", new TimestampFormatInfo())),
                        new FieldRelationShip(new FieldInfo("event_time", new TimestampFormatInfo()),
                                new FieldInfo("event_time", new TimestampFormatInfo())),
                        new FieldRelationShip(new FieldInfo("event_type", new TimestampFormatInfo()),
                                new FieldInfo("event_type", new TimestampFormatInfo())),
                        new FieldRelationShip(new FieldInfo("isddl", new TimestampFormatInfo()),
                                new FieldInfo("isddl", new TimestampFormatInfo())),
                        new FieldRelationShip(new FieldInfo("batch_id", new TimestampFormatInfo()),
                                new FieldInfo("batch_id", new TimestampFormatInfo())),
                        new FieldRelationShip(new FieldInfo("mysql_type", new TimestampFormatInfo()),
                                new FieldInfo("mysql_type", new TimestampFormatInfo())),
                        new FieldRelationShip(new FieldInfo("sql_type", new TimestampFormatInfo()),
                                new FieldInfo("sql_type", new TimestampFormatInfo())),
                        new FieldRelationShip(new FieldInfo("meta_ts", new TimestampFormatInfo()),
                                new FieldInfo("meta_ts", new TimestampFormatInfo())),
                        new FieldRelationShip(new FieldInfo("up_before", new TimestampFormatInfo()),
                                new FieldInfo("up_before", new TimestampFormatInfo()))
                );
        List<FilterFunction> filters = Arrays.asList(new SingleValueFilterFunction(EmptyOperator.getInstance(),
                new FieldInfo("name", new StringFormatInfo()),
                EqualOperator.getInstance(), new StringConstantParam("test")));
        KafkaLoadNode node = new KafkaLoadNode("2", "kafka_output2", fields, relations,
                null, "worker123", "localhost:9092",
                new JsonFormat(), null,
                null, "id");
        return node;
    }

    public NodeRelationShip buildNodeRelation(List<Node> inputs, List<Node> outputs) {
        List<String> inputIds = inputs.stream().map(Node::getId).collect(Collectors.toList());
        List<String> outputIds = outputs.stream().map(Node::getId).collect(Collectors.toList());
        return new NodeRelationShip(inputIds, outputIds);
    }

    /**
     * Test meta field sync test
     *
     * @throws Exception The exception may throws when execute the case
     */
    @Test
    public void testMetaFieldSyncTest() throws Exception {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        Node inputNode = buildMySQLExtractNode();
        Node outputNode = buildKafkaNode();
        StreamInfo streamInfo = new StreamInfo("1", Arrays.asList(inputNode, outputNode),
                Collections.singletonList(buildNodeRelation(Collections.singletonList(inputNode),
                        Collections.singletonList(outputNode))));
        GroupInfo groupInfo = new GroupInfo("1", Collections.singletonList(streamInfo));
        FlinkSqlParser parser = FlinkSqlParser.getInstance(tableEnv, groupInfo);
        FlinkSqlParseResult result = parser.parse();
        Assert.assertTrue(result.tryExecute());
    }
}
