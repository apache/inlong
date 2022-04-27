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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.inlong.sort.formats.common.FloatFormatInfo;
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.LongFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.formats.common.TimestampFormatInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.GroupInfo;
import org.apache.inlong.sort.protocol.StreamInfo;
import org.apache.inlong.sort.protocol.node.Node;
import org.apache.inlong.sort.protocol.node.extract.MySqlExtractNode;
import org.apache.inlong.sort.protocol.node.format.CanalJsonFormat;
import org.apache.inlong.sort.protocol.node.load.HiveLoadNode;
import org.apache.inlong.sort.protocol.node.load.KafkaLoadNode;
import org.apache.inlong.sort.protocol.transformation.FieldRelationShip;
import org.apache.inlong.sort.protocol.transformation.relation.NodeRelationShip;
import org.apache.inlong.sort.singletenant.flink.parser.impl.FlinkSqlParser;
import org.apache.inlong.sort.singletenant.flink.parser.result.FlinkSqlParseResult;
import org.junit.Assert;
import org.junit.Test;

/**
 * Flink sql parser unit test class
 */
public class FlinkSqlParserTest extends AbstractTestBase {

    private MySqlExtractNode buildMySQLExtractNode() {
        List<FieldInfo> fields = Arrays.asList(new FieldInfo("id", new LongFormatInfo()),
                new FieldInfo("name", new StringFormatInfo()),
                new FieldInfo("age", new IntFormatInfo()),
                new FieldInfo("salary", new FloatFormatInfo()),
                new FieldInfo("ts", new TimestampFormatInfo()));
        return new MySqlExtractNode("1", "mysql_input", fields,
                null, null, "id",
                Collections.singletonList("test"), "localhost", "username", "username",
                "test_database", null, null,
                null, null);
    }

    private KafkaLoadNode buildKafkaNode() {
        List<FieldInfo> fields = Arrays.asList(new FieldInfo("id", new LongFormatInfo()),
                new FieldInfo("name", new StringFormatInfo()),
                new FieldInfo("age", new IntFormatInfo()),
                new FieldInfo("salary", new FloatFormatInfo()),
                new FieldInfo("ts", new TimestampFormatInfo()));
        List<FieldRelationShip> relations = Arrays
                .asList(new FieldRelationShip(new FieldInfo("id", new LongFormatInfo()),
                                new FieldInfo("id", new LongFormatInfo())),
                        new FieldRelationShip(new FieldInfo("name", new StringFormatInfo()),
                                new FieldInfo("name", new StringFormatInfo())),
                        new FieldRelationShip(new FieldInfo("age", new IntFormatInfo()),
                                new FieldInfo("age", new IntFormatInfo())),
                        new FieldRelationShip(new FieldInfo("ts", new TimestampFormatInfo()),
                                new FieldInfo("ts", new TimestampFormatInfo()))
                );
        return new KafkaLoadNode("2", "kafka_output", fields, relations, null,
                "topic", "localhost:9092",
                new CanalJsonFormat(), null,
                null, null);
    }

    private NodeRelationShip buildNodeRelation(List<Node> inputs, List<Node> outputs) {
        List<String> inputIds = inputs.stream().map(Node::getId).collect(Collectors.toList());
        List<String> outputIds = outputs.stream().map(Node::getId).collect(Collectors.toList());
        return new NodeRelationShip(inputIds, outputIds);
    }

    private HiveLoadNode buildHiveNode() {
        List<FieldInfo> fields = Arrays.asList(new FieldInfo("id", new LongFormatInfo()),
                new FieldInfo("name", new StringFormatInfo()),
                new FieldInfo("age", new IntFormatInfo()),
                new FieldInfo("salary", new FloatFormatInfo()),
                new FieldInfo("ts", new TimestampFormatInfo()));
        List<FieldRelationShip> relations = Arrays
                .asList(new FieldRelationShip(new FieldInfo("id", new LongFormatInfo()),
                                new FieldInfo("id", new LongFormatInfo())),
                        new FieldRelationShip(new FieldInfo("name", new StringFormatInfo()),
                                new FieldInfo("name", new StringFormatInfo())),
                        new FieldRelationShip(new FieldInfo("age", new IntFormatInfo()),
                                new FieldInfo("age", new IntFormatInfo())),
                        new FieldRelationShip(new FieldInfo("ts", new TimestampFormatInfo()),
                                new FieldInfo("ts", new TimestampFormatInfo()))
                );
        return new HiveLoadNode("2", "hive_output",
                fields, relations, null, 1,
                null, "myCatalog", "myDB", "myTable",
                "/opt/hive/conf/", "3.1.2",
                null, null);
    }

    /**
     * Test flink sql mysql cdc to hive
     *
     * @throws Exception The exception may throws when execute the case
     */
    @Test
    public void testMysqlToHive() throws Exception {
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
        Node outputNode = buildHiveNode();
        StreamInfo streamInfo = new StreamInfo("1L", Arrays.asList(inputNode, outputNode),
                Collections.singletonList(buildNodeRelation(Collections.singletonList(inputNode),
                        Collections.singletonList(outputNode))));
        GroupInfo groupInfo = new GroupInfo("1", Collections.singletonList(streamInfo));
        FlinkSqlParser parser = FlinkSqlParser.getInstance(tableEnv, groupInfo);
        parser.parse();
    }

    /**
     * Test flink sql parse
     *
     * @throws Exception The exception may throws when execute the case
     */
    @Test
    public void testFlinkSqlParse() throws Exception {
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


    @Test
    public void testCanalSinkMetadata() throws Exception {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        tableEnv.executeSql("CREATE TABLE mysql_inlong (\n"
                + "\t`database_name` VARCHAR(63) METADATA FROM 'database_name' VIRTUAL,\n"
                + "\t`table_name` STRING  METADATA FROM 'table_name' VIRTUAL,\n"
                + "\t`sql_type` MAP<STRING, INT> METADATA FROM 'meta.sql_type' VIRTUAL,\n"
                + "\t`pk_names` ARRAY<STRING> METADATA FROM 'meta.pk_names' VIRTUAL,\n"
                + "\t`ingestion_timestamp` TIMESTAMP_LTZ(3) METADATA FROM 'meta.ts' VIRTUAL,\n"
                + "\t`event_timestamp` TIMESTAMP_LTZ(3) METADATA FROM 'meta.op_ts' VIRTUAL, \n"
                + "\t`op_type` STRING METADATA FROM 'meta.op_type' VIRTUAL,\n"
                + "\t`is_ddl` BOOLEAN METADATA FROM 'meta.is_ddl' VIRTUAL,\n"
                + "\t`mysql_type` MAP<STRING. STRING> METADATA FROM 'meta.mysql_type' VIRTUAL,\n"
                + "\t`batch_id` BIGINT METADATA FROM 'meta.batch_id' VIRTUAL,\n"
                + "\t`update_before` ARRAY<MAP<STRING, STRING>> METADATA FROM 'meta.update_before' VIRTUAL\n"
                + "\t`id` BIGINT,\n"
                + "\t`name` STRING,\n"
                + "\tPRIMARY KEY(`id`) NOT ENFORCED\n"
                + ") with (\n"
                + "\t'connector' = 'mysql-cdc-inlong',\n"
                + "\t'hostname' = 'localhost',\n"
                + "\t'username' = 'root',\n"
                + "\t'password' = '123456',\n"
                + "\t'database-name' = 'test',\n"
                + "\t'table-name' = 'mysql_inlong'\n"
                + ")");
        tableEnv.executeSql("CREATE TABLE kafka_inlong (\n"
                + "\t`database` STRING METADATA FROM 'value.database',\n"
                + "\t`id` BIGINT,\n"
                + "\t`name` VARCHAR(63),\n"
                + "\t`table` STRING METADATA FROM 'value.table'\n"
                + ") WITH (\n"
                + "\t'topic' = 'idcard',\n"
                + "\t'connector' = 'kafka-inlong',\n"
                + "\t'properties.bootstrap.servers' = 'localhost:9092',\n"
                + "\t'value.format' = 'canal-json-inlong',\n"
                + "\t'scan.startup.mode' = 'earliest-offset'\n"
                + ")\n");

        tableEnv.executeSql("desc kafka_inlong").print();
        tableEnv.executeSql("insert into kafka_inlong \n"
                + "\tselect \n"
                + "\t\tdatabase_name, id ,name, table_name\n"
                + "\tfrom\n"
                + "\t\tmysql_inlong").await();
        /*tableEnv.executeSql("select * \n"
                + "\tfrom kafka_inlong").print();*/
    }
}
