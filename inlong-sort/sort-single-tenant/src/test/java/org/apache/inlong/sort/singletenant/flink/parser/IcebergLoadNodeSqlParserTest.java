/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.inlong.sort.singletenant.flink.parser;

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
import org.apache.inlong.sort.protocol.node.load.IcebergLoadNode;
import org.apache.inlong.sort.protocol.transformation.FieldRelationShip;
import org.apache.inlong.sort.protocol.transformation.FilterFunction;
import org.apache.inlong.sort.protocol.transformation.StringConstantParam;
import org.apache.inlong.sort.protocol.transformation.function.SingleValueFilterFunction;
import org.apache.inlong.sort.protocol.transformation.operator.EmptyOperator;
import org.apache.inlong.sort.protocol.transformation.operator.EqualOperator;
import org.apache.inlong.sort.protocol.transformation.relation.NodeRelationShip;
import org.apache.inlong.sort.singletenant.flink.parser.impl.FlinkSqlParser;
import org.apache.inlong.sort.singletenant.flink.parser.result.FlinkSqlParseResult;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IcebergLoadNodeSqlParserTest extends AbstractTestBase {
    private MySqlExtractNode buildMySQLExtractNode(String id) {
        List<FieldInfo> fields = Arrays.asList(new FieldInfo("id", new LongFormatInfo()),
                new FieldInfo("name", new StringFormatInfo()),
                new FieldInfo("age", new IntFormatInfo()),
                new FieldInfo("salary", new FloatFormatInfo()),
                new FieldInfo("ts", new TimestampFormatInfo()),
                new FieldInfo("event_type", new StringFormatInfo()));
        //if you hope hive load mode of append,please add this config.
        Map<String, String> map = new HashMap<>();
        map.put("append-mode", "true");
        return new MySqlExtractNode(id, "mysql_input", fields,
                null, map, "id",
                Collections.singletonList("work1"), "localhost", "root", "123456",
                "inlong", null, null,
                null, null);
    }

    private IcebergLoadNode buildIcebergLoadNodeWithHadoopCatalog() {
        List<FieldInfo> fields = Arrays.asList(new FieldInfo("id", new LongFormatInfo()),
                new FieldInfo("name", new StringFormatInfo()),
                new FieldInfo("salary", new StringFormatInfo()),
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
        List<FilterFunction> filters = Arrays.asList(new SingleValueFilterFunction(EmptyOperator.getInstance(),
                new FieldInfo("name", new StringFormatInfo()),
                EqualOperator.getInstance(), new StringConstantParam("yunqingmo")));

        Map<String, String> props = new HashMap<>();
        props.put("catalog-type", "hadoop");
        props.put("catalog-name", "hadoop_prod");
        props.put("warehouse", "hdfs://localhost:9000/iceberg/warehouse");
        IcebergLoadNode node = new IcebergLoadNode("iceberg", "iceberg_output", fields, relations,
                filters, null, props, "inlong", "inlong_iceberg");
        return node;
    }

    private IcebergLoadNode buildIcebergLoadNodeWithHiveCatalog() {
        List<FieldInfo> fields = Arrays.asList(new FieldInfo("id", new LongFormatInfo()),
                new FieldInfo("name", new StringFormatInfo()),
                new FieldInfo("age", new IntFormatInfo()),
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

        // set HIVE_CONF_DIR,or set uri and warehouse
        Map<String, String> props = new HashMap<>();
        props.put("catalog-type", "hive");
        props.put("catalog-name", "hive_prod");
        props.put("catalog-database", "default");
        props.put("uri", "thrift://localhost:9083");
        // props.put("warehouse", "/hive/warehouse");
        // should enable iceberg.engine.hive.enabled in hive-site.xml
        IcebergLoadNode node = new IcebergLoadNode("iceberg", "iceberg_output", fields, relations,
                null, null, props, "inlong", "inlong_iceberg");
        return node;
    }

    /**
     * build node relation
     * @param inputs extract node
     * @param outputs load node
     * @return node relation
     */
    private NodeRelationShip buildNodeRelation(List<Node> inputs, List<Node> outputs) {
        List<String> inputIds = inputs.stream().map(Node::getId).collect(Collectors.toList());
        List<String> outputIds = outputs.stream().map(Node::getId).collect(Collectors.toList());
        return new NodeRelationShip(inputIds, outputIds);
    }

    @Test
    public void testIceberg() throws Exception {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        Node inputNode = buildMySQLExtractNode("1");
        Node outputNode = buildIcebergLoadNodeWithHiveCatalog();
        StreamInfo streamInfo = new StreamInfo("1L", Arrays.asList(inputNode, outputNode),
                Arrays.asList(
                        buildNodeRelation(Collections.singletonList(inputNode),
                                Collections.singletonList(outputNode))));
        GroupInfo groupInfo = new GroupInfo("group_id", Collections.singletonList(streamInfo));
        FlinkSqlParser parser = FlinkSqlParser.getInstance(tableEnv, groupInfo);
        FlinkSqlParseResult result = parser.parse();
        System.out.println(result);
    }
}
