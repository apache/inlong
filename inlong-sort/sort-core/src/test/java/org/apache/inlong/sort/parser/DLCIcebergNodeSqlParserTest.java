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

package org.apache.inlong.sort.parser;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.inlong.sort.formats.common.FloatFormatInfo;
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.LongFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.formats.common.TimestampFormatInfo;
import org.apache.inlong.sort.parser.impl.FlinkSqlParser;
import org.apache.inlong.sort.parser.result.FlinkSqlParseResult;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.GroupInfo;
import org.apache.inlong.sort.protocol.StreamInfo;
import org.apache.inlong.sort.protocol.constant.IcebergConstant.CatalogType;
import org.apache.inlong.sort.protocol.node.Node;
import org.apache.inlong.sort.protocol.node.extract.MySqlExtractNode;
import org.apache.inlong.sort.protocol.node.load.DLCIcebergLoadNode;
import org.apache.inlong.sort.protocol.node.load.IcebergLoadNode;
import org.apache.inlong.sort.protocol.transformation.FieldRelationShip;
import org.apache.inlong.sort.protocol.transformation.relation.NodeRelationShip;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DLCIcebergNodeSqlParserTest {
    private MySqlExtractNode buildMySQLExtractNode(String id) {
        List<FieldInfo> fields = Arrays.asList(
                new FieldInfo("id", new IntFormatInfo()),
                new FieldInfo("name", new StringFormatInfo()));
        // if you hope hive load mode of append, please add this config
        Map<String, String> map = new HashMap<>();
        map.put("append-mode", "true");
        return new MySqlExtractNode(id,
                "mysql_input",
                fields,
                null,
                map,
                "id",
                Collections.singletonList("test_for_mysql_extract"),
                "localhost",
                "root",
                "123456",
                "inlong",
                3306,
                null,
                null,
                null);
    }


    private DLCIcebergLoadNode buildDLCLoadNode() {
        List<FieldInfo> fields = Arrays.asList(
                new FieldInfo("id", new IntFormatInfo()),
                new FieldInfo("name", new StringFormatInfo()));
        List<FieldRelationShip> relations = Arrays
                .asList(new FieldRelationShip(new FieldInfo("id", new IntFormatInfo()),
                                new FieldInfo("id", new IntFormatInfo())),
                        new FieldRelationShip(new FieldInfo("name", new StringFormatInfo()),
                                new FieldInfo("name", new StringFormatInfo())));

        // set HIVE_CONF_DIR,or set uri and warehouse
        Map<String, String> properties = new HashMap<>();
        properties.put("qcloud.dlc.secret-id", "AKIDGMCl47tOedLnysWtzoMdeOr40f4GBoAG");
        properties.put("qcloud.dlc.secret-key", "rQikVzC8oDLCn9E3btue7KGwLUMxHK7d");
        properties.put("qcloud.dlc.region", "ap-beijing");

        properties.put("fs.cosn.impl", "org.apache.hadoop.fs.CosFileSystem");
        properties.put("fs.lakefs.impl", "org.apache.hadoop.fs.lakefs.LakeFileSystem");
        properties.put("fs.cosn.userinfo.region", "ap-beijing");
        properties.put("fs.cosn.userinfo.secretId", "AKID_-Wydb631G1EBjckoPPemAvbYFjAVtZXR69uFcFbB5dSU5d0ECSKRxqfklJnVlXP");
        properties.put("fs.cosn.userinfo.secretKey", "Ta27Bzf4/NVR6iF+jSfBL0jWbbWNC87bGIqZgUPhqLE=");
        properties.put("fs.cosn.userinfo.sessionToken", "1u2ECOq8FbsmReLKJGyONyP3nYWiFuuacbf7f6928000a870b2905f6e4cf9b308zRFG9Y48snqZ9bVSHof6hvtGD6WZQ6RelrBpuGg3Q1mfwn_-g0zrIsK13DszSclp4yGfnXK_kFm3EkVJxMq8ZiBzs09VrV_XbeJtGSIJ8iQP3fAv9RZpUBJRjEz8L2YWbQW3uTXoSd_nVs98Qnp5kKnGqm9URXWv4ZgZbioBWfkwAO7PpoYy3vPdiLq-bzhJyvz6_gvZv4_TCh9KZOT93IEq-Qkb9TvDuSnInrlBCoZbtgtlT4lFMvAQtvffHY9o-HXzDU7InSqjY95WChCQ6Ryfv0JWVNd4cZI8QDjuz9sSNtx6W5kzRGgw30f3FZPcs8iocDnAGnxGsShPa2pjh7Ls_1nAYqOSft6_D6Q295xyz6_BIIZ-Cu1Fnr-X8HPXjm02B6ADsuCg8X5XTx5EE9-QrZptthm600ssGl7Dl7tK1G-G-QvJ_A44CPuEnUb9ncXBK5IhpTAKmibau8sYy0uJNr1lwymRSc9dqn5peh9beayI7u5wCSBAezBX7dsugIx5UWjAIIAOU5qSBGkHrNhjhoYFzE5LKN3oayjJWpxXFv8YsIHbf_UTJbdOALWrsgzlQyBcyNxucgEzTmmJ5BQpurjLDxeDIHaBZJ0et81fwoPrh3Xd5GC6aMJcd6pU");
        properties.put("fs.cosn.credentials.provider", "org.apache.hadoop.fs.auth.SessionTokenCredentialProvider");
        properties.put("lake.fs.token.checker.enable", "false");
        properties.put("lake.fs.token.cache.enable", "false");
        return new DLCIcebergLoadNode(
                "iceberg",
                "iceberg_output",
                fields,
                relations,
                null,
                null,
                null,
                properties,
                "wedata_dev",
                "test_for_dlc_load",
                null,
                "dlc.tencentcloudapi.com",
                "/hive/warehouse");
    }

    /**
     * build node relation
     *
     * @param inputs  extract node
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
        Node outputNode = buildDLCLoadNode();
        StreamInfo streamInfo = new StreamInfo("1L", Arrays.asList(inputNode, outputNode),
                Arrays.asList(buildNodeRelation(Collections.singletonList(inputNode),
                        Collections.singletonList(outputNode))));
        GroupInfo groupInfo = new GroupInfo("group_id", Collections.singletonList(streamInfo));
        FlinkSqlParser parser = FlinkSqlParser.getInstance(tableEnv, groupInfo);
        FlinkSqlParseResult result = (FlinkSqlParseResult) parser.parse();
        //Assert.assertTrue(!result.getLoadSqls().isEmpty() && !result.getCreateTableSqls().isEmpty());
        result.execute();
    }
}
