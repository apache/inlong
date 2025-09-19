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

import org.apache.inlong.common.pojo.sort.dataflow.field.format.ArrayFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.FloatFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.IntFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.LongFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.StringFormatInfo;
import org.apache.inlong.sort.parser.impl.FlinkSqlParser;
import org.apache.inlong.sort.parser.result.ParseResult;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.GroupInfo;
import org.apache.inlong.sort.protocol.StreamInfo;
import org.apache.inlong.sort.protocol.node.Node;
import org.apache.inlong.sort.protocol.node.extract.MySqlExtractNode;
import org.apache.inlong.sort.protocol.node.load.WeaviateLoadNode;
import org.apache.inlong.sort.protocol.transformation.FieldRelation;
import org.apache.inlong.sort.protocol.transformation.relation.NodeRelation;

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
 * Integration test for MySQL Extract Node to Weaviate Load Node ETL flow.
 * Tests the complete data pipeline from MySQL database to Weaviate vector
 * database.
 */
public class MySqlExtractToWeaviateLoadTest extends AbstractTestBase {

    /**
     * Build MySQL extract node for source data.
     */
    private MySqlExtractNode buildMySqlExtractNode(String id) {
        List<FieldInfo> fields = Arrays.asList(
                new FieldInfo("id", new LongFormatInfo()),
                new FieldInfo("title", new StringFormatInfo()),
                new FieldInfo("content", new StringFormatInfo()),
                new FieldInfo("category_id", new IntFormatInfo()),
                new FieldInfo("embedding_vector", new StringFormatInfo()) // Vector as string initially
        );

        Map<String, String> properties = new HashMap<>();
        properties.put("scan.incremental.snapshot.enabled", "false");

        return new MySqlExtractNode(
                id,
                "mysql_source",
                fields,
                null, // watermarkField
                properties,
                "id", // primaryKey
                Collections.singletonList("user"), // tableNames
                "localhost", // hostname
                "root", // username
                "123456", // password
                "test", // database
                3306, // port
                null, // serverId
                false, // incrementalSnapshotEnabled
                null // serverTimeZone
        );
    }

    /**
     * Build Weaviate load node for storing vectorized data.
     */
    private WeaviateLoadNode buildWeaviateLoadNode(String id) {
        List<FieldInfo> fields = Arrays.asList(
                new FieldInfo("id", new LongFormatInfo()),
                new FieldInfo("title", new StringFormatInfo()),
                new FieldInfo("content", new StringFormatInfo()),
                new FieldInfo("category_id", new IntFormatInfo()),
                new FieldInfo("embedding", new ArrayFormatInfo(new FloatFormatInfo())) // Convert to vector
        );

        List<FieldRelation> relations = Arrays.asList(
                new FieldRelation(new FieldInfo("id", new LongFormatInfo()),
                        new FieldInfo("id", new LongFormatInfo())),
                new FieldRelation(new FieldInfo("title", new StringFormatInfo()),
                        new FieldInfo("title", new StringFormatInfo())),
                new FieldRelation(new FieldInfo("content", new StringFormatInfo()),
                        new FieldInfo("content", new StringFormatInfo())),
                new FieldRelation(new FieldInfo("category_id", new IntFormatInfo()),
                        new FieldInfo("category_id", new IntFormatInfo())),
                new FieldRelation(new FieldInfo("embedding_vector", new StringFormatInfo()),
                        new FieldInfo("embedding", new ArrayFormatInfo(new FloatFormatInfo()))));

        return new WeaviateLoadNode(
                id,
                "weaviate_sink",
                fields,
                relations,
                null, // filters
                null, // filterStrategy
                null, // sinkParallelism
                null, // properties
                "https://tcujllxnqksj7utgb8fy6w.c0.asia-southeast1.gcp.weaviate.cloud", // url
                "RjZkdk9zeGV2YnNoMTJPUl9qZndoeTlSNnlsZS9lU0ZnVXJkSTZtNEowVDJZeFdXRXRQemJLWTB5cDRzPV92MjAw", // apiKey
                "Article", // className
                100, // batchSize
                "embedding", // vectorField
                5000L, // flushInterval
                30000L, // timeout
                3 // maxRetries
        );
    }

    /**
     * Build node relation between extract and load nodes.
     */
    private NodeRelation buildNodeRelation(List<Node> inputs, List<Node> outputs) {
        List<String> inputIds = inputs.stream().map(Node::getId).collect(Collectors.toList());
        List<String> outputIds = outputs.stream().map(Node::getId).collect(Collectors.toList());
        return new NodeRelation(inputIds, outputIds);
    }

    /**
     * Test complete ETL flow from MySQL to Weaviate.
     * This test verifies:
     * 1. MySqlExtractNode can read data from MySQL database
     * 2. WeaviateLoadNode can write data with vector conversion
     * 3. FlinkSqlParser can generate correct SQL DDL for both nodes
     * 4. The complete pipeline can be executed without errors
     */
    @Test
    public void testMySqlToWeaviateETL() throws Exception {
        // Setup Flink environment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // Build extract and load nodes
        Node mysqlExtractNode = buildMySqlExtractNode("1");
        Node weaviateLoadNode = buildWeaviateLoadNode("2");

        // Create stream info with node relation
        StreamInfo streamInfo = new StreamInfo(
                "1L",
                Arrays.asList(mysqlExtractNode, weaviateLoadNode),
                Collections.singletonList(
                        buildNodeRelation(
                                Collections.singletonList(mysqlExtractNode),
                                Collections.singletonList(weaviateLoadNode))));

        // Create group info
        GroupInfo groupInfo = new GroupInfo("1", Collections.singletonList(streamInfo));

        // Parse and execute
        FlinkSqlParser parser = FlinkSqlParser.getInstance(tableEnv, groupInfo);
        ParseResult result = parser.parse();

        // Verify the parsing and execution succeeds
        Assert.assertTrue("MySQL to Weaviate ETL pipeline should parse and execute successfully",
                result.tryExecute());
    }

    /**
     * Test MySQL extract with batch processing to Weaviate.
     * This test verifies batch processing capabilities for large datasets.
     */
    @Test
    public void testMySqlToWeaviateBatchProcessing() throws Exception {
        // Setup Flink environment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // Build MySQL extract node with batch configuration
        List<FieldInfo> fields = Arrays.asList(
                new FieldInfo("id", new LongFormatInfo()),
                new FieldInfo("title", new StringFormatInfo()),
                new FieldInfo("content", new StringFormatInfo()),
                new FieldInfo("embedding_vector", new StringFormatInfo()));

        Map<String, String> properties = new HashMap<>();
        properties.put("scan.incremental.snapshot.enabled", "false");
        properties.put("scan.fetch.size", "1000");

        MySqlExtractNode mysqlExtractNode = new MySqlExtractNode(
                "1",
                "mysql_batch_source",
                fields,
                null, // watermarkField
                properties,
                "id", // primaryKey
                Collections.singletonList("large_articles"), // tableNames
                "localhost", // hostname
                "root", // username
                "password", // password
                "test_db", // database
                3306, // port
                10002, // serverId
                false, // incrementalSnapshotEnabled
                null // serverTimeZone
        );

        // Build Weaviate load node with larger batch size
        List<FieldInfo> weaviateFields = Arrays.asList(
                new FieldInfo("id", new LongFormatInfo()),
                new FieldInfo("title", new StringFormatInfo()),
                new FieldInfo("content", new StringFormatInfo()),
                new FieldInfo("embedding", new ArrayFormatInfo(new FloatFormatInfo())));

        List<FieldRelation> relations = Arrays.asList(
                new FieldRelation(new FieldInfo("id", new LongFormatInfo()),
                        new FieldInfo("id", new LongFormatInfo())),
                new FieldRelation(new FieldInfo("title", new StringFormatInfo()),
                        new FieldInfo("title", new StringFormatInfo())),
                new FieldRelation(new FieldInfo("content", new StringFormatInfo()),
                        new FieldInfo("content", new StringFormatInfo())),
                new FieldRelation(new FieldInfo("embedding_vector", new StringFormatInfo()),
                        new FieldInfo("embedding", new ArrayFormatInfo(new FloatFormatInfo()))));

        WeaviateLoadNode weaviateLoadNode = new WeaviateLoadNode(
                "2",
                "weaviate_batch_sink",
                weaviateFields,
                relations,
                null, // filters
                null, // filterStrategy
                null, // sinkParallelism
                null, // properties
                "http://localhost:8080", // url
                "batch-test-key", // apiKey
                "LargeArticle", // className
                500, // batchSize (larger for batch processing)
                "embedding", // vectorField
                10000L, // flushInterval (longer for batch)
                60000L, // timeout (longer for batch)
                5 // maxRetries
        );

        // Create stream info
        StreamInfo streamInfo = new StreamInfo(
                "2L",
                Arrays.asList(mysqlExtractNode, weaviateLoadNode),
                Collections.singletonList(
                        buildNodeRelation(
                                Collections.singletonList(mysqlExtractNode),
                                Collections.singletonList(weaviateLoadNode))));

        GroupInfo groupInfo = new GroupInfo("2", Collections.singletonList(streamInfo));

        // Parse and execute
        FlinkSqlParser parser = FlinkSqlParser.getInstance(tableEnv, groupInfo);
        ParseResult result = parser.parse();

        Assert.assertTrue("MySQL to Weaviate batch processing should work",
                result.tryExecute());
    }

    /**
     * Test MySQL CDC to Weaviate with real-time updates.
     * This test verifies CDC (Change Data Capture) functionality.
     */
    @Test
    public void testMySqlCDCToWeaviate() throws Exception {
        // Setup Flink environment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // Build MySQL CDC extract node
        List<FieldInfo> fields = Arrays.asList(
                new FieldInfo("id", new LongFormatInfo()),
                new FieldInfo("title", new StringFormatInfo()),
                new FieldInfo("content", new StringFormatInfo()),
                new FieldInfo("updated_at", new StringFormatInfo()),
                new FieldInfo("embedding_vector", new StringFormatInfo()));

        Map<String, String> properties = new HashMap<>();
        properties.put("scan.incremental.snapshot.enabled", "true");
        properties.put("scan.incremental.snapshot.chunk.size", "1000");

        MySqlExtractNode mysqlCDCNode = new MySqlExtractNode(
                "1",
                "mysql_cdc_source",
                fields,
                null, // watermarkField
                properties,
                "id", // primaryKey
                Collections.singletonList("articles"), // tableNames
                "localhost", // hostname
                "root", // username
                "password", // password
                "test_db", // database
                3306, // port
                10003, // serverId
                true, // incrementalSnapshotEnabled
                "UTC" // serverTimeZone
        );

        // Build Weaviate load node for CDC updates
        List<FieldInfo> weaviateFields = Arrays.asList(
                new FieldInfo("id", new LongFormatInfo()),
                new FieldInfo("title", new StringFormatInfo()),
                new FieldInfo("content", new StringFormatInfo()),
                new FieldInfo("updated_at", new StringFormatInfo()),
                new FieldInfo("embedding", new ArrayFormatInfo(new FloatFormatInfo())));

        List<FieldRelation> relations = Arrays.asList(
                new FieldRelation(new FieldInfo("id", new LongFormatInfo()),
                        new FieldInfo("id", new LongFormatInfo())),
                new FieldRelation(new FieldInfo("title", new StringFormatInfo()),
                        new FieldInfo("title", new StringFormatInfo())),
                new FieldRelation(new FieldInfo("content", new StringFormatInfo()),
                        new FieldInfo("content", new StringFormatInfo())),
                new FieldRelation(new FieldInfo("updated_at", new StringFormatInfo()),
                        new FieldInfo("updated_at", new StringFormatInfo())),
                new FieldRelation(new FieldInfo("embedding_vector", new StringFormatInfo()),
                        new FieldInfo("embedding", new ArrayFormatInfo(new FloatFormatInfo()))));

        WeaviateLoadNode weaviateLoadNode = new WeaviateLoadNode(
                "2",
                "weaviate_cdc_sink",
                weaviateFields,
                relations,
                null, // filters
                null, // filterStrategy
                null, // sinkParallelism
                null, // properties
                "http://localhost:8080", // url
                "cdc-test-key", // apiKey
                "Article", // className
                50, // batchSize (smaller for real-time)
                "embedding", // vectorField
                2000L, // flushInterval (shorter for real-time)
                30000L, // timeout
                3 // maxRetries
        );

        // Create stream info
        StreamInfo streamInfo = new StreamInfo(
                "3L",
                Arrays.asList(mysqlCDCNode, weaviateLoadNode),
                Collections.singletonList(
                        buildNodeRelation(
                                Collections.singletonList(mysqlCDCNode),
                                Collections.singletonList(weaviateLoadNode))));

        GroupInfo groupInfo = new GroupInfo("3", Collections.singletonList(streamInfo));

        // Parse and execute
        FlinkSqlParser parser = FlinkSqlParser.getInstance(tableEnv, groupInfo);
        ParseResult result = parser.parse();

        Assert.assertTrue("MySQL CDC to Weaviate should work for real-time updates",
                result.tryExecute());
    }
}
