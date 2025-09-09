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
import org.apache.inlong.sort.protocol.node.extract.WeaviateExtractNode;
import org.apache.inlong.sort.protocol.node.load.MySqlLoadNode;
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
 * Integration test for Weaviate Extract Node to MySQL Load Node ETL flow.
 * Tests the complete data pipeline from Weaviate vector database to MySQL
 * database.
 */
public class WeaviateExtractToMySqlLoadTest extends AbstractTestBase {

    /**
     * Build Weaviate extract node for source data with vector fields.
     */
    private WeaviateExtractNode buildWeaviateExtractNode(String id) {
        List<FieldInfo> fields = Arrays.asList(
                new FieldInfo("id", new LongFormatInfo()),
                new FieldInfo("title", new StringFormatInfo()),
                new FieldInfo("content", new StringFormatInfo()),
                new FieldInfo("category_id", new IntFormatInfo()),
                new FieldInfo("embedding", new ArrayFormatInfo(new FloatFormatInfo())), // Vector field
                new FieldInfo("similarity_score", new FloatFormatInfo()));

        return new WeaviateExtractNode(
                id,
                "weaviate_source",
                fields,
                null, // watermarkField
                null, // properties
                "https://tcujllxnqksj7utgb8fy6w.c0.asia-southeast1.gcp.weaviate.cloud", // url
                "RjZkdk9zeGV2YnNoMTJPUl9qZndoeTlSNnlsZS9lU0ZnVXJkSTZtNEowVDJZeFdXRXRQemJLWTB5cDRzPV92MjAw", // apiKey
                "Article", // className
                "limit: 1000", // queryConditions
                100, // batchSize
                30000, // timeout
                "embedding", // vectorField
                "http", // scheme
                "localhost", // host
                8080, // port
                null, // headers
                null, // username
                null, // password
                null, // tenant
                null, // consistency
                1000, // limit
                null, // offset
                null, // where
                null, // near
                null, // fieldsToSelect
                null, // groupBy
                null, // sort
                null, // maxConnections
                null, // connectionTimeout
                null, // readTimeout
                null, // retryAttempts
                null, // retryInterval
                null, // enableCache
                null, // cacheSize
                null // cacheTtl
        );
    }

    /**
     * Build MySQL load node for storing processed vector data.
     */
    private MySqlLoadNode buildMySqlLoadNode(String id) {
        List<FieldInfo> fields = Arrays.asList(
                new FieldInfo("id", new LongFormatInfo()),
                new FieldInfo("title", new StringFormatInfo()),
                new FieldInfo("content", new StringFormatInfo()),
                new FieldInfo("category_id", new IntFormatInfo()),
                new FieldInfo("embedding_text", new StringFormatInfo()), // Vector converted to text
                new FieldInfo("similarity_score", new FloatFormatInfo()));

        List<FieldRelation> relations = Arrays.asList(
                new FieldRelation(new FieldInfo("id", new LongFormatInfo()),
                        new FieldInfo("id", new LongFormatInfo())),
                new FieldRelation(new FieldInfo("title", new StringFormatInfo()),
                        new FieldInfo("title", new StringFormatInfo())),
                new FieldRelation(new FieldInfo("content", new StringFormatInfo()),
                        new FieldInfo("content", new StringFormatInfo())),
                new FieldRelation(new FieldInfo("category_id", new IntFormatInfo()),
                        new FieldInfo("category_id", new IntFormatInfo())),
                new FieldRelation(new FieldInfo("embedding", new ArrayFormatInfo(new FloatFormatInfo())),
                        new FieldInfo("embedding_text", new StringFormatInfo())), // Vector to text conversion
                new FieldRelation(new FieldInfo("similarity_score", new FloatFormatInfo()),
                        new FieldInfo("similarity_score", new FloatFormatInfo())));

        Map<String, String> properties = new HashMap<>();
        properties.put("sink.buffer-flush.max-rows", "1000");
        properties.put("sink.buffer-flush.interval", "5s");

        return new MySqlLoadNode(
                id,
                "mysql_sink",
                fields,
                relations,
                null, // filters
                null, // filterStrategy
                null, // sinkParallelism
                properties,
                "jdbc:mysql://localhost:3306/test", // url
                "root", // username
                "123456", // password
                "user", // tableName
                "id" // primaryKey
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
     * Test complete ETL flow from Weaviate to MySQL.
     * This test verifies:
     * 1. WeaviateExtractNode can read vector data from Weaviate
     * 2. MySqlLoadNode can write data with vector-to-text conversion
     * 3. FlinkSqlParser can generate correct SQL DDL for both nodes
     * 4. The complete pipeline can be executed without errors
     */
    @Test
    public void testWeaviateToMySqlETL() throws Exception {
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
        Node weaviateExtractNode = buildWeaviateExtractNode("1");
        Node mysqlLoadNode = buildMySqlLoadNode("2");

        // Create stream info with node relation
        StreamInfo streamInfo = new StreamInfo(
                "1L",
                Arrays.asList(weaviateExtractNode, mysqlLoadNode),
                Collections.singletonList(
                        buildNodeRelation(
                                Collections.singletonList(weaviateExtractNode),
                                Collections.singletonList(mysqlLoadNode))));

        // Create group info
        GroupInfo groupInfo = new GroupInfo("1", Collections.singletonList(streamInfo));

        // Parse and execute
        FlinkSqlParser parser = FlinkSqlParser.getInstance(tableEnv, groupInfo);
        ParseResult result = parser.parse();

        // Verify the parsing and execution succeeds
        Assert.assertTrue("Weaviate to MySQL ETL pipeline should parse and execute successfully",
                result.tryExecute());
    }

    /**
     * Test Weaviate extract with similarity search to MySQL.
     * This test verifies similarity search capabilities with vector processing.
     */
    @Test
    public void testWeaviateSimilaritySearchToMySQL() throws Exception {
        // Setup Flink environment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // Build Weaviate extract node with similarity search
        List<FieldInfo> fields = Arrays.asList(
                new FieldInfo("id", new LongFormatInfo()),
                new FieldInfo("title", new StringFormatInfo()),
                new FieldInfo("content", new StringFormatInfo()),
                new FieldInfo("embedding", new ArrayFormatInfo(new FloatFormatInfo())),
                new FieldInfo("similarity_score", new FloatFormatInfo()));

        WeaviateExtractNode weaviateExtractNode = new WeaviateExtractNode(
                "1",
                "weaviate_similarity_source",
                fields,
                null, // watermarkField
                null, // properties
                "http://localhost:8080", // url
                "similarity-test-key", // apiKey
                "Document", // className
                null, // queryConditions
                50, // batchSize
                60000, // timeout
                "embedding", // vectorField
                null, // scheme
                null, // host
                null, // port
                null, // headers
                null, // username
                null, // password
                null, // tenant
                null, // consistency
                100, // limit
                null, // offset
                null, // where
                "nearVector: {vector: [0.1, 0.2, 0.3], distance: 0.8}", // near (similarity search)
                null, // fieldsToSelect
                null, // groupBy
                null, // sort
                null, // maxConnections
                null, // connectionTimeout
                null, // readTimeout
                null, // retryAttempts
                null, // retryInterval
                null, // enableCache
                null, // cacheSize
                null // cacheTtl
        );

        // Build MySQL load node for similarity results
        List<FieldInfo> mysqlFields = Arrays.asList(
                new FieldInfo("id", new LongFormatInfo()),
                new FieldInfo("title", new StringFormatInfo()),
                new FieldInfo("content", new StringFormatInfo()),
                new FieldInfo("embedding_text", new StringFormatInfo()),
                new FieldInfo("similarity_score", new FloatFormatInfo()));

        List<FieldRelation> relations = Arrays.asList(
                new FieldRelation(new FieldInfo("id", new LongFormatInfo()),
                        new FieldInfo("id", new LongFormatInfo())),
                new FieldRelation(new FieldInfo("title", new StringFormatInfo()),
                        new FieldInfo("title", new StringFormatInfo())),
                new FieldRelation(new FieldInfo("content", new StringFormatInfo()),
                        new FieldInfo("content", new StringFormatInfo())),
                new FieldRelation(new FieldInfo("embedding", new ArrayFormatInfo(new FloatFormatInfo())),
                        new FieldInfo("embedding_text", new StringFormatInfo())),
                new FieldRelation(new FieldInfo("similarity_score", new FloatFormatInfo()),
                        new FieldInfo("similarity_score", new FloatFormatInfo())));

        MySqlLoadNode mysqlLoadNode = new MySqlLoadNode(
                "2",
                "mysql_similarity_sink",
                mysqlFields,
                relations,
                null, // filters
                null, // filterStrategy
                null, // sinkParallelism
                null, // properties
                "jdbc:mysql://localhost:3306/test_db", // url
                "root", // username
                "password", // password
                "similarity_results", // tableName
                "id" // primaryKey
        );

        // Create stream info
        StreamInfo streamInfo = new StreamInfo(
                "2L",
                Arrays.asList(weaviateExtractNode, mysqlLoadNode),
                Collections.singletonList(
                        buildNodeRelation(
                                Collections.singletonList(weaviateExtractNode),
                                Collections.singletonList(mysqlLoadNode))));

        GroupInfo groupInfo = new GroupInfo("2", Collections.singletonList(streamInfo));

        // Parse and execute
        FlinkSqlParser parser = FlinkSqlParser.getInstance(tableEnv, groupInfo);
        ParseResult result = parser.parse();

        Assert.assertTrue("Weaviate similarity search to MySQL should work",
                result.tryExecute());
    }

    /**
     * Test Weaviate extract with filtering to MySQL batch processing.
     * This test verifies filtered data extraction and batch processing.
     */
    @Test
    public void testWeaviateFilteredToMySqlBatch() throws Exception {
        // Setup Flink environment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // Build Weaviate extract node with filtering
        List<FieldInfo> fields = Arrays.asList(
                new FieldInfo("id", new LongFormatInfo()),
                new FieldInfo("title", new StringFormatInfo()),
                new FieldInfo("content", new StringFormatInfo()),
                new FieldInfo("category", new StringFormatInfo()),
                new FieldInfo("embedding", new ArrayFormatInfo(new FloatFormatInfo())),
                new FieldInfo("created_at", new StringFormatInfo()));

        WeaviateExtractNode weaviateExtractNode = new WeaviateExtractNode(
                "1",
                "weaviate_filtered_source",
                fields,
                null, // watermarkField
                null, // properties
                "http://localhost:8080", // url
                "filter-test-key", // apiKey
                "Article", // className
                null, // queryConditions
                200, // batchSize (larger for batch processing)
                90000, // timeout (longer for batch)
                "embedding", // vectorField
                null, // scheme
                null, // host
                null, // port
                null, // headers
                null, // username
                null, // password
                null, // tenant
                null, // consistency
                500, // limit (larger batch)
                null, // offset
                "where: {path: [\"category\"], operator: Equal, valueText: \"technology\"}", // where filter
                null, // near
                null, // fieldsToSelect
                null, // groupBy
                "sort: [{path: [\"created_at\"], order: desc}]", // sort by creation date
                null, // maxConnections
                null, // connectionTimeout
                null, // readTimeout
                null, // retryAttempts
                null, // retryInterval
                null, // enableCache
                null, // cacheSize
                null // cacheTtl
        );

        // Build MySQL load node for batch processing
        List<FieldInfo> mysqlFields = Arrays.asList(
                new FieldInfo("id", new LongFormatInfo()),
                new FieldInfo("title", new StringFormatInfo()),
                new FieldInfo("content", new StringFormatInfo()),
                new FieldInfo("category", new StringFormatInfo()),
                new FieldInfo("embedding_text", new StringFormatInfo()),
                new FieldInfo("created_at", new StringFormatInfo()));

        List<FieldRelation> relations = Arrays.asList(
                new FieldRelation(new FieldInfo("id", new LongFormatInfo()),
                        new FieldInfo("id", new LongFormatInfo())),
                new FieldRelation(new FieldInfo("title", new StringFormatInfo()),
                        new FieldInfo("title", new StringFormatInfo())),
                new FieldRelation(new FieldInfo("content", new StringFormatInfo()),
                        new FieldInfo("content", new StringFormatInfo())),
                new FieldRelation(new FieldInfo("category", new StringFormatInfo()),
                        new FieldInfo("category", new StringFormatInfo())),
                new FieldRelation(new FieldInfo("embedding", new ArrayFormatInfo(new FloatFormatInfo())),
                        new FieldInfo("embedding_text", new StringFormatInfo())),
                new FieldRelation(new FieldInfo("created_at", new StringFormatInfo()),
                        new FieldInfo("created_at", new StringFormatInfo())));

        Map<String, String> properties = new HashMap<>();
        properties.put("sink.buffer-flush.max-rows", "2000"); // Larger batch for performance
        properties.put("sink.buffer-flush.interval", "10s"); // Longer interval for batch
        properties.put("sink.max-retries", "5");

        MySqlLoadNode mysqlLoadNode = new MySqlLoadNode(
                "2",
                "mysql_batch_sink",
                mysqlFields,
                relations,
                null, // filters
                null, // filterStrategy
                null, // sinkParallelism
                properties,
                "jdbc:mysql://localhost:3306/test_db", // url
                "root", // username
                "password", // password
                "filtered_articles", // tableName
                "id" // primaryKey
        );

        // Create stream info
        StreamInfo streamInfo = new StreamInfo(
                "3L",
                Arrays.asList(weaviateExtractNode, mysqlLoadNode),
                Collections.singletonList(
                        buildNodeRelation(
                                Collections.singletonList(weaviateExtractNode),
                                Collections.singletonList(mysqlLoadNode))));

        GroupInfo groupInfo = new GroupInfo("3", Collections.singletonList(streamInfo));

        // Parse and execute
        FlinkSqlParser parser = FlinkSqlParser.getInstance(tableEnv, groupInfo);
        ParseResult result = parser.parse();

        Assert.assertTrue("Weaviate filtered extraction to MySQL batch processing should work",
                result.tryExecute());
    }

    /**
     * Test Weaviate extract with minimal configuration to MySQL.
     * This test verifies that the pipeline works with only required parameters.
     */
    @Test
    public void testWeaviateMinimalToMySQL() throws Exception {
        // Setup Flink environment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // Build minimal Weaviate extract node
        List<FieldInfo> fields = Arrays.asList(
                new FieldInfo("id", new LongFormatInfo()),
                new FieldInfo("title", new StringFormatInfo()),
                new FieldInfo("content", new StringFormatInfo()));

        WeaviateExtractNode weaviateExtractNode = new WeaviateExtractNode(
                "1",
                "weaviate_minimal_source",
                fields,
                null, // watermarkField
                null, // properties
                "http://localhost:8080", // url
                null, // apiKey (optional)
                "SimpleArticle", // className
                null, // queryConditions
                null, // batchSize (will use default)
                null, // timeout (will use default)
                null, // vectorField
                null, // scheme
                null, // host
                null, // port
                null, // headers
                null, // username
                null, // password
                null, // tenant
                null, // consistency
                null, // limit
                null, // offset
                null, // where
                null, // near
                null, // fieldsToSelect
                null, // groupBy
                null, // sort
                null, // maxConnections
                null, // connectionTimeout
                null, // readTimeout
                null, // retryAttempts
                null, // retryInterval
                null, // enableCache
                null, // cacheSize
                null // cacheTtl
        );

        // Build minimal MySQL load node
        List<FieldInfo> mysqlFields = Arrays.asList(
                new FieldInfo("id", new LongFormatInfo()),
                new FieldInfo("title", new StringFormatInfo()),
                new FieldInfo("content", new StringFormatInfo()));

        List<FieldRelation> relations = Arrays.asList(
                new FieldRelation(new FieldInfo("id", new LongFormatInfo()),
                        new FieldInfo("id", new LongFormatInfo())),
                new FieldRelation(new FieldInfo("title", new StringFormatInfo()),
                        new FieldInfo("title", new StringFormatInfo())),
                new FieldRelation(new FieldInfo("content", new StringFormatInfo()),
                        new FieldInfo("content", new StringFormatInfo())));

        MySqlLoadNode mysqlLoadNode = new MySqlLoadNode(
                "2",
                "mysql_minimal_sink",
                mysqlFields,
                relations,
                null, // filters
                null, // filterStrategy
                null, // sinkParallelism
                null, // properties
                "jdbc:mysql://localhost:3306/test_db", // url
                "root", // username
                "password", // password
                "simple_articles", // tableName
                "id" // primaryKey
        );

        // Create stream info
        StreamInfo streamInfo = new StreamInfo(
                "4L",
                Arrays.asList(weaviateExtractNode, mysqlLoadNode),
                Collections.singletonList(
                        buildNodeRelation(
                                Collections.singletonList(weaviateExtractNode),
                                Collections.singletonList(mysqlLoadNode))));

        GroupInfo groupInfo = new GroupInfo("4", Collections.singletonList(streamInfo));

        // Parse and execute
        FlinkSqlParser parser = FlinkSqlParser.getInstance(tableEnv, groupInfo);
        ParseResult result = parser.parse();

        Assert.assertTrue("Minimal Weaviate to MySQL configuration should work",
                result.tryExecute());
    }
}