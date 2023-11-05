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

package org.apache.inlong.manager.service.sink;

import org.apache.inlong.manager.pojo.sink.es.ElasticsearchFieldInfo;
import org.apache.inlong.manager.service.resource.sink.es.ElasticsearchApi;
import org.apache.inlong.manager.service.resource.sink.es.ElasticsearchConfig;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for {@link org.apache.inlong.manager.service.resource.sink.es.ElasticsearchApi}.
 */
public class ElasticsearchApiTest {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchApiTest.class);

    public static final Network NETWORK = Network.newNetwork();

    private static final String INTER_CONTAINER_ELASTICSEARCH_ALIAS = "elasticsearch";

    private static final String ELASTICSEARCH_DOCKER_IMAGE_NAME = "elasticsearch:7.9.3";

    private ElasticsearchApi elasticsearchApi;

    private ElasticsearchConfig elasticsearchConfig;

    private static final Gson GSON = new GsonBuilder().create();

    private static final ElasticsearchContainer ELASTICSEARCH_CONTAINER = new ElasticsearchContainer(
            DockerImageName.parse(ELASTICSEARCH_DOCKER_IMAGE_NAME)
                    .asCompatibleSubstituteFor("docker.elastic.co/elasticsearch/elasticsearch"))
                            .withNetwork(NETWORK)
                            .withAccessToHost(true)
                            .withEnv("discovery.type", "single-node")
                            .withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m")
                            .withEnv("ELASTIC_PASSWORD", "test")
                            .withNetworkAliases(INTER_CONTAINER_ELASTICSEARCH_ALIAS)
                            .withLogConsumer(new Slf4jLogConsumer(LOG));

    @BeforeAll
    public static void beforeAll() {
        ELASTICSEARCH_CONTAINER.setPortBindings(Arrays.asList("9200:9200", "9300:9300"));
        ELASTICSEARCH_CONTAINER.withStartupTimeout(Duration.ofSeconds(300));
        ELASTICSEARCH_CONTAINER.setDockerImageName(ELASTICSEARCH_DOCKER_IMAGE_NAME);
        Startables.deepStart(Stream.of(ELASTICSEARCH_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    @BeforeEach
    public void before() {
        elasticsearchConfig = new ElasticsearchConfig();
        elasticsearchConfig.setHosts("http://127.0.0.1:9200");
        elasticsearchConfig.setAuthEnable(true);
        elasticsearchConfig.setUsername("admin");
        elasticsearchConfig.setPassword("inlong");
        elasticsearchApi = new ElasticsearchApi();
        elasticsearchApi.setEsConfig(elasticsearchConfig);
    }

    @AfterAll
    public static void teardown() {
        if (ELASTICSEARCH_CONTAINER != null) {
            ELASTICSEARCH_CONTAINER.stop();
        }
    }

    /**
     * Test cases for {@link ElasticsearchApi#search(String, JsonObject)}.
     *
     * @throws Exception
     */
    @Test
    public void testSearch() throws Exception {
        final String indexName = "test_search";
        final String searchJson = "{\n  \"query\": {\n    \"match_all\": {}\n  }\n}";
        final JsonObject search = GSON.fromJson(searchJson, JsonObject.class);
        elasticsearchApi.createIndex(indexName);
        JsonObject result = elasticsearchApi.search(indexName, search);
        assertNotNull(result);
    }

    /**
     * Test cases for {@link ElasticsearchApi#indexExists(String)}.
     *
     * @throws Exception
     */
    @Test
    public void testIndexExists() throws Exception {
        final String indexName = "test_index_exists";
        boolean result = elasticsearchApi.indexExists(indexName);
        assertEquals(false, result);
        elasticsearchApi.createIndex(indexName);
        result = elasticsearchApi.indexExists(indexName);
        assertEquals(true, result);
    }

    /**
     * Test cases for {@link ElasticsearchApi#ping()}.
     *
     * @throws Exception
     */
    @Test
    public void testPing() throws Exception {
        boolean result = elasticsearchApi.ping();
        assertEquals(true, result);
    }

    /**
     * Test cases for {@link ElasticsearchApi#createIndex(String)}.
     *
     * @throws Exception
     */
    @Test
    public void testCreateIndex() throws Exception {
        final String indexName = "test_create_index";
        elasticsearchApi.createIndex(indexName);
        assertTrue(elasticsearchApi.indexExists(indexName));
    }

    /**
     * Test cases for {@link ElasticsearchApi#createIndexAndMapping(String, List)}.
     *
     * @throws Exception
     */
    @Test
    public void testCreateIndexAndMapping() throws Exception {
        final String indexName = "test_create_index_and_mapping";
        final List<ElasticsearchFieldInfo> fieldInfos = new ArrayList<>();
        final ElasticsearchFieldInfo log_ts = new ElasticsearchFieldInfo();
        log_ts.setFieldType("keyword");
        log_ts.setFieldName("log_ts");
        fieldInfos.add(log_ts);
        elasticsearchApi.createIndexAndMapping(indexName, fieldInfos);
        assertTrue(elasticsearchApi.indexExists(indexName));
    }

    /**
     * Test cases for {@link ElasticsearchApi#getMappingMap(String)}.
     *
     * @throws Exception
     */
    @Test
    public void testGetMappingInfo() throws Exception {
        final String indexName = "test_get_mapping_info";
        final List<ElasticsearchFieldInfo> fieldInfos = new ArrayList<>();
        final ElasticsearchFieldInfo count = new ElasticsearchFieldInfo();
        count.setFieldType("double");
        count.setFieldName("count");
        fieldInfos.add(count);
        final ElasticsearchFieldInfo date = new ElasticsearchFieldInfo();
        date.setFieldType("date");
        date.setFieldName("date");
        date.setFieldFormat("yyyy-MM-dd HH:mm:ss||yyy-MM-dd||epoch_millis");
        fieldInfos.add(date);
        final ElasticsearchFieldInfo delay = new ElasticsearchFieldInfo();
        delay.setFieldType("double");
        delay.setFieldName("delay");
        fieldInfos.add(delay);
        final ElasticsearchFieldInfo inlong_group_id = new ElasticsearchFieldInfo();
        inlong_group_id.setFieldType("text");
        inlong_group_id.setFieldName("inlong_group_id");
        fieldInfos.add(inlong_group_id);
        final ElasticsearchFieldInfo inlong_stream_id = new ElasticsearchFieldInfo();
        inlong_stream_id.setFieldType("text");
        inlong_stream_id.setFieldName("inlong_stream_id");
        fieldInfos.add(inlong_stream_id);
        final ElasticsearchFieldInfo log_ts = new ElasticsearchFieldInfo();
        log_ts.setFieldType("keyword");
        log_ts.setFieldName("log_ts");
        fieldInfos.add(log_ts);
        elasticsearchApi.createIndexAndMapping(indexName, fieldInfos);
        Map<String, ElasticsearchFieldInfo> result = elasticsearchApi.getMappingMap(indexName);
        assertEquals("double", result.get("count").getFieldType());
        assertEquals("double", result.get("delay").getFieldType());
        assertEquals("date", result.get("date").getFieldType());
        assertEquals("yyyy-MM-dd HH:mm:ss||yyy-MM-dd||epoch_millis", result.get("date").getFieldFormat());
        assertEquals("text", result.get("inlong_stream_id").getFieldType());
        assertEquals("text", result.get("inlong_group_id").getFieldType());
        assertEquals("keyword", result.get("log_ts").getFieldType());
    }

    /**
     * Test cases for {@link ElasticsearchApi#addFields(String, List)}.
     *
     * @throws Exception
     */
    @Test
    public void testAddFields() throws Exception {
        final String indexName = "test_add_fields";
        final List<ElasticsearchFieldInfo> fieldInfos = new ArrayList<>();
        final ElasticsearchFieldInfo count = new ElasticsearchFieldInfo();
        count.setFieldType("double");
        count.setFieldName("count");
        fieldInfos.add(count);
        elasticsearchApi.createIndexAndMapping(indexName, fieldInfos);

        final List<ElasticsearchFieldInfo> addFieldInfos = new ArrayList<>();
        final ElasticsearchFieldInfo log_ts = new ElasticsearchFieldInfo();
        log_ts.setFieldType("keyword");
        log_ts.setFieldName("log_ts");
        addFieldInfos.add(log_ts);
        elasticsearchApi.addFields(indexName, addFieldInfos);

        Map<String, ElasticsearchFieldInfo> result = elasticsearchApi.getMappingMap(indexName);
        assertEquals("double", result.get("count").getFieldType());
        assertEquals("keyword", result.get("log_ts").getFieldType());
    }
}
