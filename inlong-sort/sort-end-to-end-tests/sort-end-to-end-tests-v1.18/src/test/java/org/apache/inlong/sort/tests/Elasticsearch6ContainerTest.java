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

package org.apache.inlong.sort.tests;

import org.apache.inlong.sort.tests.utils.FlinkContainerTestEnvJRE8;
import org.apache.inlong.sort.tests.utils.TestUtils;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class Elasticsearch6ContainerTest extends FlinkContainerTestEnvJRE8 {

    private static final Logger PG_LOG = LoggerFactory.getLogger(PostgreSQLContainer.class);
    private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch6ContainerTest.class);
    private static final Path postgresJar = TestUtils.getResource("sort-connector-postgres-cdc.jar");
    private static final Path es6Jar = TestUtils.getResource("sort-connector-elasticsearch6.jar");
    private static final Path mysqlJdbcJar = TestUtils.getResource("mysql-driver.jar");
    private static final String sqlFile;
    private static final DockerImageName ELASTICSEARCH_IMAGE = DockerImageName
            .parse("docker.elastic.co/elasticsearch/elasticsearch:6.8.17");
    private static ElasticsearchContainer elasticsearchContainer;
    private static RestHighLevelClient client;

    static {
        try {
            sqlFile = Paths.get(Elasticsearch6ContainerTest.class.getResource("/flinkSql/pg2es6.sql").toURI())
                    .toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
    @ClassRule
    public static final PostgreSQLContainer POSTGRES_CONTAINER = (PostgreSQLContainer) new PostgreSQLContainer(
            DockerImageName.parse("debezium/postgres:13").asCompatibleSubstituteFor("postgres"))
                    .withUsername("flinkuser")
                    .withPassword("flinkpw")
                    .withDatabaseName("test")
                    .withNetwork(NETWORK)
                    .withNetworkAliases("postgres")
                    .withLogConsumer(new Slf4jLogConsumer(PG_LOG));

    @Before
    public void setup() {
        elasticsearchContainer = new ElasticsearchContainer(ELASTICSEARCH_IMAGE)
                .withNetwork(NETWORK)
                .withNetworkAliases("elasticsearch");
        elasticsearchContainer.start();

        client = new RestHighLevelClient(
                RestClient.builder(HttpHost.create(elasticsearchContainer.getHttpHostAddress())));
        initializePostgresTable();
        waitUntilJobRunning(Duration.ofSeconds(30));
    }

    private void initializePostgresTable() {
        try {
            Class.forName(POSTGRES_CONTAINER.getDriverClassName());
            Connection conn = DriverManager
                    .getConnection(POSTGRES_CONTAINER.getJdbcUrl(), POSTGRES_CONTAINER.getUsername(),
                            POSTGRES_CONTAINER.getPassword());
            Statement stat = conn.createStatement();
            stat.execute(
                    "CREATE TABLE test_input1 (\n"
                            + "  id SERIAL,\n"
                            + "  name VARCHAR(255) NOT NULL DEFAULT 'flink',\n"
                            + "  description VARCHAR(512),\n"
                            + "  PRIMARY  KEY(id)\n"
                            + ");");
            stat.execute(
                    "ALTER TABLE test_input1 REPLICA IDENTITY FULL; ");
            stat.close();
            conn.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void teardown() throws IOException {
        if (POSTGRES_CONTAINER != null) {
            POSTGRES_CONTAINER.stop();
        }
        if (client != null) {
            client.close();
        }
        if (elasticsearchContainer != null) {
            elasticsearchContainer.stop();
        }
    }

    /**
     * Test flink sql postgresql cdc to StarRocks
     *
     * @throws Exception The exception may throws when execute the case
     */
    @org.junit.Test
    public void testPostgresUpdateAndDelete() throws Exception {
        submitSQLJob(sqlFile, es6Jar, postgresJar, mysqlJdbcJar);
        waitUntilJobRunning(Duration.ofSeconds(10));

        // generate input
        try (Connection conn =
                DriverManager.getConnection(POSTGRES_CONTAINER.getJdbcUrl(), POSTGRES_CONTAINER.getUsername(),
                        POSTGRES_CONTAINER.getPassword());
                Statement stat = conn.createStatement()) {
            stat.execute(
                    "INSERT INTO test_input1 "
                            + "VALUES (1,'jacket','water resistent white wind breaker');");
            stat.execute(
                    "INSERT INTO test_input1 VALUES (2,'scooter','Big 2-wheel scooter ');");
            stat.execute(
                    "update test_input1 set name = 'tom' where id = 2;");
            stat.execute(
                    "delete from test_input1 where id = 1;");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }
        Thread.sleep(5000L);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchAllQuery());
        SearchRequest searchRequest = new SearchRequest("test_index");
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = searchResponse.getHits().getHits();
        assertNotEquals(0, hits.length);
        Map<String, Object> fields = hits[0].getSourceAsMap();
        assertEquals(2, fields.get("id"));
        assertEquals("tom", fields.get("name"));
        assertEquals("Big 2-wheel scooter ", fields.get("description"));
    }
}
