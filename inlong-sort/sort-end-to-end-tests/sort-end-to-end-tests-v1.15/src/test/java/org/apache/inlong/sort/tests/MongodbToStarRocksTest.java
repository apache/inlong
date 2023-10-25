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

import org.apache.inlong.sort.tests.utils.FlinkContainerTestEnv;
import org.apache.inlong.sort.tests.utils.JdbcProxy;
import org.apache.inlong.sort.tests.utils.StarRocksContainer;
import org.apache.inlong.sort.tests.utils.TestUtils;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.*;

/**
 * End-to-end tests for sort-connector-mongodb-cdc-v1.15 uber jar.
 * Test flink sql Mongodb cdc to StarRocks
 */
public class MongodbToStarRocksTest extends FlinkContainerTestEnv {

    private static final Logger LOG = LoggerFactory.getLogger(MongodbToStarRocksTest.class);

    private static final Path mongodbJar = TestUtils.getResource("sort-connector-mongodb-cdc.jar");
    private static final Path jdbcJar = TestUtils.getResource("sort-connector-starrocks.jar");
    private static final Path mysqlJdbcJar = TestUtils.getResource("mysql-driver.jar");

    private static final Logger STAR_ROCKS_LOG = LoggerFactory.getLogger(StarRocksContainer.class);

    private static final String sqlFile;

    // ----------------------------------------------------------------------------------------
    // StarRocks Variables
    // ----------------------------------------------------------------------------------------
    private static final String INTER_CONTAINER_STAR_ROCKS_ALIAS = "starrocks";
    private static final String NEW_STARROCKS_REPOSITORY = "inlong-starrocks";
    private static final String NEW_STARROCKS_TAG = "latest";
    private static final String STAR_ROCKS_IMAGE_NAME = "starrocks/allin1-ubi:3.0.4";

    static {
        try {
            sqlFile = Paths.get(PostgresToStarRocksTest.class.getResource("/flinkSql/mongodb_test.sql").toURI())
                    .toString();
            buildStarRocksImage();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getNewStarRocksImageName() {
        return NEW_STARROCKS_REPOSITORY + ":" + NEW_STARROCKS_TAG;
    }

    public static void buildStarRocksImage() {
        GenericContainer oldStarRocks = new GenericContainer(STAR_ROCKS_IMAGE_NAME);
        Startables.deepStart(Stream.of(oldStarRocks)).join();
        oldStarRocks.copyFileToContainer(MountableFile.forClasspathResource("/docker/starrocks/start_fe_be.sh"),
                "/data/deploy/");
        try {
            oldStarRocks.execInContainer("chmod", "+x", "/data/deploy/start_fe_be.sh");
        } catch (Exception e) {
            e.printStackTrace();
        }
        oldStarRocks.getDockerClient()
                .commitCmd(oldStarRocks.getContainerId())
                .withRepository(NEW_STARROCKS_REPOSITORY)
                .withTag(NEW_STARROCKS_TAG).exec();
        oldStarRocks.stop();
    }

    @ClassRule
    public static StarRocksContainer STAR_ROCKS =
            (StarRocksContainer) new StarRocksContainer(getNewStarRocksImageName())
                    .withExposedPorts(9030, 8030, 8040)
                    .withNetwork(NETWORK)
                    .withAccessToHost(true)
                    .withNetworkAliases(INTER_CONTAINER_STAR_ROCKS_ALIAS)
                    .withLogConsumer(new Slf4jLogConsumer(STAR_ROCKS_LOG));

    @ClassRule
    public static MongoDBContainer MONGODB_CONTAINER = new MongoDBContainer(
            DockerImageName.parse("mongo:4.0.10").asCompatibleSubstituteFor("mongo"))
                    .withNetwork(NETWORK)
                    .withNetworkAliases("mongo")
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @Before
    public void setup() {
        waitUntilJobRunning(Duration.ofSeconds(30));
        initializeStarRocksTable();
    }

    private void initializeStarRocksTable() {
        try (Connection conn =
                DriverManager.getConnection(STAR_ROCKS.getJdbcUrl(), STAR_ROCKS.getUsername(),
                        STAR_ROCKS.getPassword());
                Statement stat = conn.createStatement()) {
            stat.execute("CREATE TABLE IF NOT EXISTS test_output1 (\n"
                    + "       _id INT NOT NULL,\n"
                    + "       name VARCHAR(255) NOT NULL DEFAULT 'flink',\n"
                    + "       description VARCHAR(512)\n"
                    + ")\n"
                    + "PRIMARY KEY(_id)\n"
                    + "DISTRIBUTED by HASH(_id) PROPERTIES (\"replication_num\" = \"1\");");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void teardown() {
        if (MONGODB_CONTAINER != null) {
            MONGODB_CONTAINER.stop();
        }
        if (STAR_ROCKS != null) {
            STAR_ROCKS.stop();
        }
    }

    /**
     * Test flink sql postgresql cdc to StarRocks
     *
     * @throws Exception The exception may throws when execute the case
     */
    @Test
    public void testMongodbUpdateAndDelete() throws Exception {
        submitSQLJob(sqlFile, jdbcJar, mongodbJar, mysqlJdbcJar);
        waitUntilJobRunning(Duration.ofSeconds(10));

        // generate input
        MongoClient mongoClient = MongoClients.create(MONGODB_CONTAINER.getConnectionString());
        MongoDatabase database = mongoClient.getDatabase("test");
        MongoCollection<Document> collection = database.getCollection("test_input1");

        Document document1 = new Document("_id", 1)
                .append("name", "jacket")
                .append("description", "water resistent white wind breaker");
        Document document2 = new Document("_id", 2)
                .append("name", "scooter")
                .append("description", "Big 2-wheel scooter ");
        List<Document> documents = new ArrayList<Document>();
        documents.add(document1);
        documents.add(document2);
        collection.insertMany(documents);
        collection.updateOne(eq("_id", 2), combine(set("name", "tom")));
        collection.deleteOne(eq("_id", 1));

        JdbcProxy proxy =
                new JdbcProxy(STAR_ROCKS.getJdbcUrl(), STAR_ROCKS.getUsername(),
                        STAR_ROCKS.getPassword(),
                        STAR_ROCKS.getDriverClassName());
        List<String> expectResult =
                Arrays.asList("2,tom,Big 2-wheel scooter ");
        proxy.checkResultWithTimeout(
                expectResult,
                "test_output1",
                3,
                60000L);
    }
}
