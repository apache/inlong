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
import org.apache.inlong.sort.tests.utils.RedisContainer;
import org.apache.inlong.sort.tests.utils.TestUtils;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.Jedis;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

public class RedisToRedisTest extends FlinkContainerTestEnvJRE8 {

    private static final Logger LOG = LoggerFactory.getLogger(RedisContainer.class);
    private static final Path redisJar = TestUtils.getResource("sort-connector-redis.jar");
    private static final String sqlFile;
    private static Jedis jedisSource;
    private static Jedis jedisSink;

    static {
        try {
            sqlFile = Paths.get(RedisToRedisTest.class.getResource("/flinkSql/redis_test.sql").toURI())
                    .toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @ClassRule
    public static final RedisContainer REDIS_CONTAINER_SOURCE = new RedisContainer(
            DockerImageName.parse("redis:6.2.14"))
                    .withExposedPorts(6379)
                    .withNetwork(NETWORK)
                    .withNetworkAliases("redis_source")
                    .withLogConsumer(new Slf4jLogConsumer(LOG));
    @ClassRule
    public static final RedisContainer REDIS_CONTAINER_SINK = new RedisContainer(
            DockerImageName.parse("redis:6.2.14"))
                    .withExposedPorts(6379)
                    .withNetwork(NETWORK)
                    .withNetworkAliases("redis_sink")
                    .withLogConsumer(new Slf4jLogConsumer(LOG));
    @Before
    public void setup() {
        waitUntilJobRunning(Duration.ofSeconds(30));
        initializeRedisTable();
    }

    private void initializeRedisTable() {

        int sourcePort = REDIS_CONTAINER_SOURCE.getRedisPort();
        int sinkPort = REDIS_CONTAINER_SINK.getRedisPort();

        jedisSource = new Jedis("127.0.0.1", sourcePort);
        jedisSink = new Jedis("127.0.0.1", sinkPort);

        jedisSource.set("1", "value_1");
        jedisSource.set("2", "value_2");

        jedisSource.hset("3", "1", "value_1");
        jedisSource.hset("3", "2", "value_2");

        // ZREVRANK TEST
        jedisSource.zadd("rank", 10, "1");
        jedisSource.zadd("rank", 20, "2");
        jedisSource.zadd("rank", 30, "3");

        // ZSCORETEST TEST
        jedisSource.zadd("rank_score", 10, "1");
        jedisSource.zadd("rank_score", 20, "2");
        jedisSource.zadd("rank_score", 30, "3");

    }

    @AfterClass
    public static void teardown() {
        REDIS_CONTAINER_SOURCE.stop();
        REDIS_CONTAINER_SINK.stop();
    }

    /**
     * Test flink sql postgresql cdc to StarRocks
     *
     * @throws Exception The exception may throws when execute the case
     */
    @Test
    public void testRedisSourceAndSink() throws Exception {
        submitSQLJob(sqlFile, redisJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals("value_1_1", jedisSink.get("1_1"));
            assertEquals("value_2_2", jedisSink.get("2_2"));
        });
        await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals("value_1", jedisSink.hget("3_3", "1"));
            assertEquals("value_2", jedisSink.hget("3_3", "2"));
        });
        await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals("10.0", jedisSink.hget("rank_score_test", "1"));
            assertEquals("20.0", jedisSink.hget("rank_score_test", "2"));
            assertEquals("30.0", jedisSink.hget("rank_score_test", "3"));
        });
        await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals("2", jedisSink.hget("rank_test", "1"));
            assertEquals("1", jedisSink.hget("rank_test", "2"));
            assertEquals("0", jedisSink.hget("rank_test", "3"));
        });
    }

}
