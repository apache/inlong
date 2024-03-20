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

package org.apache.inlong.sort.redis;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.embedded.RedisServer;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.*;

public class RedisTableTest {

    private static int redisPort;

    private static RedisServer redisServer;

    @BeforeClass
    public static void setup() {
        redisPort = getAvailablePort();
        redisServer = new RedisServer(redisPort);
        redisServer.start();
    }

    @AfterClass
    public static void cleanup() {
        if (redisServer != null) {
            redisServer.stop();
        }
    }

    @Before
    public void prepare() {
        Jedis jedis = new Jedis("localhost", redisPort);
        // Deletes all keys from all databases.
        jedis.flushAll();
    }

    public static int getAvailablePort() {
        for (int i = 0; i < 50; i++) {
            try (ServerSocket serverSocket = new ServerSocket(0)) {
                int port = serverSocket.getLocalPort();
                if (port != 0) {
                    return port;
                }
            } catch (IOException ignored) {
            }
        }
        throw new RuntimeException("Could not find a free permitted port on the machine.");
    }

    @Test
    public void testSourceWithGet() {
        StreamExecutionEnvironment executionEnv =
                StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv =
                StreamTableEnvironment.create(executionEnv);
        Jedis jedis = new Jedis("127.0.0.1", redisPort);

        jedis.set("1", "value_1");
        jedis.set("2", "value_2");

        String dim = "CREATE TABLE dim (" +
                "    aaa varchar, bbb varchar" +
                ") WITH (" +
                "  'connector' = 'redis-inlong'," +
                "  'command' = 'get'," +
                "  'host' = 'localhost'," +
                "  'port' = '" + redisPort + "'," +
                "  'maxIdle' = '8'," +
                "  'minIdle' = '1'," +
                "  'maxTotal' = '2'," +
                "  'timeout' = '2000'" +
                ")";
        String source =
                "create table source(aaa varchar, proctime as procTime()) "
                        + "with ('connector'='datagen',  'rows-per-second'='1', "
                        + "'fields.aaa.kind'='sequence',  'fields.aaa.start'='1',  'fields.aaa.end'='2'"
                        + ")";
        String sink = "CREATE TABLE sink (" +
                "    aaa STRING," +
                "    bbb STRING," +
                "    PRIMARY KEY (`aaa`) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'redis-inlong'," +
                "  'sink.batch-size' = '1'," +
                "  'format' = 'csv'," +
                "  'data-type' = 'PLAIN'," +
                "  'redis-mode' = 'standalone'," +
                "  'host' = '127.0.0.1'," +
                "  'port' = '" + redisPort + "'," +
                "  'maxIdle' = '8'," +
                "  'minIdle' = '1'," +
                "  'maxTotal' = '2'," +
                "  'timeout' = '2000'" +
                ")";

        tableEnv.executeSql(dim);
        tableEnv.executeSql(source);
        tableEnv.executeSql(sink);
        String sql =
                " insert into sink"
                        + " select concat_ws('_', s.aaa, s.aaa),concat_ws('_', d.bbb, s.aaa) from source s"
                        + "  left join dim for system_time as of s.proctime as d "
                        + " on d.aaa = s.aaa";

        tableEnv.executeSql(sql);

        await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals("value_1_1", jedis.get("1_1"));
            assertEquals("value_2_2", jedis.get("2_2"));
        });
        //
    }

    /**
     * hget only support get data from the given one key
     */
    @Test
    public void testSourceWithHget() {
        StreamExecutionEnvironment executionEnv =
                StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv =
                StreamTableEnvironment.create(executionEnv);
        Jedis jedis = new Jedis("127.0.0.1", redisPort);

        jedis.hset("1", "1", "value_1");
        jedis.hset("1", "2", "value_2");

        String dim = "CREATE TABLE dim (" +
                "    aaa varchar, bbb varchar" +
                ") WITH (" +
                "  'connector' = 'redis-inlong'," +
                "  'command' = 'hget'," +
                "  'host' = 'localhost'," +
                "  'port' = '" + redisPort + "'," +
                "  'maxIdle' = '8'," +
                "  'minIdle' = '1'," +
                "  'maxTotal' = '2'," +
                "  'timeout' = '2000'," +
                "  'additional.key' = '1'" +
                ")";
        String source =
                "create table source(aaa varchar,bbb varchar, proctime as procTime()) "
                        + "with ('connector'='datagen',  'rows-per-second'='1', "
                        + "'fields.aaa.kind'='sequence',  'fields.aaa.start'='1',  'fields.aaa.end'='2'"
                        + ")";
        String sink = "CREATE TABLE sink (" +
                "    aaa STRING," +
                "    bbb STRING," +
                "    ccc varchar," +
                "    PRIMARY KEY (`aaa`) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'redis-inlong'," +
                "  'sink.batch-size' = '1'," +
                "  'format' = 'csv'," +
                "  'data-type' = 'HASH'," +
                "  'redis-mode' = 'standalone'," +
                "  'host' = '127.0.0.1'," +
                "  'port' = '" + redisPort + "'," +
                "  'maxIdle' = '8'," +
                "  'minIdle' = '1'," +
                "  'maxTotal' = '2'," +
                "  'timeout' = '2000'" +
                ")";

        tableEnv.executeSql(dim);
        tableEnv.executeSql(source);
        tableEnv.executeSql(sink);
        String sql =
                " insert into sink"
                        + " select '1_1',d.aaa, d.bbb from source s"
                        + "  left join dim for system_time as of s.proctime as d "
                        + " on d.aaa = s.aaa and d.bbb = s.bbb";

        tableEnv.executeSql(sql);

        await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals("value_1", jedis.hget("1_1", "1"));
            assertEquals("value_2", jedis.hget("1_1", "2"));
        });
    }
    @Test
    public void testSourceWithZrevRank() {
        StreamExecutionEnvironment executionEnv =
                StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv =
                StreamTableEnvironment.create(executionEnv);
        Jedis jedis = new Jedis("127.0.0.1", redisPort);

        jedis.zadd("rank", 10, "1");
        jedis.zadd("rank", 20, "2");
        jedis.zadd("rank", 30, "3");

        String dim = "CREATE TABLE dim (" +
                "    member_test varchar," +
                "    member_rank bigint" +
                ") WITH (" +
                "  'connector' = 'redis-inlong'," +
                "  'command' = 'zrevrank'," +
                "  'host' = 'localhost'," +
                "  'port' = '" + redisPort + "'," +
                "  'maxIdle' = '8'," +
                "  'minIdle' = '1'," +
                "  'maxTotal' = '2'," +
                "  'timeout' = '2000'," +
                "  'additional.key' = 'rank'" +
                ")";

        String source =
                "create table source(member_test varchar, proctime as procTime()) "
                        + "with ('connector'='datagen',  'rows-per-second'='1', "
                        + "'fields.member_test.kind'='sequence',  'fields.member_test.start'='1',  'fields.member_test.end'='3'"
                        + ")";
        String sink = "CREATE TABLE sink (" +
                "    aaa STRING," +
                "    bbb STRING," +
                "    ccc BIGINT," +
                "    PRIMARY KEY (`aaa`) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'redis-inlong'," +
                "  'sink.batch-size' = '1'," +
                "  'format' = 'csv'," +
                "  'data-type' = 'HASH'," +
                "  'redis-mode' = 'standalone'," +
                "  'host' = '127.0.0.1'," +
                "  'port' = '" + redisPort + "'," +
                "  'maxIdle' = '8'," +
                "  'minIdle' = '1'," +
                "  'maxTotal' = '2'," +
                "  'timeout' = '2000'" +
                ")";

        tableEnv.executeSql(dim);
        tableEnv.executeSql(source);
        tableEnv.executeSql(sink);
        String sql =
                " insert into sink"
                        + " select 'rank_test',s.member_test, d.member_rank from source s"
                        + "  left join dim for system_time as of s.proctime as d "
                        + "on d.member_test = s.member_test";

        tableEnv.executeSql(sql);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals("2", jedis.hget("rank_test", "1"));
            assertEquals("1", jedis.hget("rank_test", "2"));
            assertEquals("0", jedis.hget("rank_test", "3"));
        });
    }
    @Test
    public void testSourceWithZscore() {
        StreamExecutionEnvironment executionEnv =
                StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv =
                StreamTableEnvironment.create(executionEnv);
        Jedis jedis = new Jedis("127.0.0.1", redisPort);

        jedis.zadd("rank_score", 10, "1");
        jedis.zadd("rank_score", 20, "2");
        jedis.zadd("rank_score", 30, "3");

        String dim = "CREATE TABLE dim (" +
                "    member_test varchar, score double" +
                ") WITH (" +
                "  'connector' = 'redis-inlong'," +
                "  'command' = 'zscore'," +
                "  'host' = 'localhost'," +
                "  'port' = '" + redisPort + "'," +
                "  'maxIdle' = '8'," +
                "  'minIdle' = '1'," +
                "  'maxTotal' = '2'," +
                "  'timeout' = '2000'," +
                "  'additional.key' = 'rank_score'" +
                ")";
        String source =
                "create table source(member_test varchar, proctime as procTime()) "
                        + "with ('connector'='datagen',  'rows-per-second'='1', "
                        + "'fields.member_test.kind'='sequence',  'fields.member_test.start'='1',  'fields.member_test.end'='3'"
                        + ")";
        String sink = "CREATE TABLE sink (" +
                "    aaa STRING," +
                "    bbb STRING," +
                "    ccc double," +
                "    PRIMARY KEY (`aaa`) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'redis-inlong'," +
                "  'sink.batch-size' = '1'," +
                "  'format' = 'csv'," +
                "  'data-type' = 'HASH'," +
                "  'redis-mode' = 'standalone'," +
                "  'host' = '127.0.0.1'," +
                "  'port' = '" + redisPort + "'," +
                "  'maxIdle' = '8'," +
                "  'minIdle' = '1'," +
                "  'maxTotal' = '2'," +
                "  'timeout' = '2000'" +
                ")";

        tableEnv.executeSql(dim);
        tableEnv.executeSql(source);
        tableEnv.executeSql(sink);
        String sql =
                " insert into sink"
                        + " select 'rank_score_test',d.member_test, d.score from source s"
                        + "  left join dim for system_time as of s.proctime as d "
                        + " on d.member_test = s.member_test";

        tableEnv.executeSql(sql);

        await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals("10.0", jedis.hget("rank_score_test", "1"));
            assertEquals("20.0", jedis.hget("rank_score_test", "2"));
            assertEquals("30.0", jedis.hget("rank_score_test", "3"));
        });
    }

    @Test
    public void testSinkWithPlain() {
        StreamExecutionEnvironment executionEnv =
                StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv =
                StreamTableEnvironment.create(executionEnv);

        executionEnv.setParallelism(1);

        DataStream<Row> source =
                executionEnv.fromCollection(
                        Arrays.asList(
                                Row.of("1", "r12", 1.2, 1),
                                Row.of("2", "r22", 2.2, 2),
                                Row.of("3", "r32", 3.2, 3)));
        tableEnv.registerDataStream("source", source, "aaa, bbb, ccc, ddd");

        tableEnv.executeSql("CREATE TABLE sink (" +
                "    key STRING," +
                "    aaa STRING," +
                "    bbb DOUBLE," +
                "    ccc BIGINT," +
                "    PRIMARY KEY (`key`) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'redis-inlong'," +
                "  'sink.batch-size' = '1'," +
                "  'format' = 'csv'," +
                "  'data-type' = 'PLAIN'," +
                "  'redis-mode' = 'standalone'," +
                "  'host' = '127.0.0.1'," +
                "  'port' = '" + redisPort + "'," +
                "  'maxIdle' = '8'," +
                "  'minIdle' = '1'," +
                "  'maxTotal' = '2'," +
                "  'timeout' = '2000'" +
                ")");
        Jedis jedis = new Jedis("127.0.0.1", redisPort);

        String query = "INSERT INTO sink SELECT * FROM source";
        tableEnv.executeSql(query);

        await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals("r12,1.2,1", jedis.get("1"));
            assertEquals("r22,2.2,2", jedis.get("2"));
            assertEquals("r32,3.2,3", jedis.get("3"));
        });

    }

    @Test
    public void testSinkWithHashPrefixMatch() {
        StreamExecutionEnvironment executionEnv =
                StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv =
                StreamTableEnvironment.create(executionEnv);

        executionEnv.setParallelism(1);

        DataStream<Row> source =
                executionEnv.fromCollection(
                        Arrays.asList(
                                Row.of("1", "r12", 1.2, 1),
                                Row.of("2", "r22", 2.2, 2),
                                Row.of("3", "r32", 3.2, 3)));
        tableEnv.registerDataStream("source", source, "aaa, bbb, ccc, ddd");

        tableEnv.executeSql("CREATE TABLE sink (" +
                "    key STRING," +
                "    aaa STRING," +
                "    bbb DOUBLE," +
                "    ccc BIGINT," +
                "    PRIMARY KEY (`key`) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'redis-inlong'," +
                "  'sink.batch-size' = '1'," +
                "  'format' = 'csv'," +
                "  'data-type' = 'HASH'," +
                "  'redis-mode' = 'standalone'," +
                "  'host' = 'localhost'," +
                "  'port' = '" + redisPort + "'," +
                "  'maxIdle' = '8'," +
                "  'minIdle' = '1'," +
                "  'maxTotal' = '2'," +
                "  'timeout' = '2000'" +
                ")");
        Jedis jedis = new Jedis("localhost", redisPort);
        assertNull(jedis.get("1"));
        assertNull(jedis.get("2"));
        assertNull(jedis.get("3"));

        String query = "INSERT INTO sink SELECT * FROM source";
        tableEnv.executeSql(query);

        await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals("1.2,1", jedis.hget("1", "r12"));
            assertEquals("2.2,2", jedis.hget("2", "r22"));
            assertEquals("3.2,3", jedis.hget("3", "r32"));
        });

    }

    @Test
    public void testSinkWithHashKvPair() {
        StreamExecutionEnvironment executionEnv =
                StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv =
                StreamTableEnvironment.create(executionEnv);

        executionEnv.setParallelism(1);

        DataStream<Row> source =
                executionEnv.fromCollection(
                        Arrays.asList(
                                Row.of("1", "r12", 1.2, "r14", 1),
                                Row.of("2", "r22", 2.2, "r24", 2),
                                Row.of("3", "r32", 3.2, "r34", 3)));
        tableEnv.registerDataStream("source", source, "aaa, bbb, ccc, ddd,eee");

        tableEnv.executeSql("CREATE TABLE sink (" +
                "    key STRING," +
                "    aaa STRING," +
                "    bbb STRING," +
                "    ccc STRING," +
                "    ddd STRING," +
                "    PRIMARY KEY (`key`) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'redis-inlong'," +
                "  'sink.batch-size' = '1'," +
                "  'format' = 'csv'," +
                "  'data-type' = 'HASH'," +
                "  'schema-mapping-mode' = 'STATIC_KV_PAIR'," +
                "  'redis-mode' = 'standalone'," +
                "  'host' = 'localhost'," +
                "  'port' = '" + redisPort + "'," +
                "  'maxIdle' = '8'," +
                "  'minIdle' = '1'," +
                "  'maxTotal' = '2'," +
                "  'timeout' = '2000'" +
                ")");
        Jedis jedis = new Jedis("localhost", redisPort);
        assertNull(jedis.get("1"));
        assertNull(jedis.get("2"));
        assertNull(jedis.get("3"));

        String query = "INSERT INTO sink SELECT aaa,bbb,cast(ccc as STRING),ddd, cast(eee as STRING) FROM source";
        tableEnv.executeSql(query);

        await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals("1.2", jedis.hget("1", "r12"));
            assertEquals("2.2", jedis.hget("2", "r22"));
            assertEquals("3.2", jedis.hget("3", "r32"));
            assertEquals("1", jedis.hget("1", "r14"));
            assertEquals("2", jedis.hget("2", "r24"));
            assertEquals("3", jedis.hget("3", "r34"));
        });

    }

    @Test
    public void testSinkWithDynamic() {
        StreamExecutionEnvironment executionEnv =
                StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv =
                StreamTableEnvironment.create(executionEnv);

        executionEnv.setParallelism(1);

        DataStream<Row> source =
                executionEnv.fromCollection(
                        Arrays.asList(
                                Row.of("1", "r12", 1.2, "r14", 1),
                                Row.of("2", "r22", 2.2, "r24", 2),
                                Row.of("3", "r32", 3.2, "r34", 3)));
        tableEnv.registerDataStream("source", source, "aaa, bbb, ccc, ddd,eee");

        tableEnv.executeSql("CREATE TABLE sink (" +
                "    key STRING," +
                "    aaa MAP<STRING,STRING>," +
                "    PRIMARY KEY (`key`) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'redis-inlong'," +
                "  'sink.batch-size' = '1'," +
                "  'format' = 'csv'," +
                "  'data-type' = 'HASH'," +
                "  'schema-mapping-mode' = 'DYNAMIC'," +
                "  'redis-mode' = 'standalone'," +
                "  'host' = 'localhost'," +
                "  'port' = '" + redisPort + "'," +
                "  'maxIdle' = '8'," +
                "  'minIdle' = '1'," +
                "  'maxTotal' = '2'," +
                "  'timeout' = '2000'" +
                ")");
        Jedis jedis = new Jedis("localhost", redisPort);
        assertNull(jedis.get("1"));
        assertNull(jedis.get("2"));
        assertNull(jedis.get("3"));

        String query = "INSERT INTO sink "
                + "SELECT "
                + "aaa as key, "
                + "MAP[bbb,cast(ccc as STRING),ddd,cast(eee as STRING)] as fv "
                + "FROM source";
        tableEnv.executeSql(query);

        await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals("1.2", jedis.hget("1", "r12"));
            assertEquals("2.2", jedis.hget("2", "r22"));
            assertEquals("3.2", jedis.hget("3", "r32"));
            assertEquals("1", jedis.hget("1", "r14"));
            assertEquals("2", jedis.hget("2", "r24"));
            assertEquals("3", jedis.hget("3", "r34"));
        });

    }

    @Test
    public void testSinkWithBitmap() throws Exception {
        StreamExecutionEnvironment executionEnv =
                StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv =
                StreamTableEnvironment.create(executionEnv);

        executionEnv.setParallelism(1);

        DataStream<Row> source =
                executionEnv.fromCollection(
                        Arrays.asList(
                                Row.of("1", 2, 1.2, 4, 1),
                                Row.of("2", 2, 2.2, 4, 2),
                                Row.of("3", 2, 3.2, 4, 3)));
        tableEnv.registerDataStream("source", source, "aaa, bbb, ccc, ddd,eee");

        tableEnv.executeSql("CREATE TABLE sink (" +
                "    key STRING," +
                "    aaa BIGINT," +
                "    bbb BOOLEAN," +
                "    ccc BIGINT," +
                "    ddd BOOLEAN," +
                "    PRIMARY KEY (`key`) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'redis-inlong'," +
                "  'sink.batch-size' = '1'," +
                "  'format' = 'csv'," +
                "  'data-type' = 'BITMAP'," +
                "  'schema-mapping-mode' = 'STATIC_KV_PAIR'," +
                "  'redis-mode' = 'standalone'," +
                "  'host' = 'localhost'," +
                "  'port' = '" + redisPort + "'," +
                "  'maxIdle' = '8'," +
                "  'minIdle' = '1'," +
                "  'maxTotal' = '2'," +
                "  'timeout' = '2000'" +
                ")");
        Jedis jedis = new Jedis("localhost", redisPort);
        assertNull(jedis.get("1"));
        assertNull(jedis.get("2"));
        assertNull(jedis.get("3"));

        String query = "INSERT INTO sink "
                + "SELECT "
                + "aaa as key, "
                + "bbb,"
                + "ccc = cast(1.2 as double) as cccc,"
                + "ddd,"
                + "eee = cast(1 as bigint) as eeee "
                + "FROM source";
        tableEnv.executeSql(query);

        Thread.sleep(4000);

        await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
            assertTrue(jedis.getbit("1", 2));
            assertTrue(jedis.getbit("1", 4));
            assertFalse(jedis.getbit("2", 2));
            assertFalse(jedis.getbit("2", 4));
            assertFalse(jedis.getbit("3", 2));
            assertFalse(jedis.getbit("3", 4));
        });

    }

}
