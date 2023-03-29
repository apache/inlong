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

package org.apache.inlong.manager.service.resource.sink.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * A client for interacting with Redis resources.
 */
public class RedisResourceClient implements AutoCloseable {

    private final JedisPool jedisPool;

    /**
     * Constructs a new RedisResourceClient with the given host, port, and password.
     *
     * @param host     the Redis server host
     * @param port     the Redis server port
     * @param password the Redis server password
     * @param database the Redis server database
     */
    public RedisResourceClient(String host, int port, String password, int database) {

        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxTotal(10);
        poolConfig.setMaxIdle(5);
        poolConfig.setMinIdle(1);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setMinEvictableIdleTimeMillis(60000);
        poolConfig.setTimeBetweenEvictionRunsMillis(30000);
        poolConfig.setNumTestsPerEvictionRun(-1);
        this.jedisPool = new JedisPool(poolConfig, host, port, 5000, password, database);
    }

    /**
     * Closes the RedisResourceClient and releases any resources associated with it.
     */
    public void close() {
        jedisPool.close();
    }

    /**
     * Gets a Jedis resource from the RedisResourceClient's JedisPool.
     *
     * @return a Jedis resource
     */
    public Jedis getResource() {
        return jedisPool.getResource();
    }

    /**
     * Tests the connection to the Redis server.
     *
     * @return true if the connection is successful, false otherwise
     */
    public boolean testConnection() {
        try (Jedis jedis = getResource()) {
            return jedis.ping().equals("PONG");
        }
    }
}
