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

package org.apache.inlong.sort.weaviate.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

/**
 * Option utils for Weaviate table source sink.
 * Defines all configuration options for Weaviate connector.
 */
public class WeaviateOptions {

    private WeaviateOptions() {
    }

    // --------------------------------------------------------------------------------------------
    // Required options
    // --------------------------------------------------------------------------------------------

    /**
     * Weaviate server URL.
     */
    public static final ConfigOption<String> URL = ConfigOptions
            .key("url")
            .stringType()
            .noDefaultValue()
            .withDescription("Weaviate server URL");

    /**
     * Weaviate class name.
     */
    public static final ConfigOption<String> CLASS_NAME = ConfigOptions
            .key("class-name")
            .stringType()
            .noDefaultValue()
            .withDescription("Weaviate class name");

    // --------------------------------------------------------------------------------------------
    // Optional options
    // --------------------------------------------------------------------------------------------

    /**
     * Weaviate API key for authentication.
     */
    public static final ConfigOption<String> API_KEY = ConfigOptions
            .key("api-key")
            .stringType()
            .noDefaultValue()
            .withDescription("Weaviate API key for authentication");

    /**
     * Batch size for operations.
     */
    public static final ConfigOption<Integer> BATCH_SIZE = ConfigOptions
            .key("batch-size")
            .intType()
            .defaultValue(100)
            .withDescription("Number of records to batch before sending to Weaviate");

    /**
     * Maximum time to wait before flushing batch.
     */
    public static final ConfigOption<Duration> FLUSH_INTERVAL = ConfigOptions
            .key("flush-interval")
            .durationType()
            .defaultValue(Duration.ofSeconds(5))
            .withDescription("Maximum time to wait before flushing batch");

    /**
     * Vector field name for vector operations.
     */
    public static final ConfigOption<String> VECTOR_FIELD = ConfigOptions
            .key("vector-field")
            .stringType()
            .noDefaultValue()
            .withDescription("Name of the vector field in Weaviate class");

    /**
     * Vector dimension for vector field validation.
     */
    public static final ConfigOption<Integer> VECTOR_DIMENSION = ConfigOptions
            .key("vector-dimension")
            .intType()
            .noDefaultValue()
            .withDescription("Dimension of the vector field");

    /**
     * Connection timeout for Weaviate client.
     */
    public static final ConfigOption<Duration> CONNECTION_TIMEOUT = ConfigOptions
            .key("connection-timeout")
            .durationType()
            .defaultValue(Duration.ofSeconds(30))
            .withDescription("Connection timeout for Weaviate client");

    /**
     * Read timeout for Weaviate operations.
     */
    public static final ConfigOption<Duration> READ_TIMEOUT = ConfigOptions
            .key("read-timeout")
            .durationType()
            .defaultValue(Duration.ofSeconds(60))
            .withDescription("Read timeout for Weaviate operations");

    /**
     * Maximum number of retries for failed operations.
     */
    public static final ConfigOption<Integer> MAX_RETRIES = ConfigOptions
            .key("max-retries")
            .intType()
            .defaultValue(3)
            .withDescription("Maximum number of retries for failed operations");

    /**
     * Retry delay for failed operations.
     */
    public static final ConfigOption<Duration> RETRY_DELAY = ConfigOptions
            .key("retry-delay")
            .durationType()
            .defaultValue(Duration.ofSeconds(1))
            .withDescription("Delay between retries for failed operations");

    // --------------------------------------------------------------------------------------------
    // Source specific options
    // --------------------------------------------------------------------------------------------

    /**
     * GraphQL query conditions for source operations.
     */
    public static final ConfigOption<String> QUERY_CONDITIONS = ConfigOptions
            .key("query-conditions")
            .stringType()
            .defaultValue("limit: 1000")
            .withDescription("GraphQL query conditions for source operations");

    /**
     * Query limit for source operations.
     */
    public static final ConfigOption<Integer> QUERY_LIMIT = ConfigOptions
            .key("query-limit")
            .intType()
            .defaultValue(1000)
            .withDescription("Maximum number of records to fetch in a single query");

    /**
     * Query offset for source operations.
     */
    public static final ConfigOption<Integer> QUERY_OFFSET = ConfigOptions
            .key("query-offset")
            .intType()
            .defaultValue(0)
            .withDescription("Offset for source query operations");

    // --------------------------------------------------------------------------------------------
    // Sink specific options
    // --------------------------------------------------------------------------------------------

    /**
     * Whether to enable upsert mode for sink operations.
     */
    public static final ConfigOption<Boolean> UPSERT_ENABLED = ConfigOptions
            .key("upsert-enabled")
            .booleanType()
            .defaultValue(true)
            .withDescription("Whether to enable upsert mode for sink operations");

    /**
     * Maximum memory usage for batch buffer.
     */
    public static final ConfigOption<String> MAX_BATCH_MEMORY = ConfigOptions
            .key("max-batch-memory")
            .stringType()
            .defaultValue("64MB")
            .withDescription("Maximum memory usage for batch buffer");

    /**
     * Whether to ignore null values in sink operations.
     */
    public static final ConfigOption<Boolean> IGNORE_NULL_VALUES = ConfigOptions
            .key("ignore-null-values")
            .booleanType()
            .defaultValue(false)
            .withDescription("Whether to ignore null values in sink operations");

    /**
     * Consistency level for write operations.
     */
    public static final ConfigOption<String> CONSISTENCY_LEVEL = ConfigOptions
            .key("consistency-level")
            .stringType()
            .defaultValue("ONE")
            .withDescription("Consistency level for write operations (ONE, QUORUM, ALL)");

    // --------------------------------------------------------------------------------------------
    // Performance optimization options
    // --------------------------------------------------------------------------------------------

    /**
     * Whether to use connection pool for client management.
     */
    public static final ConfigOption<Boolean> USE_CONNECTION_POOL = ConfigOptions
            .key("use-connection-pool")
            .booleanType()
            .defaultValue(true)
            .withDescription("Whether to use connection pool for client management");

    /**
     * Connection pool size.
     */
    public static final ConfigOption<Integer> CONNECTION_POOL_SIZE = ConfigOptions
            .key("connection-pool-size")
            .intType()
            .defaultValue(10)
            .withDescription("Maximum number of connections in the pool");

    /**
     * Memory pressure threshold for triggering automatic flush.
     */
    public static final ConfigOption<Double> MEMORY_PRESSURE_THRESHOLD = ConfigOptions
            .key("memory-pressure-threshold")
            .doubleType()
            .defaultValue(0.8)
            .withDescription("Memory pressure threshold (0.0-1.0) for triggering automatic flush");

    /**
     * Backpressure delay when system is under pressure.
     */
    public static final ConfigOption<Duration> BACKPRESSURE_DELAY = ConfigOptions
            .key("backpressure-delay")
            .durationType()
            .defaultValue(Duration.ofMillis(100))
            .withDescription("Delay to apply when backpressure is detected");

    /**
     * Maximum number of concurrent batch operations.
     */
    public static final ConfigOption<Integer> MAX_CONCURRENT_BATCHES = ConfigOptions
            .key("max-concurrent-batches")
            .intType()
            .defaultValue(3)
            .withDescription("Maximum number of concurrent batch flush operations");

    /**
     * Memory monitoring interval.
     */
    public static final ConfigOption<Duration> MEMORY_MONITOR_INTERVAL = ConfigOptions
            .key("memory-monitor-interval")
            .durationType()
            .defaultValue(Duration.ofSeconds(1))
            .withDescription("Interval for memory usage monitoring");

    /**
     * Query batch size for source operations.
     */
    public static final ConfigOption<Integer> QUERY_BATCH_SIZE = ConfigOptions
            .key("query-batch-size")
            .intType()
            .defaultValue(1000)
            .withDescription("Batch size for GraphQL query operations");

    /**
     * Whether to enable query result caching.
     */
    public static final ConfigOption<Boolean> ENABLE_QUERY_CACHE = ConfigOptions
            .key("enable-query-cache")
            .booleanType()
            .defaultValue(false)
            .withDescription("Whether to enable query result caching for repeated queries");

    /**
     * Query cache TTL (time to live).
     */
    public static final ConfigOption<Duration> QUERY_CACHE_TTL = ConfigOptions
            .key("query-cache-ttl")
            .durationType()
            .defaultValue(Duration.ofMinutes(5))
            .withDescription("Time to live for cached query results");
}