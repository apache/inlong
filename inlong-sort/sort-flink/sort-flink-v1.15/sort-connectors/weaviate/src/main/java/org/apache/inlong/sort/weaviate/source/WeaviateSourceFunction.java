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

package org.apache.inlong.sort.weaviate.source;

import org.apache.inlong.sort.base.metric.MetricOption;
import org.apache.inlong.sort.base.metric.SourceExactlyMetric;
import org.apache.inlong.sort.weaviate.table.WeaviateOptions;
import org.apache.inlong.sort.weaviate.utils.WeaviateClientUtils;
import org.apache.inlong.sort.weaviate.utils.WeaviateErrorHandler;
import org.apache.inlong.sort.weaviate.utils.WeaviateErrorHandler.WeaviateErrorCode;
import org.apache.inlong.sort.weaviate.utils.WeaviateErrorHandler.WeaviateException;
import org.apache.inlong.sort.weaviate.utils.WeaviateMonitoringUtils;
import org.apache.inlong.sort.weaviate.utils.WeaviateTypeConverter;

import io.weaviate.client.WeaviateClient;
import io.weaviate.client.v1.graphql.model.GraphQLResponse;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Enhanced Weaviate source function with GraphQL query performance
 * optimizations.
 * Features include:
 * - Optimized GraphQL query batching
 * - Query result caching
 * - Connection pool support
 * - Enhanced error handling and retry mechanisms
 * - Performance monitoring and metrics
 */
public class WeaviateSourceFunction extends RichSourceFunction<RowData> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(WeaviateSourceFunction.class);

    private final ReadableConfig config;
    private final DataType physicalDataType;
    private final MetricOption metricOption;

    // Runtime state
    private transient WeaviateClient client;
    private transient WeaviateClientUtils.WeaviateConnectionPool connectionPool;
    private transient SourceExactlyMetric sourceExactlyMetric;
    private transient WeaviateTypeConverter typeConverter;
    private transient volatile AtomicBoolean isRunning;
    private transient ConcurrentHashMap<String, CachedQueryResult> queryCache;
    private transient ScheduledExecutorService cacheCleanupExecutor;

    // Enhanced monitoring and error handling
    private transient WeaviateMonitoringUtils.EnhancedMetrics enhancedMetrics;
    private transient WeaviateMonitoringUtils.ConnectionHealthMonitor healthMonitor;

    // Configuration
    private String url;
    private String apiKey;
    private String className;
    private String queryConditions;
    private Integer queryLimit;
    private Integer queryOffset;
    private String vectorField;
    private Integer vectorDimension;
    private Integer maxRetries;
    private Duration retryDelay;

    // Performance optimization settings
    private Boolean useConnectionPool;
    private Integer connectionPoolSize;
    private Integer queryBatchSize;
    private Boolean enableQueryCache;
    private Duration queryCacheTtl;

    public WeaviateSourceFunction(
            ReadableConfig config,
            DataType physicalDataType,
            MetricOption metricOption) {
        this.config = config;
        this.physicalDataType = physicalDataType;
        this.metricOption = metricOption;
        this.isRunning = new AtomicBoolean(false);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        WeaviateMonitoringUtils.PerformanceTracker tracker = WeaviateMonitoringUtils
                .startOperation("WeaviateSourceFunction.open", LOG);

        try {
            LOG.info("Opening WeaviateSourceFunction with enhanced error handling and monitoring");

            // Initialize configuration with validation
            initializeConfiguration();

            // Initialize enhanced monitoring
            initializeEnhancedMonitoring();

            // Initialize Weaviate client with retry and health monitoring
            initializeClient();

            // Initialize type converter with validation
            initializeTypeConverter();

            // Initialize metrics with enhanced reporting
            initializeMetrics();

            // Initialize query caching if enabled
            initializeQueryCache();

            // Log system resources for monitoring
            WeaviateMonitoringUtils.logSystemResources(LOG);

            tracker.recordSuccess(1, 0);
            LOG.info("WeaviateSourceFunction opened successfully with enhanced monitoring and query cache: {}",
                    enableQueryCache);

        } catch (Exception e) {
            WeaviateException weaviateException = WeaviateErrorHandler.wrapException(e,
                    "WeaviateSourceFunction.open failed");
            tracker.recordFailure(weaviateException, 0);
            throw weaviateException;
        }
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        isRunning.set(true);

        LOG.info("Starting WeaviateSourceFunction data reading");

        try {
            // Execute GraphQL query and process results
            executeQueryAndEmitData(ctx);
        } catch (Exception e) {
            LOG.error("Error during data reading from Weaviate", e);
            throw e;
        } finally {
            isRunning.set(false);
        }

        LOG.info("WeaviateSourceFunction data reading completed");
    }

    @Override
    public void cancel() {
        LOG.info("Cancelling WeaviateSourceFunction");
        isRunning.set(false);
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing WeaviateSourceFunction");

        isRunning.set(false);

        // Shutdown cache cleanup executor
        if (cacheCleanupExecutor != null && !cacheCleanupExecutor.isShutdown()) {
            cacheCleanupExecutor.shutdown();
            try {
                if (!cacheCleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    cacheCleanupExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                cacheCleanupExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        // Return client to pool or close
        if (useConnectionPool && connectionPool != null && client != null) {
            connectionPool.returnClient(client);
        }
        client = null;

        // Clear cache
        if (queryCache != null) {
            queryCache.clear();
        }

        super.close();
        LOG.info("WeaviateSourceFunction closed");
    }

    /**
     * 
     * Initialize enhanced monitoring and health tracking.
     */
    private void initializeEnhancedMonitoring() {
        // Initialize enhanced metrics
        enhancedMetrics = WeaviateMonitoringUtils.createMetrics(
                "WeaviateSourceFunction", metricOption, getRuntimeContext().getMetricGroup(), true);

        // Initialize connection health monitor
        healthMonitor = WeaviateMonitoringUtils.createHealthMonitor(url);

        LOG.info("Enhanced monitoring initialized for WeaviateSourceFunction");
    }

    /**
     * Initialize configuration from ReadableConfig with enhanced validation.
     */
    private void initializeConfiguration() throws WeaviateException {
        // Enhanced configuration validation
        url = config.get(WeaviateOptions.URL);
        WeaviateErrorHandler.validateConfig("url", url, "WeaviateSourceFunction");

        apiKey = config.getOptional(WeaviateOptions.API_KEY).orElse(null);

        className = config.get(WeaviateOptions.CLASS_NAME);
        WeaviateErrorHandler.validateConfig("className", className, "WeaviateSourceFunction");

        queryConditions = config.get(WeaviateOptions.QUERY_CONDITIONS);
        queryLimit = config.get(WeaviateOptions.QUERY_LIMIT);
        queryOffset = config.get(WeaviateOptions.QUERY_OFFSET);

        if (queryLimit <= 0) {
            throw new WeaviateException(WeaviateErrorCode.INVALID_CONFIG,
                    WeaviateErrorHandler.createContext("WeaviateSourceFunction", "queryLimit", queryLimit));
        }
        if (queryOffset < 0) {
            throw new WeaviateException(WeaviateErrorCode.INVALID_CONFIG,
                    WeaviateErrorHandler.createContext("WeaviateSourceFunction", "queryOffset", queryOffset));
        }

        vectorField = config.getOptional(WeaviateOptions.VECTOR_FIELD).orElse(null);
        vectorDimension = config.getOptional(WeaviateOptions.VECTOR_DIMENSION).orElse(null);
        maxRetries = config.get(WeaviateOptions.MAX_RETRIES);
        retryDelay = config.get(WeaviateOptions.RETRY_DELAY);

        // Performance optimization settings
        useConnectionPool = config.get(WeaviateOptions.USE_CONNECTION_POOL);
        connectionPoolSize = config.get(WeaviateOptions.CONNECTION_POOL_SIZE);
        queryBatchSize = config.get(WeaviateOptions.QUERY_BATCH_SIZE);
        enableQueryCache = config.get(WeaviateOptions.ENABLE_QUERY_CACHE);
        queryCacheTtl = config.get(WeaviateOptions.QUERY_CACHE_TTL);

        LOG.info("Initialized configuration: url={}, className={}, queryLimit={}, queryOffset={}, vectorField={}, " +
                "useConnectionPool={}, queryBatchSize={}, enableQueryCache={}",
                url, className, queryLimit, queryOffset, vectorField, useConnectionPool, queryBatchSize,
                enableQueryCache);
    }

    /**
     * Initialize Weaviate client with connection pooling support.
     */
    private void initializeClient() throws Exception {
        LOG.info("Initializing Weaviate client with connection pool: {}", useConnectionPool);

        try {
            if (useConnectionPool) {
                // Use connection pool for better resource management
                connectionPool = WeaviateClientUtils.getConnectionPool(url, apiKey, connectionPoolSize);
                client = connectionPool.borrowClient();
                LOG.info("Using connection pool with size: {}", connectionPoolSize);
            } else {
                // Use single client instance
                client = WeaviateClientUtils.createClientWithRetry(url, apiKey, maxRetries, retryDelay);
            }

            // Test connection
            if (!WeaviateClientUtils.testConnection(client)) {
                throw new RuntimeException("Failed to connect to Weaviate server: " + url);
            }

            LOG.info("Weaviate client initialized successfully");
        } catch (Exception e) {
            LOG.error("Failed to initialize Weaviate client", e);
            throw new RuntimeException("Failed to initialize Weaviate client", e);
        }
    }

    /**
     * Initialize query caching if enabled.
     */
    private void initializeQueryCache() {
        if (enableQueryCache) {
            queryCache = new ConcurrentHashMap<>();

            // Setup cache cleanup executor
            cacheCleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "weaviate-cache-cleanup");
                t.setDaemon(true);
                return t;
            });

            // Schedule periodic cache cleanup
            cacheCleanupExecutor.scheduleAtFixedRate(
                    this::cleanupExpiredCacheEntries,
                    queryCacheTtl.toMillis(),
                    queryCacheTtl.toMillis(),
                    TimeUnit.MILLISECONDS);

            LOG.info("Query cache initialized with TTL: {}", queryCacheTtl);
        }
    }

    /**
     * Clean up expired cache entries.
     */
    private void cleanupExpiredCacheEntries() {
        if (queryCache != null) {
            int initialSize = queryCache.size();
            queryCache.entrySet().removeIf(entry -> entry.getValue().isExpired());
            int finalSize = queryCache.size();
            if (initialSize != finalSize) {
                LOG.debug("Cleaned up {} expired cache entries", initialSize - finalSize);
            }
        }
    }

    /**
     * Initialize type converter.
     */
    private void initializeTypeConverter() {
        RowType rowType = (RowType) physicalDataType.getLogicalType();
        typeConverter = new WeaviateTypeConverter(rowType, vectorField, vectorDimension);
        typeConverter.validate();

        LOG.info("Type converter initialized with vector field: {}, dimension: {}", vectorField, vectorDimension);
    }

    /**
     * Initialize metrics for InLong audit.
     */
    private void initializeMetrics() {
        if (metricOption != null) {
            sourceExactlyMetric = new SourceExactlyMetric(metricOption, getRuntimeContext().getMetricGroup());
            LOG.info("Source metrics initialized");
        } else {
            LOG.warn("MetricOption is null, metrics will not be reported");
        }
    }

    /**
     * Execute GraphQL query and emit data to Flink context.
     */
    private void executeQueryAndEmitData(SourceContext<RowData> ctx) throws Exception {
        int currentOffset = queryOffset;
        int recordsProcessed = 0;

        while (isRunning.get()) {
            try {
                // Build GraphQL query
                String graphqlQuery = buildGraphQLQuery(currentOffset);
                LOG.debug("Executing GraphQL query: {}", graphqlQuery);

                // Try to get results from cache first
                List<Map<String, Object>> results = getCachedResults(graphqlQuery);

                if (results == null) {
                    // Execute query with retry
                    GraphQLResponse response = executeQueryWithRetry(graphqlQuery);

                    // Process response
                    results = extractResultsFromResponse(response);

                    // Cache results if caching is enabled
                    cacheResults(graphqlQuery, results);
                }

                if (results == null || results.isEmpty()) {
                    LOG.info("No more data available, stopping source");
                    break;
                }

                // Convert and emit data
                for (Map<String, Object> weaviateObject : results) {
                    if (!isRunning.get()) {
                        break;
                    }

                    try {
                        // Convert Weaviate object to RowData
                        GenericRowData rowData = typeConverter.convertWeaviateObjectToRowData(weaviateObject);

                        // Emit data
                        synchronized (ctx.getCheckpointLock()) {
                            ctx.collect(rowData);
                        }

                        // Report metrics
                        reportMetrics(rowData);

                        recordsProcessed++;

                    } catch (Exception e) {
                        LOG.error("Failed to convert and emit Weaviate object: {}", weaviateObject, e);
                        // Continue processing other records
                    }
                }

                LOG.info("Processed {} records in this batch, total processed: {}",
                        results.size(), recordsProcessed);

                // Update offset for next query
                currentOffset += results.size();

                // If we got fewer results than the limit, we've reached the end
                if (results.size() < queryLimit) {
                    LOG.info("Reached end of data, processed {} total records", recordsProcessed);
                    break;
                }

            } catch (Exception e) {
                LOG.error("Error during query execution at offset {}", currentOffset, e);
                throw e;
            }
        }

        LOG.info("Data reading completed, total records processed: {}", recordsProcessed);
    }

    /**
     * Build GraphQL query string.
     */
    private String buildGraphQLQuery(int offset) {
        RowType rowType = (RowType) physicalDataType.getLogicalType();
        StringBuilder queryBuilder = new StringBuilder();

        queryBuilder.append("{ Get { ").append(className).append("(");

        // Add limit and offset
        queryBuilder.append("limit: ").append(queryLimit);
        queryBuilder.append(", offset: ").append(offset);

        // Add additional query conditions if specified
        if (queryConditions != null && !queryConditions.trim().isEmpty()
                && !queryConditions.trim().equals("limit: 1000")) {
            // Remove default limit from queryConditions if present
            String cleanConditions = queryConditions.replaceAll("limit:\\s*\\d+", "").trim();
            if (!cleanConditions.isEmpty()) {
                queryBuilder.append(", ").append(cleanConditions);
            }
        }

        queryBuilder.append(") { ");

        // Add field selections
        for (RowType.RowField field : rowType.getFields()) {
            queryBuilder.append(field.getName()).append(" ");
        }

        // Add _additional fields for metadata
        queryBuilder.append("_additional { id } ");

        queryBuilder.append("} } }");

        return queryBuilder.toString();
    }

    /**
     * Execute GraphQL query with retry mechanism.
     */
    private GraphQLResponse executeQueryWithRetry(String query) throws Exception {
        Exception lastException = null;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                // Use the raw GraphQL query approach
                GraphQLResponse response = client.graphQL().raw().withQuery(query).run().getResult();

                if (response.getErrors() != null && response.getErrors().length > 0) {
                    throw new RuntimeException(
                            "GraphQL query failed: " + java.util.Arrays.toString(response.getErrors()));
                }

                return response;

            } catch (Exception e) {
                lastException = e;
                LOG.warn("Query attempt {}/{} failed: {}", attempt, maxRetries, e.getMessage());

                if (attempt < maxRetries && isRunning.get()) {
                    try {
                        // Exponential backoff with jitter
                        long delayMs = retryDelay.toMillis() * (1L << (attempt - 1));
                        delayMs += (long) (Math.random() * 1000); // Add jitter
                        Thread.sleep(delayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted during retry", ie);
                    }
                }
            }
        }

        throw new RuntimeException("Failed to execute query after " + maxRetries + " attempts", lastException);
    }

    /**
     * Extract results from GraphQL response.
     */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> extractResultsFromResponse(GraphQLResponse response) {
        try {
            @SuppressWarnings("rawtypes")
            Map data = (Map) response.getData();
            if (data == null) {
                return null;
            }

            Object getObj = data.get("Get");
            if (getObj == null || !(getObj instanceof Map)) {
                return null;
            }

            @SuppressWarnings("rawtypes")
            Map get = (Map) getObj;
            Object classObj = get.get(className);

            if (classObj == null || !(classObj instanceof List)) {
                return null;
            }

            @SuppressWarnings("rawtypes")
            List result = (List) classObj;
            return result;

        } catch (Exception e) {
            LOG.error("Failed to extract results from GraphQL response", e);
            throw new RuntimeException("Failed to extract results from response", e);
        }
    }

    /**
     * Get cached query results if available and not expired.
     */
    private List<Map<String, Object>> getCachedResults(String query) {
        if (!enableQueryCache || queryCache == null) {
            return null;
        }

        CachedQueryResult cached = queryCache.get(query);
        if (cached != null && !cached.isExpired()) {
            LOG.debug("Using cached results for query");
            return cached.getResults();
        }

        return null;
    }

    /**
     * Cache query results if caching is enabled.
     */
    private void cacheResults(String query, List<Map<String, Object>> results) {
        if (enableQueryCache && queryCache != null && results != null) {
            queryCache.put(query, new CachedQueryResult(results, queryCacheTtl.toMillis()));
            LOG.debug("Cached results for query, cache size: {}", queryCache.size());
        }
    }

    /**
     * Report metrics for processed data.
     */
    private void reportMetrics(RowData rowData) {
        if (sourceExactlyMetric != null) {
            try {
                sourceExactlyMetric.outputMetrics(1, rowData.toString().getBytes().length, System.currentTimeMillis());
            } catch (Exception e) {
                LOG.warn("Failed to report metrics", e);
            }
        }
    }

    /**
     * Cached query result for performance optimization.
     */
    private static class CachedQueryResult {

        private final List<Map<String, Object>> results;
        private final long timestamp;
        private final long ttlMs;

        public CachedQueryResult(List<Map<String, Object>> results, long ttlMs) {
            this.results = results;
            this.timestamp = System.currentTimeMillis();
            this.ttlMs = ttlMs;
        }

        public boolean isExpired() {
            return System.currentTimeMillis() - timestamp > ttlMs;
        }

        public List<Map<String, Object>> getResults() {
            return results;
        }
    }
}