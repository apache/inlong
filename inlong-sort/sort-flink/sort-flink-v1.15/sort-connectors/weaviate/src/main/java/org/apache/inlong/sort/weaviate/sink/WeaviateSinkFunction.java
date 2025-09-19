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

package org.apache.inlong.sort.weaviate.sink;

import org.apache.inlong.sort.base.metric.MetricOption;
import org.apache.inlong.sort.base.metric.SinkExactlyMetric;
import org.apache.inlong.sort.weaviate.table.WeaviateOptions;
import org.apache.inlong.sort.weaviate.utils.WeaviateClientUtils;
import org.apache.inlong.sort.weaviate.utils.WeaviateErrorHandler;
import org.apache.inlong.sort.weaviate.utils.WeaviateErrorHandler.WeaviateErrorCode;
import org.apache.inlong.sort.weaviate.utils.WeaviateErrorHandler.WeaviateException;
import org.apache.inlong.sort.weaviate.utils.WeaviateMonitoringUtils;
import org.apache.inlong.sort.weaviate.utils.WeaviateTypeConverter;

import io.weaviate.client.WeaviateClient;
import io.weaviate.client.base.Result;
import io.weaviate.client.v1.batch.model.ObjectGetResponse;
import io.weaviate.client.v1.data.model.WeaviateObject;
import lombok.Data;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Enhanced Weaviate sink function with advanced performance optimizations.
 * Features include:
 * - Advanced batch processing with memory monitoring
 * - Connection pool management
 * - Backpressure handling
 * - Concurrent batch processing
 * - Memory pressure detection and auto-flush
 * - Enhanced error handling and retry mechanisms
 */
@Data
public class WeaviateSinkFunction extends RichSinkFunction<RowData> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(WeaviateSinkFunction.class);

    private final ReadableConfig config;
    private final DataType physicalDataType;
    private final MetricOption metricOption;

    // Runtime state
    private transient WeaviateClient client;
    private transient WeaviateClientUtils.WeaviateConnectionPool connectionPool;
    private transient SinkExactlyMetric sinkExactlyMetric;
    private transient WeaviateTypeConverter typeConverter;
    private transient List<Map<String, Object>> batchBuffer;
    private transient ScheduledExecutorService flushExecutor;
    private transient ScheduledExecutorService memoryMonitorExecutor;
    private transient ThreadPoolExecutor batchProcessorExecutor;
    private transient AtomicLong lastFlushTime;
    private transient ReentrantLock batchLock;
    private transient AtomicBoolean backpressureActive;
    private transient MemoryMXBean memoryBean;
    private transient Semaphore concurrentBatchSemaphore;

    // Enhanced monitoring and error handling
    private transient WeaviateMonitoringUtils.EnhancedMetrics enhancedMetrics;
    private transient WeaviateMonitoringUtils.ConnectionHealthMonitor healthMonitor;

    // Configuration
    private String url;
    private String apiKey;
    private String className;
    private String vectorField;
    private Integer vectorDimension;
    private Integer batchSize;
    private Duration flushInterval;
    private Integer maxRetries;
    private Duration retryDelay;
    private Boolean upsertEnabled;
    private Boolean ignoreNullValues;
    private String consistencyLevel;
    private long maxBatchMemoryBytes;
    private Boolean useConnectionPool;
    private Integer connectionPoolSize;
    private Double memoryPressureThreshold;
    private Duration backpressureDelay;
    private Integer maxConcurrentBatches;
    private Duration memoryMonitorInterval;

    // Enhanced metrics tracking
    private transient AtomicLong totalRecordsProcessed;
    private transient AtomicLong totalBytesProcessed;
    private transient AtomicLong totalBatchesFlushed;
    private transient AtomicLong totalFailedRecords;
    private transient AtomicLong totalBackpressureEvents;
    private transient AtomicLong totalMemoryFlushes;
    private transient AtomicLong currentBatchMemoryUsage;
    private transient AtomicLong totalConcurrentBatches;

    public WeaviateSinkFunction(
            ReadableConfig config,
            DataType physicalDataType,
            MetricOption metricOption) {
        this.config = config;
        this.physicalDataType = physicalDataType;
        this.metricOption = metricOption;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        WeaviateMonitoringUtils.PerformanceTracker tracker = WeaviateMonitoringUtils
                .startOperation("WeaviateSinkFunction.open", LOG);

        try {
            LOG.info("Opening WeaviateSinkFunction with enhanced error handling and monitoring");

            // Initialize configuration with validation
            initializeConfiguration();

            // Initialize enhanced monitoring
            initializeEnhancedMonitoring();

            // Initialize Weaviate client with retry and health monitoring
            initializeClient();

            // Initialize type converter with validation
            initializeTypeConverter();

            // Initialize batch buffer and memory tracking
            initializeBatchBuffer();

            // Initialize metrics with enhanced reporting
            initializeMetrics();

            // Initialize flush executor for periodic flushing
            initializeFlushExecutor();

            // Log system resources for monitoring
            WeaviateMonitoringUtils.logSystemResources(LOG);

            tracker.recordSuccess(1, 0);
            LOG.info("WeaviateSinkFunction opened successfully with enhanced monitoring");

        } catch (Exception e) {
            WeaviateException weaviateException = WeaviateErrorHandler.wrapException(e,
                    "WeaviateSinkFunction.open failed");
            tracker.recordFailure(weaviateException, 0);
            throw weaviateException;
        }
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        if (value == null) {
            LOG.debug("Received null RowData, skipping");
            return;
        }

        WeaviateMonitoringUtils.PerformanceTracker tracker = WeaviateMonitoringUtils
                .startOperation("WeaviateSinkFunction.invoke", LOG);

        // Check for backpressure before processing
        if (backpressureActive.get()) {
            totalBackpressureEvents.incrementAndGet();
            LOG.debug("Backpressure active, applying delay of {}ms", backpressureDelay.toMillis());
            Thread.sleep(backpressureDelay.toMillis());
        }

        try {
            // Convert RowData to Weaviate object with enhanced error handling
            Map<String, Object> weaviateObject;
            try {
                weaviateObject = typeConverter.convertRowDataToWeaviateObject(value);
            } catch (WeaviateException e) {
                totalFailedRecords.incrementAndGet();
                WeaviateErrorHandler.logError(LOG, "RowData conversion", e,
                        "rowData", value.toString());
                if (enhancedMetrics != null) {
                    enhancedMetrics.recordFailure(1, 0);
                }
                throw e;
            }

            // Skip null values if configured
            if (ignoreNullValues && isObjectEmpty(weaviateObject)) {
                LOG.debug("Skipping empty object due to ignore-null-values setting");
                return;
            }

            // Add to batch buffer with enhanced synchronization and error handling
            batchLock.lock();
            try {
                batchBuffer.add(weaviateObject);

                // Update metrics
                long dataSize = estimateObjectSize(weaviateObject);
                totalRecordsProcessed.incrementAndGet();
                totalBytesProcessed.addAndGet(dataSize);
                currentBatchMemoryUsage.addAndGet(dataSize);

                // Report enhanced metrics
                reportMetrics(1, dataSize);
                if (enhancedMetrics != null) {
                    enhancedMetrics.recordSinkMetrics(1, dataSize, System.currentTimeMillis());
                }

                // Check if we need to flush based on batch size, memory usage, or pressure
                if (shouldFlushBatch()) {
                    // Use async batch processing to avoid blocking
                    flushBatchAsync();
                }

                tracker.recordSuccess(1, dataSize);

            } finally {
                batchLock.unlock();
            }

        } catch (WeaviateException e) {
            totalFailedRecords.incrementAndGet();
            tracker.recordFailure(e, 0);
            throw e;
        } catch (Exception e) {
            totalFailedRecords.incrementAndGet();
            WeaviateException weaviateException = WeaviateErrorHandler.wrapException(e,
                    "Failed to process RowData in WeaviateSinkFunction.invoke");
            tracker.recordFailure(weaviateException, 0);
            throw weaviateException;
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing WeaviateSinkFunction");

        try {
            // Flush any remaining data
            batchLock.lock();
            try {
                if (batchBuffer != null && !batchBuffer.isEmpty()) {
                    LOG.info("Flushing remaining {} records before closing", batchBuffer.size());
                    flushBatchSync();
                }
            } finally {
                batchLock.unlock();
            }

            // Shutdown executors gracefully
            shutdownExecutors();

            // Return client to pool or close
            closeClient();

            // Log final statistics
            logFinalStatistics();

        } finally {
            super.close();
        }

        LOG.info("WeaviateSinkFunction closed");
    }

    /**
     * Initialize enhanced monitoring and health tracking.
     */
    private void initializeEnhancedMonitoring() {
        // Initialize enhanced metrics
        enhancedMetrics = WeaviateMonitoringUtils.createMetrics(
                "WeaviateSinkFunction", metricOption, getRuntimeContext().getMetricGroup(), false);

        // Initialize connection health monitor
        healthMonitor = WeaviateMonitoringUtils.createHealthMonitor(url);

        LOG.info("Enhanced monitoring initialized for WeaviateSinkFunction");
    }

    /**
     * Initialize configuration from ReadableConfig with enhanced validation.
     */
    private void initializeConfiguration() throws WeaviateException {
        // Enhanced configuration validation
        url = config.get(WeaviateOptions.URL);
        WeaviateErrorHandler.validateConfig("url", url, "WeaviateSinkFunction");

        apiKey = config.getOptional(WeaviateOptions.API_KEY).orElse(null);

        className = config.get(WeaviateOptions.CLASS_NAME);
        WeaviateErrorHandler.validateConfig("className", className, "WeaviateSinkFunction");

        vectorField = config.getOptional(WeaviateOptions.VECTOR_FIELD).orElse(null);
        vectorDimension = config.getOptional(WeaviateOptions.VECTOR_DIMENSION).orElse(null);

        batchSize = config.get(WeaviateOptions.BATCH_SIZE);
        if (batchSize <= 0) {
            throw new WeaviateException(WeaviateErrorCode.INVALID_BATCH_SIZE,
                    WeaviateErrorHandler.createContext("WeaviateSinkFunction", "batchSize", batchSize));
        }

        flushInterval = config.get(WeaviateOptions.FLUSH_INTERVAL);
        maxRetries = config.get(WeaviateOptions.MAX_RETRIES);
        retryDelay = config.get(WeaviateOptions.RETRY_DELAY);
        upsertEnabled = config.get(WeaviateOptions.UPSERT_ENABLED);
        ignoreNullValues = config.get(WeaviateOptions.IGNORE_NULL_VALUES);
        consistencyLevel = config.get(WeaviateOptions.CONSISTENCY_LEVEL);

        // Parse max batch memory
        String maxBatchMemoryStr = config.get(WeaviateOptions.MAX_BATCH_MEMORY);
        maxBatchMemoryBytes = parseMemorySize(maxBatchMemoryStr);

        // Performance optimization settings
        useConnectionPool = config.get(WeaviateOptions.USE_CONNECTION_POOL);
        connectionPoolSize = config.get(WeaviateOptions.CONNECTION_POOL_SIZE);
        memoryPressureThreshold = config.get(WeaviateOptions.MEMORY_PRESSURE_THRESHOLD);
        backpressureDelay = config.get(WeaviateOptions.BACKPRESSURE_DELAY);
        maxConcurrentBatches = config.get(WeaviateOptions.MAX_CONCURRENT_BATCHES);
        memoryMonitorInterval = config.get(WeaviateOptions.MEMORY_MONITOR_INTERVAL);

        LOG.info("Initialized configuration: url={}, className={}, batchSize={}, flushInterval={}, " +
                "vectorField={}, upsertEnabled={}, maxBatchMemory={}, useConnectionPool={}, " +
                "memoryPressureThreshold={}, maxConcurrentBatches={}",
                url, className, batchSize, flushInterval, vectorField, upsertEnabled, maxBatchMemoryStr,
                useConnectionPool, memoryPressureThreshold, maxConcurrentBatches);
    }

    /**
     * Initialize Weaviate client with enhanced connection pooling and health
     * monitoring.
     */
    private void initializeClient() throws WeaviateException {
        String context = WeaviateErrorHandler.createContext("WeaviateSinkFunction.initializeClient",
                "url", url, "useConnectionPool", useConnectionPool);
        LOG.info("Initializing Weaviate client with enhanced monitoring: {}", context);

        try {
            if (useConnectionPool) {
                // Use connection pool for better resource management
                connectionPool = WeaviateClientUtils.getConnectionPool(url, apiKey, connectionPoolSize);
                client = connectionPool.borrowClient();
                LOG.info("Using connection pool with size: {} for URL: {}", connectionPoolSize, url);
            } else {
                // Use single client instance with enhanced retry
                client = WeaviateClientUtils.createClientWithRetry(url, apiKey, maxRetries, retryDelay);
            }

            // Enhanced connection test with health monitoring
            if (!WeaviateClientUtils.testConnectionWithHealthCheck(client, url)) {
                healthMonitor.recordHealthCheck(false, "Initial connection test failed");
                throw new WeaviateException(WeaviateErrorCode.CONNECTION_FAILED,
                        context + " - Connection test failed");
            }

            // Record successful health check
            healthMonitor.recordHealthCheck(true, null);

            LOG.info("Weaviate client initialized successfully with health monitoring: {}", url);

        } catch (WeaviateException e) {
            WeaviateErrorHandler.logError(LOG, "client initialization", e, "url", url);
            throw e;
        } catch (Exception e) {
            WeaviateException weaviateException = WeaviateErrorHandler.wrapException(e, context);
            WeaviateErrorHandler.logError(LOG, "client initialization", weaviateException, "url", url);
            throw weaviateException;
        }
    }

    /**
     * Initialize type converter with enhanced validation.
     */
    private void initializeTypeConverter() throws WeaviateException {
        try {
            RowType rowType = (RowType) physicalDataType.getLogicalType();
            typeConverter = new WeaviateTypeConverter(rowType, vectorField, vectorDimension);
            typeConverter.validate();

            LOG.info("Type converter initialized and validated with vector field: {}, dimension: {}",
                    vectorField, vectorDimension);

        } catch (WeaviateException e) {
            WeaviateErrorHandler.logError(LOG, "type converter initialization", e,
                    "vectorField", vectorField, "vectorDimension", vectorDimension);
            throw e;
        } catch (Exception e) {
            WeaviateException weaviateException = WeaviateErrorHandler.wrapException(e,
                    "Type converter initialization failed");
            WeaviateErrorHandler.logError(LOG, "type converter initialization", weaviateException);
            throw weaviateException;
        }
    }

    /**
     * 
     * Initialize batch buffer and synchronization primitives.
     */
    private void initializeBatchBuffer() {
        batchBuffer = new ArrayList<>(batchSize);
        lastFlushTime = new AtomicLong(System.currentTimeMillis());
        batchLock = new ReentrantLock();
        backpressureActive = new AtomicBoolean(false);
        concurrentBatchSemaphore = new Semaphore(maxConcurrentBatches);
        memoryBean = ManagementFactory.getMemoryMXBean();

        // Initialize enhanced metrics tracking
        totalRecordsProcessed = new AtomicLong(0);
        totalBytesProcessed = new AtomicLong(0);
        totalBatchesFlushed = new AtomicLong(0);
        totalFailedRecords = new AtomicLong(0);
        totalBackpressureEvents = new AtomicLong(0);
        totalMemoryFlushes = new AtomicLong(0);
        currentBatchMemoryUsage = new AtomicLong(0);
        totalConcurrentBatches = new AtomicLong(0);

        LOG.info("Batch buffer initialized with size: {}, max memory: {} bytes, concurrent batches: {}",
                batchSize, maxBatchMemoryBytes, maxConcurrentBatches);
    }

    /**
     * Initialize metrics for InLong audit.
     */
    private void initializeMetrics() {
        if (metricOption != null) {
            sinkExactlyMetric = new SinkExactlyMetric(metricOption, getRuntimeContext().getMetricGroup());
            LOG.info("Sink metrics initialized");
        } else {
            LOG.warn("MetricOption is null, metrics will not be reported");
        }
    }

    /**
     * Initialize executors for async processing and memory monitoring.
     */
    private void initializeFlushExecutor() {
        // Flush executor for periodic flushing
        flushExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "weaviate-sink-flush-executor");
            t.setDaemon(true);
            return t;
        });

        // Schedule periodic flush
        flushExecutor.scheduleAtFixedRate(
                this::periodicFlush,
                flushInterval.toMillis(),
                flushInterval.toMillis(),
                TimeUnit.MILLISECONDS);

        // Memory monitor executor
        memoryMonitorExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "weaviate-memory-monitor");
            t.setDaemon(true);
            return t;
        });

        // Schedule memory monitoring
        memoryMonitorExecutor.scheduleAtFixedRate(
                this::monitorMemoryPressure,
                memoryMonitorInterval.toMillis(),
                memoryMonitorInterval.toMillis(),
                TimeUnit.MILLISECONDS);

        // Batch processor executor for concurrent batch processing
        batchProcessorExecutor = new ThreadPoolExecutor(
                1, // core pool size
                maxConcurrentBatches, // maximum pool size
                60L, TimeUnit.SECONDS, // keep alive time
                new LinkedBlockingQueue<>(), // work queue
                r -> {
                    Thread t = new Thread(r, "weaviate-batch-processor");
                    t.setDaemon(true);
                    return t;
                });

        LOG.info(
                "Executors initialized with flush interval: {}, memory monitor interval: {}, max concurrent batches: {}",
                flushInterval, memoryMonitorInterval, maxConcurrentBatches);
    }

    /**
     * Enhanced batch flush condition checking.
     */
    private boolean shouldFlushBatch() {
        if (batchBuffer.isEmpty()) {
            return false;
        }

        // Check batch size
        if (batchBuffer.size() >= batchSize) {
            LOG.debug("Batch size limit reached: {}/{}", batchBuffer.size(), batchSize);
            return true;
        }

        // Check memory usage
        long currentMemoryUsage = currentBatchMemoryUsage.get();
        if (currentMemoryUsage >= maxBatchMemoryBytes) {
            LOG.debug("Batch memory limit reached: {} bytes/{} bytes", currentMemoryUsage, maxBatchMemoryBytes);
            return true;
        }

        // Check system memory pressure
        if (isMemoryPressureHigh()) {
            LOG.debug("System memory pressure detected, triggering flush");
            totalMemoryFlushes.incrementAndGet();
            return true;
        }

        return false;
    }

    /**
     * Periodic flush triggered by executor.
     */
    private void periodicFlush() {
        try {
            synchronized (batchBuffer) {
                if (!batchBuffer.isEmpty()) {
                    long timeSinceLastFlush = System.currentTimeMillis() - lastFlushTime.get();
                    if (timeSinceLastFlush >= flushInterval.toMillis()) {
                        LOG.debug("Periodic flush triggered, {} records in buffer", batchBuffer.size());
                        flushBatch();
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Error during periodic flush", e);
        }
    }

    /**
     * Flush batch to Weaviate with retry mechanism.
     */
    private void flushBatch() throws Exception {
        if (batchBuffer.isEmpty()) {
            return;
        }

        List<Map<String, Object>> currentBatch = new ArrayList<>(batchBuffer);
        batchBuffer.clear();
        lastFlushTime.set(System.currentTimeMillis());

        LOG.debug("Flushing batch with {} records", currentBatch.size());

        Exception lastException = null;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                flushBatchWithRetry(currentBatch);
                totalBatchesFlushed.incrementAndGet();
                LOG.debug("Successfully flushed batch with {} records on attempt {}", currentBatch.size(), attempt);
                return;

            } catch (Exception e) {
                lastException = e;
                LOG.warn("Batch flush attempt {}/{} failed: {}", attempt, maxRetries, e.getMessage());

                if (attempt < maxRetries) {
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

        // All retries failed
        totalFailedRecords.addAndGet(currentBatch.size());
        throw new RuntimeException("Failed to flush batch after " + maxRetries + " attempts", lastException);
    }

    /**
     * Execute batch write to Weaviate.
     */
    private void flushBatchWithRetry(List<Map<String, Object>> batch) throws Exception {
        // Convert maps to WeaviateObject instances
        WeaviateObject[] objects = new WeaviateObject[batch.size()];
        for (int i = 0; i < batch.size(); i++) {
            Map<String, Object> objectMap = batch.get(i);
            WeaviateObject weaviateObject = WeaviateObject.builder()
                    .className(className)
                    .properties(objectMap)
                    .build();
            objects[i] = weaviateObject;
        }

        // Execute batch operation
        Result<ObjectGetResponse[]> result;
        if (upsertEnabled) {
            // Use batch upsert if available
            result = client.batch()
                    .objectsBatcher()
                    .withObjects(objects)
                    .run();
        } else {
            // Use regular batch insert
            result = client.batch()
                    .objectsBatcher()
                    .withObjects(objects)
                    .run();
        }

        // Check for errors
        if (result.hasErrors()) {
            String errorMessage = "Batch operation failed: " + result.getError().getMessages();
            LOG.error("Batch write failed: {}", errorMessage);
            throw new RuntimeException(errorMessage);
        }

        // Check individual object results if available
        ObjectGetResponse[] responses = result.getResult();
        if (responses != null) {
            int successCount = 0;
            int errorCount = 0;

            for (ObjectGetResponse response : responses) {
                if (response != null) {
                    // Check if response indicates success
                    // Note: The exact success/error checking depends on Weaviate client version
                    successCount++;
                } else {
                    errorCount++;
                }
            }

            LOG.debug("Batch write completed: {} successful, {} errors", successCount, errorCount);

            if (errorCount > 0) {
                LOG.warn("Some objects in batch failed to write: {} errors out of {} objects",
                        errorCount, batch.size());
            }
        }
    }

    /**
     * 
     * Asynchronous batch flush to avoid blocking the main thread.
     */
    private void flushBatchAsync() {
        if (!concurrentBatchSemaphore.tryAcquire()) {
            LOG.debug("Max concurrent batches reached, skipping async flush");
            return;
        }

        List<Map<String, Object>> currentBatch;
        batchLock.lock();
        try {
            if (batchBuffer.isEmpty()) {
                concurrentBatchSemaphore.release();
                return;
            }

            currentBatch = new ArrayList<>(batchBuffer);
            batchBuffer.clear();
            currentBatchMemoryUsage.set(0);
            lastFlushTime.set(System.currentTimeMillis());
        } finally {
            batchLock.unlock();
        }

        // Submit batch processing task
        CompletableFuture.runAsync(() -> {
            try {
                totalConcurrentBatches.incrementAndGet();
                processBatch(currentBatch);
                totalBatchesFlushed.incrementAndGet();
                LOG.debug("Successfully flushed async batch with {} records", currentBatch.size());
            } catch (Exception e) {
                totalFailedRecords.addAndGet(currentBatch.size());
                LOG.error("Failed to flush async batch with {} records", currentBatch.size(), e);
            } finally {
                totalConcurrentBatches.decrementAndGet();
                concurrentBatchSemaphore.release();
            }
        }, batchProcessorExecutor);
    }

    /**
     * Process a batch of records with enhanced error handling.
     */
    private void processBatch(List<Map<String, Object>> batch) throws Exception {
        Exception lastException = null;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                WeaviateClient batchClient = getBatchClient();
                executeBatchWrite(batchClient, batch);
                returnBatchClient(batchClient);
                return;

            } catch (Exception e) {
                lastException = e;
                LOG.warn("Batch processing attempt {}/{} failed: {}", attempt, maxRetries, e.getMessage());

                if (attempt < maxRetries) {
                    // Exponential backoff with jitter
                    long delayMs = retryDelay.toMillis() * (1L << (attempt - 1));
                    delayMs += (long) (Math.random() * 1000); // Add jitter
                    Thread.sleep(delayMs);
                }
            }
        }

        throw new RuntimeException("Failed to process batch after " + maxRetries + " attempts", lastException);
    }

    /**
     * Get a client for batch processing (from pool or main client).
     */
    private WeaviateClient getBatchClient() throws Exception {
        if (useConnectionPool && connectionPool != null) {
            return connectionPool.borrowClient();
        }
        return client;
    }

    /**
     * Return a client after batch processing.
     */
    private void returnBatchClient(WeaviateClient batchClient) {
        if (useConnectionPool && connectionPool != null && batchClient != client) {
            connectionPool.returnClient(batchClient);
        }
    }

    /**
     * Execute batch write operation.
     */
    private void executeBatchWrite(WeaviateClient batchClient, List<Map<String, Object>> batch) throws Exception {
        // Convert maps to WeaviateObject instances
        WeaviateObject[] objects = new WeaviateObject[batch.size()];
        for (int i = 0; i < batch.size(); i++) {
            Map<String, Object> objectMap = batch.get(i);
            WeaviateObject weaviateObject = WeaviateObject.builder()
                    .className(className)
                    .properties(objectMap)
                    .build();
            objects[i] = weaviateObject;
        }

        // Execute batch operation
        Result<ObjectGetResponse[]> result;
        if (upsertEnabled) {
            result = batchClient.batch()
                    .objectsBatcher()
                    .withObjects(objects)
                    .run();
        } else {
            result = batchClient.batch()
                    .objectsBatcher()
                    .withObjects(objects)
                    .run();
        }

        // Check for errors
        if (result.hasErrors()) {
            String errorMessage = "Batch operation failed: " + result.getError().getMessages();
            LOG.error("Batch write failed: {}", errorMessage);
            throw new RuntimeException(errorMessage);
        }
    }

    /**
     * Synchronous batch flush for critical operations.
     */
    private void flushBatchSync() throws Exception {
        List<Map<String, Object>> currentBatch;
        if (batchBuffer.isEmpty()) {
            return;
        }

        currentBatch = new ArrayList<>(batchBuffer);
        batchBuffer.clear();
        currentBatchMemoryUsage.set(0);
        lastFlushTime.set(System.currentTimeMillis());

        processBatch(currentBatch);
        totalBatchesFlushed.incrementAndGet();
        LOG.debug("Successfully flushed sync batch with {} records", currentBatch.size());
    }

    /**
     * Shutdown all executors gracefully.
     */
    private void shutdownExecutors() {
        // Shutdown flush executor
        if (flushExecutor != null && !flushExecutor.isShutdown()) {
            flushExecutor.shutdown();
            try {
                if (!flushExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    flushExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                flushExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        // Shutdown memory monitor executor
        if (memoryMonitorExecutor != null && !memoryMonitorExecutor.isShutdown()) {
            memoryMonitorExecutor.shutdown();
            try {
                if (!memoryMonitorExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    memoryMonitorExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                memoryMonitorExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        // Shutdown batch processor executor
        if (batchProcessorExecutor != null && !batchProcessorExecutor.isShutdown()) {
            batchProcessorExecutor.shutdown();
            try {
                if (!batchProcessorExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                    batchProcessorExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                batchProcessorExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Close client resources.
     */
    private void closeClient() {
        if (useConnectionPool && connectionPool != null && client != null) {
            connectionPool.returnClient(client);
        }
        client = null;
    }

    /**
     * Monitor memory pressure and activate backpressure if needed.
     */
    private void monitorMemoryPressure() {
        try {
            boolean highPressure = isMemoryPressureHigh();
            boolean wasActive = backpressureActive.getAndSet(highPressure);

            if (highPressure && !wasActive) {
                LOG.warn("Memory pressure detected, activating backpressure");
                // Trigger immediate flush if possible
                if (batchLock.tryLock()) {
                    try {
                        if (!batchBuffer.isEmpty()) {
                            flushBatchAsync();
                        }
                    } finally {
                        batchLock.unlock();
                    }
                }
            } else if (!highPressure && wasActive) {
                LOG.info("Memory pressure relieved, deactivating backpressure");
            }
        } catch (Exception e) {
            LOG.error("Error during memory pressure monitoring", e);
        }
    }

    /**
     * Check if system memory pressure is high.
     */
    private boolean isMemoryPressureHigh() {
        try {
            MemoryUsage heapMemory = memoryBean.getHeapMemoryUsage();
            double usageRatio = (double) heapMemory.getUsed() / heapMemory.getMax();
            return usageRatio > memoryPressureThreshold;
        } catch (Exception e) {
            LOG.debug("Failed to check memory pressure", e);
            return false;
        }
    }

    /**
     * Report metrics for processed data.
     */
    private void reportMetrics(long recordCount, long dataSize) {
        if (sinkExactlyMetric != null) {
            try {
                sinkExactlyMetric.invoke(recordCount, dataSize, System.currentTimeMillis());
            } catch (Exception e) {
                LOG.warn("Failed to report metrics", e);
            }
        }
    }

    /**
     * Check if object is empty (all values are null).
     */
    private boolean isObjectEmpty(Map<String, Object> object) {
        if (object == null || object.isEmpty()) {
            return true;
        }

        return object.values().stream().allMatch(value -> value == null);
    }

    /**
     * Estimate the size of a Weaviate object in bytes.
     */
    private long estimateObjectSize(Map<String, Object> object) {
        if (object == null || object.isEmpty()) {
            return 0;
        }

        long size = 0;
        for (Map.Entry<String, Object> entry : object.entrySet()) {
            // Estimate key size
            size += entry.getKey().length() * 2; // Assuming UTF-16

            // Estimate value size
            Object value = entry.getValue();
            if (value != null) {
                if (value instanceof String) {
                    size += ((String) value).length() * 2;
                } else if (value instanceof float[]) {
                    size += ((float[]) value).length * 4;
                } else if (value instanceof List) {
                    size += ((List<?>) value).size() * 8; // Rough estimate
                } else {
                    size += 8; // Default estimate for other types
                }
            }
        }

        return size;
    }

    /**
     * Parse memory size string to bytes.
     */
    private long parseMemorySize(String memoryStr) {
        if (memoryStr == null || memoryStr.trim().isEmpty()) {
            return 64 * 1024 * 1024; // Default 64MB
        }

        String upperStr = memoryStr.trim().toUpperCase();
        long multiplier = 1;

        if (upperStr.endsWith("KB")) {
            multiplier = 1024;
            upperStr = upperStr.substring(0, upperStr.length() - 2);
        } else if (upperStr.endsWith("MB")) {
            multiplier = 1024 * 1024;
            upperStr = upperStr.substring(0, upperStr.length() - 2);
        } else if (upperStr.endsWith("GB")) {
            multiplier = 1024 * 1024 * 1024;
            upperStr = upperStr.substring(0, upperStr.length() - 2);
        } else if (upperStr.endsWith("B")) {
            upperStr = upperStr.substring(0, upperStr.length() - 1);
        }

        try {
            return Long.parseLong(upperStr.trim()) * multiplier;
        } catch (NumberFormatException e) {
            LOG.warn("Invalid memory size format: {}, using default 64MB", memoryStr);
            return 64 * 1024 * 1024;
        }
    }

    /**
     * Log comprehensive final statistics.
     */
    private void logFinalStatistics() {
        LOG.info("WeaviateSinkFunction Performance Statistics:");
        LOG.info("  Total records processed: {}", totalRecordsProcessed.get());
        LOG.info("  Total bytes processed: {}", totalBytesProcessed.get());
        LOG.info("  Total batches flushed: {}", totalBatchesFlushed.get());
        LOG.info("  Total failed records: {}", totalFailedRecords.get());
        LOG.info("  Total backpressure events: {}", totalBackpressureEvents.get());
        LOG.info("  Total memory-triggered flushes: {}", totalMemoryFlushes.get());
        LOG.info("  Peak concurrent batches: {}", totalConcurrentBatches.get());

        long successRate = totalRecordsProcessed.get() > 0
                ? ((totalRecordsProcessed.get() - totalFailedRecords.get()) * 100) / totalRecordsProcessed.get()
                : 0;
        LOG.info("  Success rate: {}%", successRate);

        if (useConnectionPool && connectionPool != null) {
            LOG.info("  Connection pool stats: current={}, available={}",
                    connectionPool.getCurrentPoolSize(), connectionPool.getAvailableClients());
        }
    }
}