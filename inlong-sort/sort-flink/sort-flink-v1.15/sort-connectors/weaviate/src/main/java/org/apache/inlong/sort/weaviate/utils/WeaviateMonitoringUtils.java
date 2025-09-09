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

package org.apache.inlong.sort.weaviate.utils;

import org.apache.inlong.sort.base.metric.MetricOption;
import org.apache.inlong.sort.base.metric.SinkExactlyMetric;
import org.apache.inlong.sort.base.metric.SourceExactlyMetric;

import org.apache.flink.metrics.MetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Enhanced monitoring and metrics utility for Weaviate connector operations.
 * Provides detailed logging, performance metrics, and operational insights.
 */
public class WeaviateMonitoringUtils implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(WeaviateMonitoringUtils.class);

    // Metric keys for structured logging
    public static final String METRIC_COMPONENT = "component";
    public static final String METRIC_OPERATION = "operation";
    public static final String METRIC_STATUS = "status";
    public static final String METRIC_DURATION = "duration_ms";
    public static final String METRIC_RECORDS = "records";
    public static final String METRIC_BYTES = "bytes";
    public static final String METRIC_ERROR_CODE = "error_code";
    public static final String METRIC_BATCH_SIZE = "batch_size";
    public static final String METRIC_RETRY_COUNT = "retry_count";

    /**
     * Performance metrics tracker for operations.
     */
    public static class PerformanceTracker {

        private final String operationName;
        private final Instant startTime;
        private final Map<String, Object> context;
        private final Logger logger;

        public PerformanceTracker(String operationName, Logger logger) {
            this.operationName = operationName;
            this.startTime = Instant.now();
            this.context = new HashMap<>();
            this.logger = logger;

            // Set MDC for structured logging
            MDC.put(METRIC_OPERATION, operationName);
            MDC.put("start_time", startTime.toString());
        }

        public PerformanceTracker addContext(String key, Object value) {
            context.put(key, value);
            MDC.put(key, String.valueOf(value));
            return this;
        }

        public void recordSuccess(long recordCount, long byteCount) {
            Duration duration = Duration.between(startTime, Instant.now());

            MDC.put(METRIC_STATUS, "SUCCESS");
            MDC.put(METRIC_DURATION, String.valueOf(duration.toMillis()));
            MDC.put(METRIC_RECORDS, String.valueOf(recordCount));
            MDC.put(METRIC_BYTES, String.valueOf(byteCount));

            logger.info("Operation {} completed successfully in {}ms - {} records, {} bytes",
                    operationName, duration.toMillis(), recordCount, byteCount);

            // Calculate throughput metrics
            if (duration.toMillis() > 0) {
                double recordsPerSecond = (recordCount * 1000.0) / duration.toMillis();
                double mbPerSecond = (byteCount * 1000.0) / (duration.toMillis() * 1024 * 1024);

                logger.debug("Performance metrics - Records/sec: {:.2f}, MB/sec: {:.2f}",
                        recordsPerSecond, mbPerSecond);

                MDC.put("records_per_second", String.format("%.2f", recordsPerSecond));
                MDC.put("mb_per_second", String.format("%.2f", mbPerSecond));
            }

            clearMDC();
        }

        public void recordFailure(WeaviateErrorHandler.WeaviateException error, int retryCount) {
            Duration duration = Duration.between(startTime, Instant.now());

            MDC.put(METRIC_STATUS, "FAILURE");
            MDC.put(METRIC_DURATION, String.valueOf(duration.toMillis()));
            MDC.put(METRIC_ERROR_CODE, String.valueOf(error.getErrorCode().getCode()));
            MDC.put(METRIC_RETRY_COUNT, String.valueOf(retryCount));

            logger.error("Operation {} failed after {}ms and {} retries: [WE-{}] {}",
                    operationName, duration.toMillis(), retryCount,
                    error.getErrorCode().getCode(), error.getMessage(), error);

            clearMDC();
        }

        public void recordPartialSuccess(long successCount, long failureCount, long byteCount) {
            Duration duration = Duration.between(startTime, Instant.now());

            MDC.put(METRIC_STATUS, "PARTIAL_SUCCESS");
            MDC.put(METRIC_DURATION, String.valueOf(duration.toMillis()));
            MDC.put(METRIC_RECORDS, String.valueOf(successCount));
            MDC.put(METRIC_BYTES, String.valueOf(byteCount));
            MDC.put("failed_records", String.valueOf(failureCount));

            logger.warn("Operation {} completed with partial success in {}ms - {} succeeded, {} failed",
                    operationName, duration.toMillis(), successCount, failureCount);

            clearMDC();
        }

        private void clearMDC() {
            MDC.remove(METRIC_OPERATION);
            MDC.remove(METRIC_STATUS);
            MDC.remove(METRIC_DURATION);
            MDC.remove(METRIC_RECORDS);
            MDC.remove(METRIC_BYTES);
            MDC.remove(METRIC_ERROR_CODE);
            MDC.remove(METRIC_RETRY_COUNT);
            MDC.remove("start_time");
            MDC.remove("records_per_second");
            MDC.remove("mb_per_second");
            MDC.remove("failed_records");

            // Clear custom context
            for (String key : context.keySet()) {
                MDC.remove(key);
            }
        }
    }

    /**
     * Enhanced metrics wrapper for InLong audit integration.
     */
    public static class EnhancedMetrics {

        private final SourceExactlyMetric sourceMetric;
        private final SinkExactlyMetric sinkMetric;
        private final String componentName;
        private final Logger logger;

        // Additional metrics tracking
        private final AtomicLong totalOperations = new AtomicLong(0);
        private final AtomicLong successfulOperations = new AtomicLong(0);
        private final AtomicLong failedOperations = new AtomicLong(0);
        private final AtomicLong totalRetries = new AtomicLong(0);
        private final AtomicLong totalProcessingTime = new AtomicLong(0);

        public EnhancedMetrics(String componentName, MetricOption metricOption,
                MetricGroup metricGroup, boolean isSource) {
            this.componentName = componentName;
            this.logger = LoggerFactory.getLogger(componentName);

            if (metricOption != null) {
                if (isSource) {
                    this.sourceMetric = new SourceExactlyMetric(metricOption, metricGroup);
                    this.sinkMetric = null;
                } else {
                    this.sourceMetric = null;
                    this.sinkMetric = new SinkExactlyMetric(metricOption, metricGroup);
                }
            } else {
                this.sourceMetric = null;
                this.sinkMetric = null;
                logger.warn("MetricOption is null, InLong audit metrics will not be reported");
            }
        }

        public void recordSourceMetrics(long recordCount, long byteCount, long timestamp) {
            if (sourceMetric != null) {
                try {
                    sourceMetric.outputMetrics(recordCount, byteCount, timestamp);
                    logger.debug("Source metrics reported: {} records, {} bytes", recordCount, byteCount);
                } catch (Exception e) {
                    logger.warn("Failed to report source metrics", e);
                }
            }

            updateOperationMetrics(recordCount, true, 0);
        }

        public void recordSinkMetrics(long recordCount, long byteCount, long timestamp) {
            if (sinkMetric != null) {
                try {
                    sinkMetric.invoke(recordCount, byteCount, timestamp);
                    logger.debug("Sink metrics reported: {} records, {} bytes", recordCount, byteCount);
                } catch (Exception e) {
                    logger.warn("Failed to report sink metrics", e);
                }
            }

            updateOperationMetrics(recordCount, true, 0);
        }

        public void recordFailure(long recordCount, int retryCount) {
            updateOperationMetrics(recordCount, false, retryCount);

            logger.warn("Operation failed for {} records after {} retries in component {}",
                    recordCount, retryCount, componentName);
        }

        private void updateOperationMetrics(long recordCount, boolean success, int retryCount) {
            totalOperations.addAndGet(recordCount);
            if (success) {
                successfulOperations.addAndGet(recordCount);
            } else {
                failedOperations.addAndGet(recordCount);
            }
            totalRetries.addAndGet(retryCount);
        }

        public void recordProcessingTime(long processingTimeMs) {
            totalProcessingTime.addAndGet(processingTimeMs);
        }

        public void logPeriodicStats() {
            long total = totalOperations.get();
            long successful = successfulOperations.get();
            long failed = failedOperations.get();
            long retries = totalRetries.get();
            long avgProcessingTime = total > 0 ? totalProcessingTime.get() / total : 0;

            if (total > 0) {
                double successRate = (successful * 100.0) / total;
                double avgRetries = (double) retries / total;

                logger.info("Component {} stats - Total: {}, Success: {} ({:.2f}%), Failed: {}, " +
                        "Avg retries: {:.2f}, Avg processing time: {}ms",
                        componentName, total, successful, successRate, failed,
                        avgRetries, avgProcessingTime);

                // Set MDC for structured logging
                MDC.put(METRIC_COMPONENT, componentName);
                MDC.put("total_operations", String.valueOf(total));
                MDC.put("successful_operations", String.valueOf(successful));
                MDC.put("failed_operations", String.valueOf(failed));
                MDC.put("success_rate", String.format("%.2f", successRate));
                MDC.put("avg_retries", String.format("%.2f", avgRetries));
                MDC.put("avg_processing_time", String.valueOf(avgProcessingTime));

                logger.info("Periodic stats logged for component {}", componentName);

                // Clear MDC
                MDC.clear();
            }
        }

        public long getTotalOperations() {
            return totalOperations.get();
        }

        public long getSuccessfulOperations() {
            return successfulOperations.get();
        }

        public long getFailedOperations() {
            return failedOperations.get();
        }

        public long getTotalRetries() {
            return totalRetries.get();
        }

        public double getSuccessRate() {
            long total = totalOperations.get();
            return total > 0 ? (successfulOperations.get() * 100.0) / total : 0.0;
        }
    }

    /**
     * Connection health monitor for tracking connection status.
     */
    public static class ConnectionHealthMonitor {

        private final String connectionId;
        private final Logger logger;
        private volatile boolean isHealthy = true;
        private volatile long lastHealthCheckTime = 0;
        private volatile long consecutiveFailures = 0;
        private volatile String lastError = null;

        public ConnectionHealthMonitor(String connectionId) {
            this.connectionId = connectionId;
            this.logger = LoggerFactory.getLogger(ConnectionHealthMonitor.class);
        }

        public void recordHealthCheck(boolean success, String errorMessage) {
            lastHealthCheckTime = System.currentTimeMillis();

            if (success) {
                if (!isHealthy) {
                    logger.info("Connection {} recovered after {} consecutive failures",
                            connectionId, consecutiveFailures);
                }
                isHealthy = true;
                consecutiveFailures = 0;
                lastError = null;
            } else {
                consecutiveFailures++;
                lastError = errorMessage;

                if (isHealthy) {
                    logger.warn("Connection {} became unhealthy: {}", connectionId, errorMessage);
                    isHealthy = false;
                } else {
                    logger.debug("Connection {} still unhealthy (failure #{}): {}",
                            connectionId, consecutiveFailures, errorMessage);
                }
            }

            // Log structured health status
            MDC.put("connection_id", connectionId);
            MDC.put("is_healthy", String.valueOf(isHealthy));
            MDC.put("consecutive_failures", String.valueOf(consecutiveFailures));
            MDC.put("last_check_time", String.valueOf(lastHealthCheckTime));
            if (lastError != null) {
                MDC.put("last_error", lastError);
            }

            logger.debug("Connection health check completed for {}", connectionId);
            MDC.clear();
        }

        public boolean isHealthy() {
            return isHealthy;
        }

        public long getConsecutiveFailures() {
            return consecutiveFailures;
        }

        public long getLastHealthCheckTime() {
            return lastHealthCheckTime;
        }

        public String getLastError() {
            return lastError;
        }
    }

    /**
     * Creates a performance tracker for an operation.
     */
    public static PerformanceTracker startOperation(String operationName, Logger logger) {
        return new PerformanceTracker(operationName, logger);
    }

    /**
     * Creates enhanced metrics wrapper.
     */
    public static EnhancedMetrics createMetrics(String componentName, MetricOption metricOption,
            MetricGroup metricGroup, boolean isSource) {
        return new EnhancedMetrics(componentName, metricOption, metricGroup, isSource);
    }

    /**
     * Creates connection health monitor.
     */
    public static ConnectionHealthMonitor createHealthMonitor(String connectionId) {
        return new ConnectionHealthMonitor(connectionId);
    }

    /**
     * Logs system resource information for monitoring.
     */
    public static void logSystemResources(Logger logger) {
        Runtime runtime = Runtime.getRuntime();
        long maxMemory = runtime.maxMemory();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;

        double memoryUsagePercent = (usedMemory * 100.0) / maxMemory;
        int availableProcessors = runtime.availableProcessors();

        MDC.put("max_memory_mb", String.valueOf(maxMemory / (1024 * 1024)));
        MDC.put("used_memory_mb", String.valueOf(usedMemory / (1024 * 1024)));
        MDC.put("memory_usage_percent", String.format("%.2f", memoryUsagePercent));
        MDC.put("available_processors", String.valueOf(availableProcessors));

        logger.debug("System resources - Memory: {:.2f}% ({} MB / {} MB), Processors: {}",
                memoryUsagePercent, usedMemory / (1024 * 1024), maxMemory / (1024 * 1024),
                availableProcessors);

        MDC.clear();
    }

    /**
     * Logs configuration information for debugging.
     */
    public static void logConfiguration(Logger logger, String component, Map<String, Object> config) {
        logger.info("Configuration for component {}: {}", component, config);

        MDC.put(METRIC_COMPONENT, component);
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            MDC.put("config_" + entry.getKey(), String.valueOf(entry.getValue()));
        }

        logger.debug("Configuration logged for component {}", component);

        // Clear config entries from MDC
        for (String key : config.keySet()) {
            MDC.remove("config_" + key);
        }
        MDC.remove(METRIC_COMPONENT);
    }
}