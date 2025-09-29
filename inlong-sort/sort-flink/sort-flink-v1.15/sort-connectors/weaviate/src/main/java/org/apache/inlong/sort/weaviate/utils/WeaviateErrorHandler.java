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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Enhanced error handling utility for Weaviate connector operations.
 * Provides standardized error codes, retry mechanisms, and logging.
 */
public class WeaviateErrorHandler implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(WeaviateErrorHandler.class);

    /**
     * Weaviate-specific error codes for better error categorization and handling.
     */
    public enum WeaviateErrorCode {

        // Connection errors (1000-1099)
        CONNECTION_FAILED(1001, "Failed to connect to Weaviate server", true),
        CONNECTION_TIMEOUT(1002, "Connection timeout to Weaviate server", true),
        AUTHENTICATION_FAILED(1003, "Authentication failed with Weaviate server", false),
        INVALID_URL(1004, "Invalid Weaviate server URL", false),
        SSL_HANDSHAKE_FAILED(1005, "SSL handshake failed", true),

        // Configuration errors (1100-1199)
        INVALID_CONFIG(1101, "Invalid configuration parameter", false),
        MISSING_REQUIRED_CONFIG(1102, "Missing required configuration parameter", false),
        INVALID_CLASS_NAME(1103, "Invalid Weaviate class name", false),
        INVALID_VECTOR_DIMENSION(1104, "Invalid vector dimension", false),
        INVALID_BATCH_SIZE(1105, "Invalid batch size configuration", false),

        // Data processing errors (1200-1299)
        DATA_CONVERSION_FAILED(1201, "Failed to convert data type", false),
        VECTOR_CONVERSION_FAILED(1202, "Failed to convert vector data", false),
        INVALID_VECTOR_FORMAT(1203, "Invalid vector data format", false),
        SCHEMA_MISMATCH(1204, "Schema mismatch between source and target", false),
        NULL_DATA_ERROR(1205, "Unexpected null data encountered", false),

        // Query errors (1300-1399)
        GRAPHQL_QUERY_FAILED(1301, "GraphQL query execution failed", true),
        INVALID_QUERY_SYNTAX(1302, "Invalid GraphQL query syntax", false),
        QUERY_TIMEOUT(1303, "Query execution timeout", true),
        RESULT_PARSING_FAILED(1304, "Failed to parse query results", false),
        EMPTY_RESULT_SET(1305, "Query returned empty result set", false),

        // Write operation errors (1400-1499)
        BATCH_WRITE_FAILED(1401, "Batch write operation failed", true),
        OBJECT_CREATION_FAILED(1402, "Failed to create Weaviate object", true),
        UPSERT_OPERATION_FAILED(1403, "Upsert operation failed", true),
        BATCH_SIZE_EXCEEDED(1404, "Batch size limit exceeded", false),
        WRITE_PERMISSION_DENIED(1405, "Write permission denied", false),

        // Resource errors (1500-1599)
        MEMORY_PRESSURE(1501, "System memory pressure detected", true),
        CONNECTION_POOL_EXHAUSTED(1502, "Connection pool exhausted", true),
        THREAD_POOL_EXHAUSTED(1503, "Thread pool exhausted", true),
        DISK_SPACE_INSUFFICIENT(1504, "Insufficient disk space", false),
        RESOURCE_CLEANUP_FAILED(1505, "Failed to cleanup resources", false),

        // Generic errors (1900-1999)
        UNKNOWN_ERROR(1901, "Unknown error occurred", true),
        OPERATION_INTERRUPTED(1902, "Operation was interrupted", false),
        SERIALIZATION_FAILED(1903, "Object serialization failed", false),
        DESERIALIZATION_FAILED(1904, "Object deserialization failed", false),
        VALIDATION_FAILED(1905, "Data validation failed", false);

        private final int code;
        private final String message;
        private final boolean retryable;

        WeaviateErrorCode(int code, String message, boolean retryable) {
            this.code = code;
            this.message = message;
            this.retryable = retryable;
        }

        public int getCode() {
            return code;
        }

        public String getMessage() {
            return message;
        }

        public boolean isRetryable() {
            return retryable;
        }
    }

    /**
     * Weaviate-specific exception with error codes and context.
     */
    public static class WeaviateException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        private final WeaviateErrorCode errorCode;
        private final String context;
        private final long timestamp;

        public WeaviateException(WeaviateErrorCode errorCode, String context) {
            super(formatMessage(errorCode, context));
            this.errorCode = errorCode;
            this.context = context;
            this.timestamp = System.currentTimeMillis();
        }

        public WeaviateException(WeaviateErrorCode errorCode, String context, Throwable cause) {
            super(formatMessage(errorCode, context), cause);
            this.errorCode = errorCode;
            this.context = context;
            this.timestamp = System.currentTimeMillis();
        }

        private static String formatMessage(WeaviateErrorCode errorCode, String context) {
            return String.format("[WE-%d] %s%s",
                    errorCode.getCode(),
                    errorCode.getMessage(),
                    context != null ? " - " + context : "");
        }

        public WeaviateErrorCode getErrorCode() {
            return errorCode;
        }

        public String getContext() {
            return context;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public boolean isRetryable() {
            return errorCode.isRetryable();
        }
    }

    /**
     * Retry configuration for different types of operations.
     */
    public static class RetryConfig {

        private final int maxRetries;
        private final Duration initialDelay;
        private final Duration maxDelay;
        private final double backoffMultiplier;
        private final boolean enableJitter;

        public RetryConfig(int maxRetries, Duration initialDelay, Duration maxDelay,
                double backoffMultiplier, boolean enableJitter) {
            this.maxRetries = maxRetries;
            this.initialDelay = initialDelay;
            this.maxDelay = maxDelay;
            this.backoffMultiplier = backoffMultiplier;
            this.enableJitter = enableJitter;
        }

        public static RetryConfig defaultConfig() {
            return new RetryConfig(3, Duration.ofSeconds(1), Duration.ofMinutes(1), 2.0, true);
        }

        public static RetryConfig connectionConfig() {
            return new RetryConfig(5, Duration.ofSeconds(2), Duration.ofMinutes(2), 1.5, true);
        }

        public static RetryConfig queryConfig() {
            return new RetryConfig(3, Duration.ofMillis(500), Duration.ofSeconds(30), 2.0, true);
        }

        public static RetryConfig writeConfig() {
            return new RetryConfig(4, Duration.ofSeconds(1), Duration.ofMinutes(1), 1.8, true);
        }

        public int getMaxRetries() {
            return maxRetries;
        }

        public Duration getInitialDelay() {
            return initialDelay;
        }

        public Duration getMaxDelay() {
            return maxDelay;
        }

        public double getBackoffMultiplier() {
            return backoffMultiplier;
        }

        public boolean isEnableJitter() {
            return enableJitter;
        }
    }

    /**
     * Executes an operation with retry logic and enhanced error handling.
     */
    public static <T> T executeWithRetry(String operationName, RetryableOperation<T> operation,
            RetryConfig retryConfig) throws WeaviateException {
        return executeWithRetry(operationName, operation, retryConfig, LOG);
    }

    /**
     * Executes an operation with retry logic using custom logger.
     */
    public static <T> T executeWithRetry(String operationName, RetryableOperation<T> operation,
            RetryConfig retryConfig, Logger logger) throws WeaviateException {
        WeaviateException lastException = null;
        long startTime = System.currentTimeMillis();

        for (int attempt = 1; attempt <= retryConfig.getMaxRetries() + 1; attempt++) {
            try {
                logger.debug("Executing {} - attempt {}/{}", operationName, attempt, retryConfig.getMaxRetries() + 1);

                T result = operation.execute();

                if (attempt > 1) {
                    long totalTime = System.currentTimeMillis() - startTime;
                    logger.info("Operation {} succeeded on attempt {} after {}ms",
                            operationName, attempt, totalTime);
                }

                return result;

            } catch (WeaviateException e) {
                lastException = e;
                logger.warn("Operation {} failed on attempt {}/{}: [{}] {}",
                        operationName, attempt, retryConfig.getMaxRetries() + 1,
                        e.getErrorCode().getCode(), e.getMessage());

                // Don't retry if error is not retryable or we've exhausted attempts
                if (!e.isRetryable() || attempt > retryConfig.getMaxRetries()) {
                    break;
                }

                // Calculate delay for next attempt
                if (attempt <= retryConfig.getMaxRetries()) {
                    long delayMs = calculateRetryDelay(attempt, retryConfig);
                    logger.debug("Retrying {} in {}ms", operationName, delayMs);

                    try {
                        Thread.sleep(delayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new WeaviateException(WeaviateErrorCode.OPERATION_INTERRUPTED,
                                "Operation interrupted during retry delay");
                    }
                }

            } catch (Exception e) {
                // Wrap unexpected exceptions
                lastException = new WeaviateException(WeaviateErrorCode.UNKNOWN_ERROR,
                        "Unexpected error in " + operationName, e);
                logger.error("Unexpected error in {} on attempt {}/{}",
                        operationName, attempt, retryConfig.getMaxRetries() + 1, e);
                break;
            }
        }

        long totalTime = System.currentTimeMillis() - startTime;
        logger.error("Operation {} failed after {} attempts in {}ms",
                operationName, retryConfig.getMaxRetries() + 1, totalTime);

        throw lastException != null ? lastException
                : new WeaviateException(WeaviateErrorCode.UNKNOWN_ERROR, "Operation failed with unknown error");
    }

    /**
     * Calculates retry delay with exponential backoff and optional jitter.
     */
    private static long calculateRetryDelay(int attempt, RetryConfig config) {
        long baseDelay = config.getInitialDelay().toMillis();
        long delay = (long) (baseDelay * Math.pow(config.getBackoffMultiplier(), attempt - 1));

        // Cap at max delay
        delay = Math.min(delay, config.getMaxDelay().toMillis());

        // Add jitter if enabled
        if (config.isEnableJitter()) {
            double jitterFactor = 0.1; // 10% jitter
            long jitter = (long) (delay * jitterFactor * ThreadLocalRandom.current().nextDouble());
            delay += ThreadLocalRandom.current().nextBoolean() ? jitter : -jitter;
        }

        return Math.max(delay, 0);
    }

    /**
     * Logs detailed error information for debugging and monitoring.
     */
    public static void logError(Logger logger, String operation, WeaviateException e, Object... context) {
        StringBuilder contextStr = new StringBuilder();
        if (context != null && context.length > 0) {
            contextStr.append(" Context: ");
            for (int i = 0; i < context.length; i += 2) {
                if (i > 0)
                    contextStr.append(", ");
                if (i + 1 < context.length) {
                    contextStr.append(context[i]).append("=").append(context[i + 1]);
                } else {
                    contextStr.append(context[i]);
                }
            }
        }

        logger.error("Weaviate operation '{}' failed: [WE-{}] {} - {} (retryable: {}){}",
                operation, e.getErrorCode().getCode(), e.getErrorCode().getMessage(),
                e.getContext(), e.isRetryable(), contextStr.toString(), e);
    }

    /**
     * Logs warning for retryable errors.
     */
    public static void logRetryableError(Logger logger, String operation, WeaviateException e,
            int attempt, int maxAttempts) {
        logger.warn("Weaviate operation '{}' failed on attempt {}/{}: [WE-{}] {} - {} (will retry: {})",
                operation, attempt, maxAttempts, e.getErrorCode().getCode(),
                e.getErrorCode().getMessage(), e.getContext(), attempt < maxAttempts);
    }

    /**
     * Creates a standardized error context string.
     */
    public static String createContext(String component, Object... keyValuePairs) {
        StringBuilder context = new StringBuilder(component);
        if (keyValuePairs != null && keyValuePairs.length > 0) {
            context.append(" [");
            for (int i = 0; i < keyValuePairs.length; i += 2) {
                if (i > 0)
                    context.append(", ");
                if (i + 1 < keyValuePairs.length) {
                    context.append(keyValuePairs[i]).append("=").append(keyValuePairs[i + 1]);
                } else {
                    context.append(keyValuePairs[i]);
                }
            }
            context.append("]");
        }
        return context.toString();
    }

    /**
     * Validates configuration and throws appropriate errors.
     */
    public static void validateConfig(String paramName, Object value, String component) {
        if (value == null) {
            throw new WeaviateException(WeaviateErrorCode.MISSING_REQUIRED_CONFIG,
                    createContext(component, "parameter", paramName));
        }

        if (value instanceof String && ((String) value).trim().isEmpty()) {
            throw new WeaviateException(WeaviateErrorCode.INVALID_CONFIG,
                    createContext(component, "parameter", paramName, "reason", "empty string"));
        }

        if (value instanceof Number && ((Number) value).doubleValue() < 0) {
            throw new WeaviateException(WeaviateErrorCode.INVALID_CONFIG,
                    createContext(component, "parameter", paramName, "reason", "negative value"));
        }
    }

    /**
     * Wraps exceptions with appropriate Weaviate error codes.
     */
    public static WeaviateException wrapException(Throwable cause, String context) {
        if (cause instanceof WeaviateException) {
            return (WeaviateException) cause;
        }

        String message = cause.getMessage();
        if (message == null) {
            message = cause.getClass().getSimpleName();
        }

        // Categorize common exceptions
        if (cause instanceof java.net.ConnectException ||
                cause instanceof java.net.NoRouteToHostException) {
            return new WeaviateException(WeaviateErrorCode.CONNECTION_FAILED, context, cause);
        }

        if (cause instanceof java.net.SocketTimeoutException ||
                cause instanceof java.util.concurrent.TimeoutException) {
            return new WeaviateException(WeaviateErrorCode.CONNECTION_TIMEOUT, context, cause);
        }

        if (cause instanceof java.security.cert.CertificateException ||
                cause instanceof javax.net.ssl.SSLHandshakeException) {
            return new WeaviateException(WeaviateErrorCode.SSL_HANDSHAKE_FAILED, context, cause);
        }

        if (cause instanceof InterruptedException) {
            return new WeaviateException(WeaviateErrorCode.OPERATION_INTERRUPTED, context, cause);
        }

        if (cause instanceof OutOfMemoryError) {
            return new WeaviateException(WeaviateErrorCode.MEMORY_PRESSURE, context, cause);
        }

        // Default to unknown error
        return new WeaviateException(WeaviateErrorCode.UNKNOWN_ERROR, context, cause);
    }

    /**
     * Functional interface for retryable operations.
     */
    @FunctionalInterface
    public interface RetryableOperation<T> {

        T execute() throws Exception;
    }
}