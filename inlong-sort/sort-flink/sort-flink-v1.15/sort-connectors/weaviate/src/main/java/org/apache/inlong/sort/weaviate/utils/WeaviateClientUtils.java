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

import org.apache.inlong.sort.weaviate.utils.WeaviateErrorHandler.WeaviateErrorCode;
import org.apache.inlong.sort.weaviate.utils.WeaviateErrorHandler.WeaviateException;
import org.apache.inlong.sort.weaviate.utils.WeaviateMonitoringUtils.ConnectionHealthMonitor;

import io.weaviate.client.Config;
import io.weaviate.client.WeaviateClient;
import io.weaviate.client.v1.misc.model.Meta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Utility class for creating and managing Weaviate clients.
 * Provides methods for client creation, connection testing, health checks, and
 * connection pooling.
 */
public class WeaviateClientUtils {

    private static final Logger LOG = LoggerFactory.getLogger(WeaviateClientUtils.class);

    // Connection pool management
    private static final ConcurrentHashMap<String, WeaviateConnectionPool> CONNECTION_POOLS = new ConcurrentHashMap<>();

    // Default configuration values
    private static final Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration DEFAULT_READ_TIMEOUT = Duration.ofSeconds(60);
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final Duration DEFAULT_RETRY_DELAY = Duration.ofSeconds(1);
    private static final int DEFAULT_POOL_SIZE = 10;

    private WeaviateClientUtils() {
        // Utility class, prevent instantiation
    }

    /**
     * Creates a Weaviate client with the specified configuration.
     *
     * @param url    Weaviate server URL
     * @param apiKey API key for authentication (can be null)
     * @return WeaviateClient instance
     * @throws WeaviateException if client creation fails
     */
    public static WeaviateClient createClient(String url, String apiKey) throws WeaviateException {
        return createClient(url, apiKey, DEFAULT_CONNECTION_TIMEOUT, DEFAULT_READ_TIMEOUT);
    }

    /**
     * Creates a Weaviate client with custom timeout settings and enhanced error
     * handling.
     *
     * @param url               Weaviate server URL
     * @param apiKey            API key for authentication (can be null)
     * @param connectionTimeout Connection timeout duration
     * @param readTimeout       Read timeout duration
     * @return WeaviateClient instance
     * @throws WeaviateException if client creation fails
     */
    public static WeaviateClient createClient(String url, String apiKey, Duration connectionTimeout,
            Duration readTimeout) throws WeaviateException {

        String context = WeaviateErrorHandler.createContext("WeaviateClientUtils.createClient",
                "url", url, "hasApiKey", apiKey != null);

        // Enhanced validation with proper error codes
        WeaviateErrorHandler.validateConfig("url", url, "WeaviateClientUtils");
        WeaviateErrorHandler.validateConfig("connectionTimeout", connectionTimeout, "WeaviateClientUtils");
        WeaviateErrorHandler.validateConfig("readTimeout", readTimeout, "WeaviateClientUtils");

        try {
            URL parsedUrl = new URL(url);
            String scheme = parsedUrl.getProtocol();
            String host = parsedUrl.getHost();
            int port = parsedUrl.getPort();

            // Enhanced URL validation
            if (scheme == null || scheme.trim().isEmpty()) {
                throw new WeaviateException(WeaviateErrorCode.INVALID_URL,
                        context + " - URL scheme cannot be null or empty");
            }
            if (host == null || host.trim().isEmpty()) {
                throw new WeaviateException(WeaviateErrorCode.INVALID_URL,
                        context + " - URL host cannot be null or empty");
            }

            // Ensure scheme is supported (http or https)
            if (!scheme.equalsIgnoreCase("http") && !scheme.equalsIgnoreCase("https")) {
                throw new WeaviateException(WeaviateErrorCode.INVALID_URL,
                        context + " - Unsupported URL scheme: " + scheme + ". Only http and https are supported");
            }

            // Enhanced timeout validation
            if (connectionTimeout.isNegative()) {
                throw new WeaviateException(WeaviateErrorCode.INVALID_CONFIG,
                        context + " - connectionTimeout must be non-negative, got: " + connectionTimeout);
            }
            if (readTimeout.isNegative()) {
                throw new WeaviateException(WeaviateErrorCode.INVALID_CONFIG,
                        context + " - readTimeout must be non-negative, got: " + readTimeout);
            }

            // Build the host string properly
            String hostString = host;
            if (port != -1) {
                hostString = host + ":" + port;
            }

            // Set timeouts with proper bounds checking
            long connectionTimeoutMs = connectionTimeout.toMillis();
            long readTimeoutMs = readTimeout.toMillis();

            // Ensure timeouts don't exceed Integer.MAX_VALUE
            if (connectionTimeoutMs > Integer.MAX_VALUE) {
                throw new WeaviateException(WeaviateErrorCode.INVALID_CONFIG,
                        context + " - connectionTimeout too large: " + connectionTimeoutMs + "ms");
            }
            if (readTimeoutMs > Integer.MAX_VALUE) {
                throw new WeaviateException(WeaviateErrorCode.INVALID_CONFIG,
                        context + " - readTimeout too large: " + readTimeoutMs + "ms");
            }

            // Create Config with basic constructor
            Config config = new Config(scheme, hostString);

            // Try to set timeouts if the methods exist
            try {
                // Some versions of Config may have these methods
                java.lang.reflect.Method setConnTimeout = config.getClass().getMethod("setConnectionTimeout",
                        int.class);
                java.lang.reflect.Method setReadTimeout = config.getClass().getMethod("setReadTimeout", int.class);

                setConnTimeout.invoke(config, (int) connectionTimeoutMs);
                setReadTimeout.invoke(config, (int) readTimeoutMs);

                LOG.debug("Successfully set timeouts: connection={}ms, read={}ms",
                        connectionTimeoutMs, readTimeoutMs);
            } catch (Exception e) {
                // Methods don't exist or failed to invoke - this is expected for some versions
                LOG.debug("Timeout methods not available on Config class, using defaults. " +
                        "Requested: connection={}ms, read={}ms", connectionTimeoutMs, readTimeoutMs);
            }

            // Add API key to headers if provided
            if (apiKey != null && !apiKey.trim().isEmpty()) {
                try {
                    // Weaviate typically uses Authorization header with Bearer token
                    config.getHeaders().put("Authorization", "Bearer " + apiKey);
                    LOG.debug("API key configured for authentication");
                } catch (Exception e) {
                    LOG.warn("Failed to set API key in headers, authentication may fail", e);
                }
            }

            // Create WeaviateClient using constructor
            WeaviateClient client = new WeaviateClient(config);
            LOG.info("Successfully created Weaviate client for URL: {} with timeouts: connection={}ms, read={}ms",
                    url, connectionTimeoutMs, readTimeoutMs);
            return client;

        } catch (MalformedURLException e) {
            throw new WeaviateException(WeaviateErrorCode.INVALID_URL, context + " - Malformed URL", e);
        } catch (WeaviateException e) {
            // Re-throw Weaviate exceptions as-is
            throw e;
        } catch (Exception e) {
            LOG.error("Unexpected error creating Weaviate client for URL: {}", url, e);
            throw WeaviateErrorHandler.wrapException(e, context);
        }
    }

    /**
     * Creates a Weaviate client with retry mechanism.
     *
     * @param url        Weaviate server URL
     * @param apiKey     API key for authentication (can be null)
     * @param maxRetries Maximum number of retry attempts
     * @return WeaviateClient instance
     * @throws WeaviateException if all retry attempts fail
     */
    public static WeaviateClient createClientWithRetry(String url, String apiKey, int maxRetries)
            throws WeaviateException {
        return createClientWithRetry(url, apiKey, maxRetries, DEFAULT_RETRY_DELAY);
    }

    /**
     * Creates a Weaviate client with enhanced retry mechanism and monitoring.
     *
     * @param url        Weaviate server URL
     * @param apiKey     API key for authentication (can be null)
     * @param maxRetries Maximum number of retry attempts
     * @param retryDelay Delay between retry attempts
     * @return WeaviateClient instance
     * @throws WeaviateException if all retry attempts fail
     */
    public static WeaviateClient createClientWithRetry(String url, String apiKey, int maxRetries, Duration retryDelay)
            throws WeaviateException {

        String context = WeaviateErrorHandler.createContext("WeaviateClientUtils.createClientWithRetry",
                "url", url, "maxRetries", maxRetries);

        // Enhanced validation
        if (maxRetries <= 0) {
            throw new WeaviateException(WeaviateErrorCode.INVALID_CONFIG,
                    context + " - maxRetries must be positive, got: " + maxRetries);
        }
        if (retryDelay == null || retryDelay.isNegative()) {
            throw new WeaviateException(WeaviateErrorCode.INVALID_CONFIG,
                    context + " - retryDelay must be non-null and non-negative");
        }

        // Use enhanced retry mechanism
        WeaviateErrorHandler.RetryConfig retryConfig = new WeaviateErrorHandler.RetryConfig(
                maxRetries, retryDelay, Duration.ofMinutes(2), 2.0, true);

        return WeaviateErrorHandler.executeWithRetry(
                "createWeaviateClient",
                () -> {
                    WeaviateClient client = createClient(url, apiKey);

                    // Enhanced connection test with health monitoring
                    if (!testConnectionWithHealthCheck(client, url)) {
                        throw new WeaviateException(WeaviateErrorCode.CONNECTION_FAILED,
                                context + " - Connection test failed");
                    }

                    return client;
                },
                retryConfig,
                LOG);
    }

    /**
     * Tests the connection to Weaviate server with basic validation.
     *
     * @param client WeaviateClient instance
     * @return true if connection is successful, false otherwise
     */
    public static boolean testConnection(WeaviateClient client) {
        if (client == null) {
            LOG.debug("Connection test failed: client is null");
            return false;
        }

        try {
            // Try to get server meta information
            Meta meta = client.misc().metaGetter().run().getResult();
            if (meta != null) {
                LOG.debug("Connection test successful. Server version: {}", meta.getVersion());
                return true;
            }
            return false;
        } catch (Exception e) {
            LOG.debug("Connection test failed: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Enhanced connection test with health monitoring and detailed logging.
     *
     * @param client WeaviateClient instance
     * @param url    Server URL for context
     * @return true if connection is successful, false otherwise
     */
    public static boolean testConnectionWithHealthCheck(WeaviateClient client, String url) {
        if (client == null) {
            LOG.warn("Connection test failed: client is null for URL: {}", url);
            return false;
        }

        String context = WeaviateErrorHandler.createContext("testConnectionWithHealthCheck", "url", url);
        ConnectionHealthMonitor healthMonitor = new ConnectionHealthMonitor(url);

        try {
            LOG.debug("Testing connection to Weaviate server: {}", url);

            // Try to get server meta information with timeout
            Meta meta = client.misc().metaGetter().run().getResult();

            if (meta != null) {
                String version = meta.getVersion();
                LOG.info("Connection test successful for URL: {} - Server version: {}", url, version);

                // Record successful health check
                healthMonitor.recordHealthCheck(true, null);

                // Additional validation - try to get server readiness
                try {
                    Boolean ready = client.misc().readyChecker().run().getResult();
                    if (Boolean.TRUE.equals(ready)) {
                        LOG.debug("Server readiness check passed for URL: {}", url);
                    } else {
                        LOG.warn("Server readiness check failed for URL: {} - server not ready", url);
                    }
                } catch (Exception e) {
                    LOG.debug("Server readiness check failed for URL: {} - {}", url, e.getMessage());
                }

                return true;
            } else {
                String errorMsg = "Server meta information is null";
                LOG.warn("Connection test failed for URL: {} - {}", url, errorMsg);
                healthMonitor.recordHealthCheck(false, errorMsg);
                return false;
            }

        } catch (Exception e) {
            String errorMsg = "Connection test exception: " + e.getMessage();
            LOG.warn("Connection test failed for URL: {} - {}", url, errorMsg, e);
            healthMonitor.recordHealthCheck(false, errorMsg);
            return false;
        }
    }

    /**
     * Performs an enhanced health check on the Weaviate server with detailed
     * monitoring.
     *
     * @param client WeaviateClient instance
     * @param url    Server URL for context
     * @return true if server is healthy, false otherwise
     */
    public static boolean healthCheck(WeaviateClient client, String url) {
        if (client == null) {
            LOG.warn("Health check failed: client is null for URL: {}", url);
            return false;
        }

        String context = WeaviateErrorHandler.createContext("healthCheck", "url", url);
        ConnectionHealthMonitor healthMonitor = new ConnectionHealthMonitor(url);

        try {
            LOG.debug("Performing health check for Weaviate server: {}", url);

            // Get server readiness status
            Boolean ready = client.misc().readyChecker().run().getResult();

            if (Boolean.TRUE.equals(ready)) {
                LOG.debug("Health check successful - server is ready for URL: {}", url);
                healthMonitor.recordHealthCheck(true, null);

                // Additional health checks
                try {
                    // Try to get server meta for additional validation
                    Meta meta = client.misc().metaGetter().run().getResult();
                    if (meta != null) {
                        LOG.debug("Extended health check passed - server meta available for URL: {}", url);
                    }
                } catch (Exception e) {
                    LOG.debug("Extended health check warning for URL: {} - {}", url, e.getMessage());
                }

                return true;
            } else {
                String errorMsg = "Server is not ready";
                LOG.warn("Health check failed for URL: {} - {}", url, errorMsg);
                healthMonitor.recordHealthCheck(false, errorMsg);
                return false;
            }

        } catch (Exception e) {
            String errorMsg = "Health check exception: " + e.getMessage();
            LOG.warn("Health check failed for URL: {} - {}", url, errorMsg, e);
            healthMonitor.recordHealthCheck(false, errorMsg);
            return false;
        }
    }

    /**
     * Performs a health check on the Weaviate server (legacy method for backward
     * compatibility).
     *
     * @param client WeaviateClient instance
     * @return true if server is healthy, false otherwise
     */
    public static boolean healthCheck(WeaviateClient client) {
        return healthCheck(client, "unknown");
    }

    /**
     * Gets or creates a connection pool for the specified configuration.
     *
     * @param url      Weaviate server URL
     * @param apiKey   API key for authentication (can be null)
     * @param poolSize Maximum pool size
     * @return WeaviateConnectionPool instance
     */
    public static WeaviateConnectionPool getConnectionPool(String url, String apiKey, int poolSize) {
        if (poolSize <= 0) {
            throw new IllegalArgumentException("poolSize must be positive, got: " + poolSize);
        }

        String poolKey = generatePoolKey(url, apiKey);
        return CONNECTION_POOLS.computeIfAbsent(poolKey, k -> new WeaviateConnectionPool(url, apiKey, poolSize));
    }

    /**
     * Gets or creates a connection pool with default size.
     *
     * @param url    Weaviate server URL
     * @param apiKey API key for authentication (can be null)
     * @return WeaviateConnectionPool instance
     */
    public static WeaviateConnectionPool getConnectionPool(String url, String apiKey) {
        return getConnectionPool(url, apiKey, DEFAULT_POOL_SIZE);
    }

    /**
     * Closes and removes a connection pool.
     *
     * @param url    Weaviate server URL
     * @param apiKey API key for authentication (can be null)
     */
    public static void closeConnectionPool(String url, String apiKey) {
        String poolKey = generatePoolKey(url, apiKey);
        WeaviateConnectionPool pool = CONNECTION_POOLS.remove(poolKey);
        if (pool != null) {
            pool.close();
            LOG.info("Closed connection pool for: {}", url);
        }
    }

    /**
     * Closes all connection pools.
     */
    public static void closeAllConnectionPools() {
        CONNECTION_POOLS.values().forEach(WeaviateConnectionPool::close);
        CONNECTION_POOLS.clear();
        LOG.info("Closed all connection pools");
    }

    /**
     * Generates a unique key for connection pool identification.
     */
    private static String generatePoolKey(String url, String apiKey) {
        return url + "#" + (apiKey != null ? apiKey.hashCode() : "noauth");
    }

    /**
     * Connection pool implementation for managing Weaviate client instances.
     */
    public static class WeaviateConnectionPool {

        private final String url;
        private final String apiKey;
        private final int maxPoolSize;
        private final BlockingQueue<WeaviateClient> availableClients;
        private final AtomicInteger currentPoolSize;
        private volatile boolean closed = false;

        public WeaviateConnectionPool(String url, String apiKey, int maxPoolSize) {
            if (url == null || url.trim().isEmpty()) {
                throw new IllegalArgumentException("URL cannot be null or empty");
            }
            if (maxPoolSize <= 0) {
                throw new IllegalArgumentException("maxPoolSize must be positive, got: " + maxPoolSize);
            }

            this.url = url;
            this.apiKey = apiKey;
            this.maxPoolSize = maxPoolSize;
            this.availableClients = new LinkedBlockingQueue<>();
            this.currentPoolSize = new AtomicInteger(0);
        }

        /**
         * Borrows a client from the pool with enhanced error handling and monitoring.
         *
         * @return WeaviateClient instance
         * @throws InterruptedException if interrupted while waiting
         * @throws WeaviateException    if pool is closed or client creation fails
         */
        public WeaviateClient borrowClient() throws InterruptedException, WeaviateException {
            if (closed) {
                throw new WeaviateException(WeaviateErrorCode.CONNECTION_POOL_EXHAUSTED,
                        WeaviateErrorHandler.createContext("WeaviateConnectionPool.borrowClient",
                                "url", url, "reason", "pool is closed"));
            }

            String context = WeaviateErrorHandler.createContext("WeaviateConnectionPool.borrowClient",
                    "url", url, "poolSize", currentPoolSize.get(),
                    "maxPoolSize", maxPoolSize);

            LOG.debug("Borrowing client from pool: {}", context);

            WeaviateClient client = availableClients.poll();
            if (client != null) {
                // Test the client before returning with enhanced health check
                if (testConnectionWithHealthCheck(client, url)) {
                    LOG.debug("Reusing existing client from pool for URL: {}", url);
                    return client;
                } else {
                    // Client is not healthy, discard it
                    currentPoolSize.decrementAndGet();
                    LOG.warn("Discarded unhealthy client from pool for URL: {}", url);
                }
            }

            // Create new client if pool is not full
            if (currentPoolSize.get() < maxPoolSize) {
                try {
                    LOG.debug("Creating new client for pool: {}", context);
                    client = createClientWithRetry(url, apiKey, DEFAULT_MAX_RETRIES, DEFAULT_RETRY_DELAY);
                    currentPoolSize.incrementAndGet();
                    LOG.info("Created new client for pool, current size: {}/{}", currentPoolSize.get(), maxPoolSize);
                    return client;
                } catch (WeaviateException e) {
                    LOG.error("Failed to create new client for pool: {}", context, e);
                    throw e;
                } catch (Exception e) {
                    LOG.error("Unexpected error creating new client for pool: {}", context, e);
                    throw WeaviateErrorHandler.wrapException(e, context);
                }
            }

            // Wait for available client with timeout
            LOG.debug("Pool is full, waiting for available client: {}", context);
            client = availableClients.poll(30, TimeUnit.SECONDS);
            if (client == null) {
                throw new WeaviateException(WeaviateErrorCode.CONNECTION_POOL_EXHAUSTED,
                        context + " - Timeout waiting for available client");
            }

            // Test the borrowed client
            if (!testConnectionWithHealthCheck(client, url)) {
                currentPoolSize.decrementAndGet();
                throw new WeaviateException(WeaviateErrorCode.CONNECTION_FAILED,
                        context + " - Borrowed client failed health check");
            }

            LOG.debug("Successfully borrowed client from pool after waiting: {}", url);
            return client;
        }

        /**
         * Returns a client to the pool with enhanced health checking.
         *
         * @param client WeaviateClient to return
         */
        public void returnClient(WeaviateClient client) {
            if (closed) {
                LOG.debug("Pool is closed, not returning client for URL: {}", url);
                return;
            }

            if (client == null) {
                LOG.warn("Attempted to return null client to pool for URL: {}", url);
                return;
            }

            String context = WeaviateErrorHandler.createContext("WeaviateConnectionPool.returnClient",
                    "url", url, "poolSize", currentPoolSize.get());

            // Enhanced health check before returning to pool
            if (testConnectionWithHealthCheck(client, url)) {
                boolean offered = availableClients.offer(client);
                if (offered) {
                    LOG.debug("Successfully returned healthy client to pool: {}", context);
                } else {
                    LOG.warn("Failed to return client to pool (queue full?): {}", context);
                    currentPoolSize.decrementAndGet();
                }
            } else {
                // Client is unhealthy, don't return to pool
                currentPoolSize.decrementAndGet();
                LOG.warn("Discarded unhealthy client instead of returning to pool: {}", context);
            }
        }

        /**
         * Gets the current pool size.
         *
         * @return current number of clients in the pool
         */
        public int getCurrentPoolSize() {
            return currentPoolSize.get();
        }

        /**
         * Gets the number of available clients.
         *
         * @return number of available clients
         */
        public int getAvailableClients() {
            return availableClients.size();
        }

        /**
         * Closes the connection pool and all clients.
         */
        public void close() {
            closed = true;

            // Close all clients in the pool
            WeaviateClient client;
            while ((client = availableClients.poll()) != null) {
                try {
                    // Weaviate client doesn't have explicit close method
                    // The client will be garbage collected
                } catch (Exception e) {
                    LOG.warn("Error closing client", e);
                }
            }

            currentPoolSize.set(0);
            LOG.info("Connection pool closed for URL: {}", url);
        }

        /**
         * Checks if the pool is closed.
         *
         * @return true if pool is closed
         */
        public boolean isClosed() {
            return closed;
        }
    }
}