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

import org.apache.inlong.sort.base.metric.MetricOption;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkState;
import static org.apache.inlong.sort.base.Constants.AUDIT_KEYS;
import static org.apache.inlong.sort.base.Constants.INLONG_AUDIT;
import static org.apache.inlong.sort.base.Constants.INLONG_METRIC;
import static org.apache.inlong.sort.weaviate.table.WeaviateOptions.API_KEY;
import static org.apache.inlong.sort.weaviate.table.WeaviateOptions.BATCH_SIZE;
import static org.apache.inlong.sort.weaviate.table.WeaviateOptions.CLASS_NAME;
import static org.apache.inlong.sort.weaviate.table.WeaviateOptions.CONNECTION_TIMEOUT;
import static org.apache.inlong.sort.weaviate.table.WeaviateOptions.CONSISTENCY_LEVEL;
import static org.apache.inlong.sort.weaviate.table.WeaviateOptions.FLUSH_INTERVAL;
import static org.apache.inlong.sort.weaviate.table.WeaviateOptions.IGNORE_NULL_VALUES;
import static org.apache.inlong.sort.weaviate.table.WeaviateOptions.MAX_BATCH_MEMORY;
import static org.apache.inlong.sort.weaviate.table.WeaviateOptions.MAX_RETRIES;
import static org.apache.inlong.sort.weaviate.table.WeaviateOptions.QUERY_CONDITIONS;
import static org.apache.inlong.sort.weaviate.table.WeaviateOptions.QUERY_LIMIT;
import static org.apache.inlong.sort.weaviate.table.WeaviateOptions.QUERY_OFFSET;
import static org.apache.inlong.sort.weaviate.table.WeaviateOptions.READ_TIMEOUT;
import static org.apache.inlong.sort.weaviate.table.WeaviateOptions.RETRY_DELAY;
import static org.apache.inlong.sort.weaviate.table.WeaviateOptions.UPSERT_ENABLED;
import static org.apache.inlong.sort.weaviate.table.WeaviateOptions.URL;
import static org.apache.inlong.sort.weaviate.table.WeaviateOptions.VECTOR_DIMENSION;
import static org.apache.inlong.sort.weaviate.table.WeaviateOptions.VECTOR_FIELD;

/**
 * Weaviate dynamic table factory for creating Weaviate table source and sink.
 * This factory implements both DynamicTableSourceFactory and
 * DynamicTableSinkFactory
 * to support reading from and writing to Weaviate vector database with enhanced
 * error handling.
 */
public class WeaviateDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private static final Logger LOG = LoggerFactory.getLogger(WeaviateDynamicTableFactory.class);

    /**
     * The identifier of Weaviate Connector.
     */
    public static final String IDENTIFIER = "weaviate-inlong";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();

        helper.validate();
        validateConfigOptions(config);

        // Build metric option for InLong audit
        MetricOption metricOption = buildMetricOption(config);

        return new WeaviateDynamicTableSource(
                context.getCatalogTable().getResolvedSchema(),
                config,
                metricOption);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();

        helper.validate();
        validateConfigOptions(config);

        // Build metric option for InLong audit
        MetricOption metricOption = buildMetricOption(config);

        return new WeaviateDynamicTableSink(
                context.getCatalogTable().getResolvedSchema(),
                config,
                metricOption);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(URL);
        requiredOptions.add(CLASS_NAME);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> optionalOptions = new HashSet<>();

        // Connection options
        optionalOptions.add(API_KEY);
        optionalOptions.add(CONNECTION_TIMEOUT);
        optionalOptions.add(READ_TIMEOUT);
        optionalOptions.add(MAX_RETRIES);
        optionalOptions.add(RETRY_DELAY);

        // Batch processing options
        optionalOptions.add(BATCH_SIZE);
        optionalOptions.add(FLUSH_INTERVAL);
        optionalOptions.add(MAX_BATCH_MEMORY);

        // Vector field options
        optionalOptions.add(VECTOR_FIELD);
        optionalOptions.add(VECTOR_DIMENSION);

        // Source specific options
        optionalOptions.add(QUERY_CONDITIONS);
        optionalOptions.add(QUERY_LIMIT);
        optionalOptions.add(QUERY_OFFSET);

        // Sink specific options
        optionalOptions.add(UPSERT_ENABLED);
        optionalOptions.add(IGNORE_NULL_VALUES);
        optionalOptions.add(CONSISTENCY_LEVEL);

        // InLong metric options
        optionalOptions.add(INLONG_METRIC);
        optionalOptions.add(INLONG_AUDIT);
        optionalOptions.add(AUDIT_KEYS);

        return optionalOptions;
    }

    /**
     * Validates the configuration options with enha
     *
     * @param config the readable configuration
     */
    private void validateConfigOptions(ReadableConfig config) {
        // Enhanced URL validation
        String url = config.get(URL);
        checkState(!StringUtils.isNullOrWhitespaceOnly(url),
                "Weaviate URL cannot be empty or null");

        // Validate URL format
        try {
            java.net.URL urlObj = new java.net.URL(url);
            String scheme = urlObj.getProtocol();
            checkState("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme),
                    "Weaviate URL must use http or https protocol, but got: " + scheme);
        } catch (java.net.MalformedURLException e) {
            throw new IllegalStateException("Invalid Weaviate URL format: " + url, e);
        }

        // Enhanced class name validation
        String className = config.get(CLASS_NAME);
        checkState(!StringUtils.isNullOrWhitespaceOnly(className),
                "Weaviate class name cannot be empty or null");
        checkState(className.matches("^[A-Z][a-zA-Z0-9_]*$"),
                "Weaviate class name should start with uppercase letter and contain only alphanumeric characters and underscores, but got: "
                        + className);

        // Enhanced batch size validation
        Integer batchSize = config.get(BATCH_SIZE);
        checkState(batchSize > 0,
                "Batch size must be greater than 0, but got: " + batchSize);
        checkState(batchSize <= 10000,
                "Batch size too large (max 10000 for performance), but got: " + batchSize);

        // Enhanced query limit validation for source
        Integer queryLimit = config.get(QUERY_LIMIT);
        checkState(queryLimit > 0,
                "Query limit must be greater than 0, but got: " + queryLimit);
        checkState(queryLimit <= 100000,
                "Query limit too large (max 100000 for performance), but got: " + queryLimit);

        // Enhanced query offset validation for source
        Integer queryOffset = config.get(QUERY_OFFSET);
        checkState(queryOffset >= 0,
                "Query offset must be greater than or equal to 0, but got: " + queryOffset);

        // Enhanced vector dimension validation
        Integer vectorDimension = config.getOptional(VECTOR_DIMENSION).orElse(null);
        if (vectorDimension != null) {
            checkState(vectorDimension > 0,
                    "Vector dimension must be greater than 0, but got: " + vectorDimension);
            checkState(vectorDimension <= 4096,
                    "Vector dimension too large (max 4096 for most models), but got: "
                            + vectorDimension);
        }

        // Enhanced max retries validation
        Integer maxRetries = config.get(MAX_RETRIES);
        checkState(maxRetries >= 0,
                "Max retries must be greater than or equal to 0, but got: " + maxRetries);
        checkState(maxRetries <= 10,
                "Max retries too large (max 10 to avoid long delays), but got: " + maxRetries);

        // Enhanced consistency level validation
        String consistencyLevel = config.get(CONSISTENCY_LEVEL);
        if (!StringUtils.isNullOrWhitespaceOnly(consistencyLevel)) {
            checkState(
                    consistencyLevel.equals("ONE") || consistencyLevel.equals("QUORUM")
                            || consistencyLevel.equals("ALL"),
                    "Consistency level must be one of [ONE, QUORUM, ALL], but got: "
                            + consistencyLevel);
        }

        // Validate timeout configurations
        Duration connectionTimeout = config.get(CONNECTION_TIMEOUT);
        checkState(!connectionTimeout.isNegative() && !connectionTimeout.isZero(),
                "Connection timeout must be positive, but got: " + connectionTimeout);
        checkState(connectionTimeout.toMillis() <= 300000,
                "Connection timeout too large (max 5 minutes), but got: " + connectionTimeout);

        Duration readTimeout = config.get(READ_TIMEOUT);
        checkState(!readTimeout.isNegative() && !readTimeout.isZero(),
                "Read timeout must be positive, but got: " + readTimeout);
        checkState(readTimeout.toMillis() <= 600000,
                "Read timeout too large (max 10 minutes), but got: " + readTimeout);

        // Validate flush interval for sink
        Duration flushInterval = config.get(FLUSH_INTERVAL);
        checkState(!flushInterval.isNegative() && !flushInterval.isZero(),
                "Flush interval must be positive, but got: " + flushInterval);
        checkState(flushInterval.toMillis() <= 300000,
                "Flush interval too large (max 5 minutes), but got: " + flushInterval);

        // Log configuration summary for debugging
        LOG.info("Weaviate connector configuration validated: url={}, className={}, batchSize={}, " +
                "queryLimit={}, maxRetries={}, vectorDimension={}",
                url, className, batchSize, queryLimit, maxRetries, vectorDimension);
    }

    /**
     * Builds the metric option for InLong audit.
     *
     * @param config the readable configuration
     * @return the metric option
     */
    private MetricOption buildMetricOption(ReadableConfig config) {
        String inlongMetric = config.getOptional(INLONG_METRIC).orElse(null);
        String auditHostAndPorts = config.getOptional(INLONG_AUDIT).orElse(null);
        String auditKeys = config.getOptional(AUDIT_KEYS).orElse(null);

        return MetricOption.builder()
                .withInlongLabels(inlongMetric)
                .withAuditAddress(auditHostAndPorts)
                .withAuditKeys(auditKeys)
                .build();
    }
}