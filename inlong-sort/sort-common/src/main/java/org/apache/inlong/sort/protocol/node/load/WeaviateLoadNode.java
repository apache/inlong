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

package org.apache.inlong.sort.protocol.node.load;

import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.InlongMetric;
import org.apache.inlong.sort.protocol.enums.FilterStrategy;
import org.apache.inlong.sort.protocol.node.LoadNode;
import org.apache.inlong.sort.protocol.transformation.FieldRelation;
import org.apache.inlong.sort.protocol.transformation.FilterFunction;

import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Weaviate load node can load data into Weaviate vector database with enhanced
 * error handling
 */
@EqualsAndHashCode(callSuper = true)
@JsonTypeName("weaviateLoad")
@Data
@NoArgsConstructor
@Slf4j
public class WeaviateLoadNode extends LoadNode implements InlongMetric, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Weaviate server URL (e.g., http://localhost:8080)
     */
    @JsonProperty("url")
    private String url;

    /**
     * API key for authentication (optional)
     */
    @Nullable
    @JsonProperty("apiKey")
    private String apiKey;

    /**
     * Weaviate class name to load data into
     */
    @JsonProperty("className")
    private String className;

    /**
     * Batch size for bulk operations
     */
    @JsonProperty("batchSize")
    private Integer batchSize;

    /**
     * Vector field name for storing embeddings
     */
    @Nullable
    @JsonProperty("vectorField")
    private String vectorField;

    /**
     * Flush interval in milliseconds
     */
    @JsonProperty("flushInterval")
    private Long flushInterval;

    /**
     * Connection timeout in milliseconds
     */
    @JsonProperty("timeout")
    private Long timeout;

    /**
     * Maximum retry attempts for failed operations
     */
    @JsonProperty("maxRetries")
    private Integer maxRetries;

    @JsonCreator
    public WeaviateLoadNode(
            @JsonProperty("id") String id,
            @JsonProperty("name") String name,
            @JsonProperty("fields") List<FieldInfo> fields,
            @JsonProperty("fieldRelations") List<FieldRelation> fieldRelations,
            @JsonProperty("filters") List<FilterFunction> filters,
            @JsonProperty("filterStrategy") FilterStrategy filterStrategy,
            @JsonProperty("sinkParallelism") Integer sinkParallelism,
            @JsonProperty("properties") Map<String, String> properties,
            @JsonProperty("url") String url,
            @JsonProperty("apiKey") String apiKey,
            @JsonProperty("className") String className,
            @JsonProperty("batchSize") Integer batchSize,
            @JsonProperty("vectorField") String vectorField,
            @JsonProperty("flushInterval") Long flushInterval,
            @JsonProperty("timeout") Long timeout,
            @JsonProperty("maxRetries") Integer maxRetries) {
        super(id, name, fields, fieldRelations, filters, filterStrategy, sinkParallelism, properties);

        // Enhanced validation with detailed error messages
        this.url = Preconditions.checkNotNull(url, "Weaviate URL cannot be null");
        Preconditions.checkArgument(url != null && !url.trim().isEmpty(), "Weaviate URL cannot be blank");

        // Validate URL format
        try {
            java.net.URL urlObj = new java.net.URL(url);
            String urlScheme = urlObj.getProtocol();
            if (!"http".equalsIgnoreCase(urlScheme) && !"https".equalsIgnoreCase(urlScheme)) {
                throw new IllegalArgumentException("Weaviate URL must use http or https protocol, got: " + urlScheme);
            }
        } catch (java.net.MalformedURLException e) {
            throw new IllegalArgumentException("Invalid Weaviate URL format: " + url, e);
        }

        this.className = Preconditions.checkNotNull(className, "Weaviate class name cannot be null");
        Preconditions.checkArgument(className != null && !className.trim().isEmpty(),
                "Weaviate class name cannot be blank");

        // Validate className format (basic validation)
        if (!className.matches("^[A-Z][a-zA-Z0-9_]*$")) {
            log.warn("Weaviate className '{}' may not follow naming conventions (should start with uppercase letter)",
                    className);
        }

        // Set optional parameters with defaults and validation
        this.apiKey = apiKey;
        this.batchSize = batchSize != null ? batchSize : 100;
        this.vectorField = vectorField;
        this.flushInterval = flushInterval != null ? flushInterval : 5000L;
        this.timeout = timeout != null ? timeout : 30000L;
        this.maxRetries = maxRetries != null ? maxRetries : 3;

        // Enhanced validation with specific error messages
        Preconditions.checkArgument(this.batchSize > 0,
                "Batch size must be positive, got: %s", this.batchSize);
        Preconditions.checkArgument(this.batchSize <= 10000,
                "Batch size too large (max 10000), got: %s", this.batchSize);

        Preconditions.checkArgument(this.flushInterval > 0,
                "Flush interval must be positive, got: %s", this.flushInterval);
        Preconditions.checkArgument(this.flushInterval <= 300000,
                "Flush interval too large (max 5 minutes), got: %s", this.flushInterval);

        Preconditions.checkArgument(this.timeout > 0,
                "Timeout must be positive, got: %s", this.timeout);
        Preconditions.checkArgument(this.timeout <= 600000,
                "Timeout too large (max 10 minutes), got: %s", this.timeout);

        Preconditions.checkArgument(this.maxRetries >= 0,
                "Max retries must be non-negative, got: %s", this.maxRetries);
        Preconditions.checkArgument(this.maxRetries <= 10,
                "Max retries too large (max 10), got: %s", this.maxRetries);

        // Log warnings for potentially problematic configurations
        if (this.batchSize > 1000) {
            log.warn("Large batch size {} may cause memory issues or timeouts", this.batchSize);
        }
        if (this.flushInterval < 1000) {
            log.warn("Small flush interval {}ms may cause high CPU usage", this.flushInterval);
        }
        if (this.maxRetries > 5) {
            log.warn("High retry count {} may cause long delays on failures", this.maxRetries);
        }

        // Log configuration summary for debugging
        log.info("WeaviateLoadNode configured: url={}, className={}, batchSize={}, flushInterval={}ms, " +
                "timeout={}ms, maxRetries={}, vectorField={}",
                this.url, this.className, this.batchSize, this.flushInterval,
                this.timeout, this.maxRetries, this.vectorField);
    }

    @Override
    public Map<String, String> tableOptions() {
        Map<String, String> options = super.tableOptions();
        options.put("connector", "weaviate-inlong");
        options.put("url", url);
        options.put("class-name", className);
        options.put("batch-size", String.valueOf(batchSize));
        options.put("flush-interval", String.valueOf(flushInterval));
        options.put("timeout", String.valueOf(timeout));
        options.put("max-retries", String.valueOf(maxRetries));

        if (apiKey != null) {
            options.put("api-key", apiKey);
        }
        if (vectorField != null) {
            options.put("vector-field", vectorField);
        }

        return options;
    }

    @Override
    public String genTableName() {
        return String.format("table_%s", super.getId());
    }
}