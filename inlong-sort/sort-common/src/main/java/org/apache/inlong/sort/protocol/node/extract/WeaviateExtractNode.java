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

package org.apache.inlong.sort.protocol.node.extract;

import org.apache.inlong.common.enums.MetaField;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.InlongMetric;
import org.apache.inlong.sort.protocol.Metadata;
import org.apache.inlong.sort.protocol.node.ExtractNode;
import org.apache.inlong.sort.protocol.transformation.WatermarkField;

import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude.Include;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Weaviate extract node for reading data from Weaviate vector database with
 * enhanced error handling
 */
@EqualsAndHashCode(callSuper = true)
@JsonTypeName("weaviateExtract")
@JsonInclude(Include.NON_NULL)
@Data
@Slf4j
public class WeaviateExtractNode extends ExtractNode implements InlongMetric, Metadata, Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("url")
    @Nonnull
    private String url;

    @JsonProperty("apiKey")
    @Nullable
    private String apiKey;

    @JsonProperty("className")
    @Nonnull
    private String className;

    @JsonProperty("queryConditions")
    @Nullable
    private String queryConditions;

    @JsonProperty("batchSize")
    @Nullable
    private Integer batchSize;

    @JsonProperty("timeout")
    @Nullable
    private Integer timeout;

    @JsonProperty("vectorField")
    @Nullable
    private String vectorField;

    @JsonProperty("scheme")
    @Nullable
    private String scheme;

    @JsonProperty("host")
    @Nullable
    private String host;

    @JsonProperty("port")
    @Nullable
    private Integer port;

    @JsonProperty("headers")
    @Nullable
    private Map<String, String> headers;

    @JsonProperty("username")
    @Nullable
    private String username;

    @JsonProperty("password")
    @Nullable
    private String password;

    @JsonProperty("tenant")
    @Nullable
    private String tenant;

    @JsonProperty("consistency")
    @Nullable
    private String consistency;

    @JsonProperty("limit")
    @Nullable
    private Integer limit;

    @JsonProperty("offset")
    @Nullable
    private Integer offset;

    @JsonProperty("where")
    @Nullable
    private String where;

    @JsonProperty("near")
    @Nullable
    private String near;

    @JsonProperty("fieldsToSelect")
    @Nullable
    private String fieldsToSelect;

    @JsonProperty("groupBy")
    @Nullable
    private String groupBy;

    @JsonProperty("sort")
    @Nullable
    private String sort;

    @JsonProperty("maxConnections")
    @Nullable
    private Integer maxConnections;

    @JsonProperty("connectionTimeout")
    @Nullable
    private Integer connectionTimeout;

    @JsonProperty("readTimeout")
    @Nullable
    private Integer readTimeout;

    @JsonProperty("retryAttempts")
    @Nullable
    private Integer retryAttempts;

    @JsonProperty("retryInterval")
    @Nullable
    private Integer retryInterval;

    @JsonProperty("enableCache")
    @Nullable
    private Boolean enableCache;

    @JsonProperty("cacheSize")
    @Nullable
    private Integer cacheSize;

    @JsonProperty("cacheTtl")
    @Nullable
    private Integer cacheTtl;

    @JsonCreator
    public WeaviateExtractNode(@JsonProperty("id") String id,
            @JsonProperty("name") String name,
            @JsonProperty("fields") List<FieldInfo> fields,
            @Nullable @JsonProperty("watermarkField") WatermarkField watermarkField,
            @Nullable @JsonProperty("properties") Map<String, String> properties,
            @JsonProperty("url") @Nonnull String url,
            @JsonProperty("apiKey") @Nullable String apiKey,
            @JsonProperty("className") @Nonnull String className,
            @JsonProperty("queryConditions") @Nullable String queryConditions,
            @JsonProperty("batchSize") @Nullable Integer batchSize,
            @JsonProperty("timeout") @Nullable Integer timeout,
            @JsonProperty("vectorField") @Nullable String vectorField,
            @JsonProperty("scheme") @Nullable String scheme,
            @JsonProperty("host") @Nullable String host,
            @JsonProperty("port") @Nullable Integer port,
            @JsonProperty("headers") @Nullable Map<String, String> headers,
            @JsonProperty("username") @Nullable String username,
            @JsonProperty("password") @Nullable String password,
            @JsonProperty("tenant") @Nullable String tenant,
            @JsonProperty("consistency") @Nullable String consistency,
            @JsonProperty("limit") @Nullable Integer limit,
            @JsonProperty("offset") @Nullable Integer offset,
            @JsonProperty("where") @Nullable String where,
            @JsonProperty("near") @Nullable String near,
            @JsonProperty("fieldsToSelect") @Nullable String fieldsToSelect,
            @JsonProperty("groupBy") @Nullable String groupBy,
            @JsonProperty("sort") @Nullable String sort,
            @JsonProperty("maxConnections") @Nullable Integer maxConnections,
            @JsonProperty("connectionTimeout") @Nullable Integer connectionTimeout,
            @JsonProperty("readTimeout") @Nullable Integer readTimeout,
            @JsonProperty("retryAttempts") @Nullable Integer retryAttempts,
            @JsonProperty("retryInterval") @Nullable Integer retryInterval,
            @JsonProperty("enableCache") @Nullable Boolean enableCache,
            @JsonProperty("cacheSize") @Nullable Integer cacheSize,
            @JsonProperty("cacheTtl") @Nullable Integer cacheTtl) {
        super(id, name, fields, watermarkField, properties);

        // Enhanced validation with detailed error messages
        this.url = Preconditions.checkNotNull(url, "Weaviate URL cannot be null");
        Preconditions.checkArgument(StringUtils.isNotBlank(url), "Weaviate URL cannot be blank");

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

        this.apiKey = apiKey;
        this.className = Preconditions.checkNotNull(className, "Weaviate className cannot be null");
        Preconditions.checkArgument(StringUtils.isNotBlank(className), "Weaviate className cannot be blank");

        // Validate className format (basic validation)
        if (!className.matches("^[A-Z][a-zA-Z0-9_]*$")) {
            log.warn("Weaviate className '{}' may not follow naming conventions (should start with uppercase letter)",
                    className);
        }

        this.queryConditions = queryConditions;
        this.batchSize = batchSize != null ? batchSize : 100;

        // Validate batch size
        if (this.batchSize <= 0) {
            throw new IllegalArgumentException("Batch size must be positive, got: " + this.batchSize);
        }
        if (this.batchSize > 10000) {
            log.warn("Large batch size {} may cause performance issues", this.batchSize);
        }

        this.timeout = timeout != null ? timeout : 30000;

        // Validate timeout
        if (this.timeout <= 0) {
            throw new IllegalArgumentException("Timeout must be positive, got: " + this.timeout);
        }

        this.vectorField = vectorField;

        // Connection configuration
        this.scheme = scheme;
        this.host = host;
        this.port = port;
        this.headers = headers;

        // Authentication configuration
        this.username = username;
        this.password = password;
        this.tenant = tenant;

        // Query configuration
        this.consistency = consistency;
        this.limit = limit;
        this.offset = offset;
        this.where = where;
        this.near = near;
        this.fieldsToSelect = fieldsToSelect;
        this.groupBy = groupBy;
        this.sort = sort;

        // Connection pool and performance configuration with validation
        this.maxConnections = maxConnections != null ? maxConnections : 10;
        if (this.maxConnections <= 0) {
            throw new IllegalArgumentException("Max connections must be positive, got: " + this.maxConnections);
        }

        this.connectionTimeout = connectionTimeout != null ? connectionTimeout : 10000;
        if (this.connectionTimeout <= 0) {
            throw new IllegalArgumentException("Connection timeout must be positive, got: " + this.connectionTimeout);
        }

        this.readTimeout = readTimeout != null ? readTimeout : 30000;
        if (this.readTimeout <= 0) {
            throw new IllegalArgumentException("Read timeout must be positive, got: " + this.readTimeout);
        }

        this.retryAttempts = retryAttempts != null ? retryAttempts : 3;
        if (this.retryAttempts < 0) {
            throw new IllegalArgumentException("Retry attempts cannot be negative, got: " + this.retryAttempts);
        }
        if (this.retryAttempts > 10) {
            log.warn("High retry attempts {} may cause long delays on failures", this.retryAttempts);
        }

        this.retryInterval = retryInterval != null ? retryInterval : 1000;
        if (this.retryInterval < 0) {
            throw new IllegalArgumentException("Retry interval cannot be negative, got: " + this.retryInterval);
        }

        // Cache configuration with validation
        this.enableCache = enableCache != null ? enableCache : false;
        this.cacheSize = cacheSize != null ? cacheSize : 1000;
        if (this.cacheSize <= 0) {
            throw new IllegalArgumentException("Cache size must be positive, got: " + this.cacheSize);
        }

        this.cacheTtl = cacheTtl != null ? cacheTtl : 300;
        if (this.cacheTtl <= 0) {
            throw new IllegalArgumentException("Cache TTL must be positive, got: " + this.cacheTtl);
        }

        // Log configuration summary for debugging
        log.info("WeaviateExtractNode configured: url={}, className={}, batchSize={}, timeout={}ms, " +
                "maxConnections={}, retryAttempts={}, enableCache={}",
                this.url, this.className, this.batchSize, this.timeout,
                this.maxConnections, this.retryAttempts, this.enableCache);
    }

    @Override
    public String genTableName() {
        return String.format("table_%s", super.getId());
    }

    @Override
    public Map<String, String> tableOptions() {
        Map<String, String> options = super.tableOptions();
        options.put("connector", "weaviate-inlong");
        options.put("url", url);
        options.put("class-name", className);

        // Authentication configuration
        if (StringUtils.isNotBlank(apiKey)) {
            options.put("api-key", apiKey);
        }
        if (StringUtils.isNotBlank(username)) {
            options.put("username", username);
        }
        if (StringUtils.isNotBlank(password)) {
            options.put("password", password);
        }
        if (StringUtils.isNotBlank(tenant)) {
            options.put("tenant", tenant);
        }

        // Connection configuration
        if (StringUtils.isNotBlank(scheme)) {
            options.put("scheme", scheme);
        }
        if (StringUtils.isNotBlank(host)) {
            options.put("host", host);
        }
        if (port != null) {
            options.put("port", String.valueOf(port));
        }
        if (headers != null && !headers.isEmpty()) {
            // Convert headers to string format, e.g.: "key1:value1,key2:value2"
            StringBuilder headerStr = new StringBuilder();
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                if (entry.getKey() != null && entry.getValue() != null) {
                    if (headerStr.length() > 0) {
                        headerStr.append(",");
                    }
                    headerStr.append(entry.getKey()).append(":").append(entry.getValue());
                }
            }
            if (headerStr.length() > 0) {
                options.put("headers", headerStr.toString());
            }
        }

        // Query configuration
        if (StringUtils.isNotBlank(queryConditions)) {
            options.put("query-conditions", queryConditions);
        }
        if (StringUtils.isNotBlank(consistency)) {
            options.put("consistency", consistency);
        }
        if (limit != null) {
            options.put("limit", String.valueOf(limit));
        }
        if (offset != null) {
            options.put("offset", String.valueOf(offset));
        }
        if (StringUtils.isNotBlank(where)) {
            options.put("where", where);
        }
        if (StringUtils.isNotBlank(near)) {
            options.put("near", near);
        }
        if (StringUtils.isNotBlank(fieldsToSelect)) {
            options.put("fields", fieldsToSelect);
        }
        if (StringUtils.isNotBlank(groupBy)) {
            options.put("group-by", groupBy);
        }
        if (StringUtils.isNotBlank(sort)) {
            options.put("sort", sort);
        }

        // Performance configuration
        if (batchSize != null) {
            options.put("batch-size", String.valueOf(batchSize));
        }
        if (timeout != null) {
            options.put("timeout", String.valueOf(timeout));
        }
        if (StringUtils.isNotBlank(vectorField)) {
            options.put("vector-field", vectorField);
        }

        // Connection pool configuration
        if (maxConnections != null) {
            options.put("max-connections", String.valueOf(maxConnections));
        }
        if (connectionTimeout != null) {
            options.put("connection-timeout", String.valueOf(connectionTimeout));
        }
        if (readTimeout != null) {
            options.put("read-timeout", String.valueOf(readTimeout));
        }
        if (retryAttempts != null) {
            options.put("retry-attempts", String.valueOf(retryAttempts));
        }
        if (retryInterval != null) {
            options.put("retry-interval", String.valueOf(retryInterval));
        }

        // Cache configuration
        if (enableCache != null) {
            options.put("enable-cache", String.valueOf(enableCache));
        }
        if (cacheSize != null) {
            options.put("cache-size", String.valueOf(cacheSize));
        }
        if (cacheTtl != null) {
            options.put("cache-ttl", String.valueOf(cacheTtl));
        }

        return options;
    }

    @Override
    public String getMetadataKey(MetaField metaField) {
        String metadataKey;
        switch (metaField) {
            case TABLE_NAME:
                metadataKey = "table_name";
                break;
            case DATABASE_NAME:
                metadataKey = "database_name";
                break;
            case OP_TS:
                metadataKey = "op_ts";
                break;
            case PROCESS_TIME:
                metadataKey = "proc_time";
                break;
            default:
                throw new UnsupportedOperationException(String.format("Unsupported meta field for %s: %s",
                        this.getClass().getSimpleName(), metaField));
        }
        return metadataKey;
    }

    @Override
    public boolean isVirtual(MetaField metaField) {
        return true;
    }

    @Override
    public Set<MetaField> supportedMetaFields() {
        return EnumSet.of(MetaField.PROCESS_TIME, MetaField.TABLE_NAME,
                MetaField.DATABASE_NAME, MetaField.OP_TS);
    }
}