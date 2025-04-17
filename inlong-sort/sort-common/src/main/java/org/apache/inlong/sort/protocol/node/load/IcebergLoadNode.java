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

import org.apache.inlong.common.enums.MetaField;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.InlongMetric;
import org.apache.inlong.sort.protocol.Metadata;
import org.apache.inlong.sort.protocol.constant.IcebergConstant;
import org.apache.inlong.sort.protocol.constant.IcebergConstant.CatalogType;
import org.apache.inlong.sort.protocol.enums.FilterStrategy;
import org.apache.inlong.sort.protocol.enums.SchemaChangePolicy;
import org.apache.inlong.sort.protocol.enums.SchemaChangeType;
import org.apache.inlong.sort.protocol.node.LoadNode;
import org.apache.inlong.sort.protocol.node.format.Format;
import org.apache.inlong.sort.protocol.transformation.FieldRelation;
import org.apache.inlong.sort.protocol.transformation.FilterFunction;

import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.inlong.sort.protocol.constant.DorisConstant.SINK_MULTIPLE_DATABASE_PATTERN;
import static org.apache.inlong.sort.protocol.constant.DorisConstant.SINK_MULTIPLE_ENABLE;
import static org.apache.inlong.sort.protocol.constant.DorisConstant.SINK_MULTIPLE_FORMAT;
import static org.apache.inlong.sort.protocol.constant.DorisConstant.SINK_MULTIPLE_TABLE_PATTERN;

@JsonTypeName("icebergLoad")
@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class IcebergLoadNode extends LoadNode implements InlongMetric, Metadata, Serializable {

    private static final long serialVersionUID = -1L;

    @JsonProperty("tableName")
    @Nonnull
    private String tableName;

    @JsonProperty("dbName")
    @Nonnull
    private String dbName;

    @JsonProperty("primaryKey")
    private String primaryKey;

    @JsonProperty("catalogType")
    private IcebergConstant.CatalogType catalogType;

    @JsonProperty("uri")
    private String uri;

    @JsonProperty("warehouse")
    private String warehouse;

    @JsonProperty("appendMode")
    private String appendMode;

    @Nullable
    @JsonProperty("sinkMultipleEnable")
    private Boolean sinkMultipleEnable = false;

    @Nullable
    @JsonProperty("sinkMultipleFormat")
    private Format sinkMultipleFormat;

    @Nullable
    @JsonProperty("databasePattern")
    private String databasePattern;

    @Nullable
    @JsonProperty("tablePattern")
    private String tablePattern;

    @Nullable
    @JsonProperty("enableSchemaChange")
    private boolean enableSchemaChange;

    @Nullable
    @JsonProperty("policyMap")
    private Map<SchemaChangeType, SchemaChangePolicy> policyMap;

    public IcebergLoadNode(@JsonProperty("id") String id,
            @JsonProperty("name") String name,
            @JsonProperty("fields") List<FieldInfo> fields,
            @JsonProperty("fieldRelations") List<FieldRelation> fieldRelations,
            @JsonProperty("filters") List<FilterFunction> filters,
            @JsonProperty("filterStrategy") FilterStrategy filterStrategy,
            @Nullable @JsonProperty("sinkParallelism") Integer sinkParallelism,
            @JsonProperty("properties") Map<String, String> properties,
            @Nonnull @JsonProperty("dbName") String dbName,
            @Nonnull @JsonProperty("tableName") String tableName,
            @JsonProperty("primaryKey") String primaryKey,
            @JsonProperty("catalogType") IcebergConstant.CatalogType catalogType,
            @JsonProperty("uri") String uri,
            @JsonProperty("warehouse") String warehouse,
            @JsonProperty("appendMode") String appendMode) {
        this(id, name, fields, fieldRelations, filters, filterStrategy, sinkParallelism, properties, dbName, tableName,
                primaryKey, catalogType, uri, warehouse, appendMode, false, null,
                null, null, false, null);
    }

    @JsonCreator
    public IcebergLoadNode(@JsonProperty("id") String id,
            @JsonProperty("name") String name,
            @JsonProperty("fields") List<FieldInfo> fields,
            @JsonProperty("fieldRelations") List<FieldRelation> fieldRelations,
            @JsonProperty("filters") List<FilterFunction> filters,
            @JsonProperty("filterStrategy") FilterStrategy filterStrategy,
            @Nullable @JsonProperty("sinkParallelism") Integer sinkParallelism,
            @JsonProperty("properties") Map<String, String> properties,
            @Nonnull @JsonProperty("dbName") String dbName,
            @Nonnull @JsonProperty("tableName") String tableName,
            @JsonProperty("primaryKey") String primaryKey,
            @JsonProperty("catalogType") IcebergConstant.CatalogType catalogType,
            @JsonProperty("uri") String uri,
            @JsonProperty("warehouse") String warehouse,
            @JsonProperty("appendMode") String appendMode,
            @Nullable @JsonProperty(value = "sinkMultipleEnable", defaultValue = "false") Boolean sinkMultipleEnable,
            @Nullable @JsonProperty("sinkMultipleFormat") Format sinkMultipleFormat,
            @Nullable @JsonProperty("databasePattern") String databasePattern,
            @Nullable @JsonProperty("tablePattern") String tablePattern,
            @JsonProperty("enableSchemaChange") boolean enableSchemaChange,
            @Nullable @JsonProperty("policyMap") Map<SchemaChangeType, SchemaChangePolicy> policyMap) {
        super(id, name, fields, fieldRelations, filters, filterStrategy, sinkParallelism, properties);
        this.primaryKey = primaryKey;
        this.catalogType = catalogType == null ? CatalogType.HIVE : catalogType;
        this.uri = uri;
        this.warehouse = warehouse;
        this.appendMode = appendMode;
        this.sinkMultipleEnable = sinkMultipleEnable;
        if (sinkMultipleEnable == null || !sinkMultipleEnable) {
            this.tableName = Preconditions.checkNotNull(tableName, "table name is null");
            this.dbName = Preconditions.checkNotNull(dbName, "db name is null");
        } else {
            this.databasePattern = Preconditions.checkNotNull(databasePattern, "databasePattern is null");
            this.tablePattern = Preconditions.checkNotNull(tablePattern, "tablePattern is null");
            this.sinkMultipleFormat = Preconditions.checkNotNull(sinkMultipleFormat,
                    "sinkMultipleFormat is null");
        }
        this.enableSchemaChange = enableSchemaChange;
        this.policyMap = policyMap;
    }

    @Override
    public Map<String, String> tableOptions() {
        Map<String, String> options = super.tableOptions();
        options.put(IcebergConstant.CONNECTOR_KEY, IcebergConstant.CONNECTOR);
        options.put(IcebergConstant.DATABASE_KEY, dbName);
        options.put(IcebergConstant.TABLE_KEY, tableName);
        options.put(IcebergConstant.DEFAULT_DATABASE_KEY, dbName);
        options.put(IcebergConstant.CATALOG_TYPE_KEY, catalogType.name());
        options.put(IcebergConstant.CATALOG_NAME_KEY, catalogType.name());

        if ("upsert".equalsIgnoreCase(appendMode)) {
            options.put(IcebergConstant.UPSERT_ENABLED_KEY, Boolean.TRUE.toString());
        } else {
            options.put(IcebergConstant.UPSERT_ENABLED_KEY, Boolean.FALSE.toString());
        }

        if (null != uri) {
            options.put(IcebergConstant.URI_KEY, uri);
        }
        if (null != warehouse) {
            options.put(IcebergConstant.WAREHOUSE_KEY, warehouse);
        }

        if (sinkMultipleEnable != null && sinkMultipleEnable) {
            options.put(SINK_MULTIPLE_ENABLE, sinkMultipleEnable.toString());
            options.put(SINK_MULTIPLE_FORMAT, Objects.requireNonNull(sinkMultipleFormat).identifier());
            options.put(SINK_MULTIPLE_DATABASE_PATTERN, databasePattern);
            options.put(SINK_MULTIPLE_TABLE_PATTERN, tablePattern);
        } else {
            options.put(SINK_MULTIPLE_ENABLE, Boolean.FALSE.toString());
        }

        return options;
    }

    @Override
    public String genTableName() {
        return tableName;
    }

    @Override
    public String getPrimaryKey() {
        return primaryKey;
    }

    @Override
    public List<FieldInfo> getPartitionFields() {
        return super.getPartitionFields();
    }

    @Override
    public String getMetadataKey(MetaField metaField) {
        String metadataKey;
        switch (metaField) {
            case AUDIT_DATA_TIME:
                metadataKey = "audit_data_time";
                break;
            default:
                throw new UnsupportedOperationException(String.format("Unsupport meta field for %s: %s",
                        this.getClass().getSimpleName(), metaField));
        }
        return metadataKey;
    }

    @Override
    public boolean isVirtual(MetaField metaField) {
        return false;
    }

    @Override
    public Set<MetaField> supportedMetaFields() {
        return EnumSet.of(MetaField.AUDIT_DATA_TIME);
    }
}
