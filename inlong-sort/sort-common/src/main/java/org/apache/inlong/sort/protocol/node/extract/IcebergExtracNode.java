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

import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.constant.IcebergConstant;
import org.apache.inlong.sort.protocol.node.ExtractNode;
import org.apache.inlong.sort.protocol.transformation.WatermarkField;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Iceberg extract node for extract data from iceberg
 */
@EqualsAndHashCode(callSuper = true)
@JsonTypeName("icebergExtract")
@JsonInclude(JsonInclude.Include.NON_NULL)
@Data
public class IcebergExtracNode extends ExtractNode implements Serializable {

    @JsonProperty("tableName")
    @Nonnull
    private String tableName;

    @JsonProperty("dbName")
    @Nonnull
    private String dbName;

    @JsonProperty("catalogType")
    private IcebergConstant.CatalogType catalogType;

    @Nullable
    @JsonProperty("uri")
    private String uri;

    @JsonProperty("warehouse")
    private String warehouse;

    @JsonProperty("catalogName")
    private String catalogName;

    @JsonProperty("primaryKey")
    private String primaryKey;

    @JsonProperty("startSnapShotId")
    @Nullable
    private Long startSnapShotId;

    public IcebergExtracNode(
            @Nonnull @JsonProperty("id") String id,
            @Nonnull @JsonProperty("name") String name,
            @Nonnull @JsonProperty("fields") List<FieldInfo> fields,
            @Nullable @JsonProperty("watermarkField") WatermarkField watermarkField,
            @Nullable @JsonProperty("uri") String uri,
            @Nullable @JsonProperty("warehouse") String warehouse,
            @Nonnull @JsonProperty("dbName") String dbName,
            @Nonnull @JsonProperty("tableName") String tableName,
            @JsonProperty("catalogType") IcebergConstant.CatalogType catalogType,
            @Nullable @JsonProperty("catalogName") String catalogName,
            @JsonProperty("primaryKey") String primaryKey,
            @Nullable @JsonProperty("startSnapShotId") Long startSnapShotId,
            @Nullable @JsonProperty("properties") Map<String, String> properties) {
        super(id, name, fields, watermarkField, properties);
        this.uri = uri;
        this.warehouse = warehouse;
        this.dbName = dbName;
        this.tableName = tableName;
        this.catalogName = catalogName == null ? IcebergConstant.DEFAULT_CATALOG_NAME : catalogName;
        this.primaryKey = primaryKey;
        this.startSnapShotId = startSnapShotId;
        this.catalogType = catalogType == null ? IcebergConstant.CatalogType.HIVE : catalogType;
    }

    @Override
    public String genTableName() {
        return String.format("iceberg_table_%s", getId());
    }

    @Override
    public Map<String, String> tableOptions() {
        Map<String, String> options = super.tableOptions();
        options.put(IcebergConstant.CONNECTOR_KEY, IcebergConstant.CONNECTOR);
        options.put(IcebergConstant.DATABASE_KEY, dbName);
        options.put(IcebergConstant.TABLE_KEY, tableName);
        options.put(IcebergConstant.CATALOG_TYPE_KEY, catalogType.name());
        options.put(IcebergConstant.CATALOG_NAME_KEY, catalogName);
        if (null != uri) {
            options.put(IcebergConstant.URI_KEY, uri);
        }
        if (null != warehouse) {
            options.put(IcebergConstant.WAREHOUSE_KEY, warehouse);
        }
        if (null != startSnapShotId) {
            options.put(IcebergConstant.START_SNAPSHOT_ID, startSnapShotId.toString());
        }
        return options;
    }

    @Override
    public String getPrimaryKey() {
        return primaryKey;
    }

    @Override
    public List<FieldInfo> getPartitionFields() {
        return super.getPartitionFields();
    }

}
