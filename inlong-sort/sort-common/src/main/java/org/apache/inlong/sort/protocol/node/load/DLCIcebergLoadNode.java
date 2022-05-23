/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.inlong.sort.protocol.node.load;

import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.constant.IcebergConstant.CatalogType;
import org.apache.inlong.sort.protocol.enums.FilterStrategy;
import org.apache.inlong.sort.protocol.node.LoadNode;
import org.apache.inlong.sort.protocol.transformation.FieldRelationShip;
import org.apache.inlong.sort.protocol.transformation.FilterFunction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

@JsonTypeName("dlcIcebergLoad")
@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class DLCIcebergLoadNode extends LoadNode implements Serializable {

    private static final long serialVersionUID = -1L;

    @JsonProperty("tableName")
    @Nonnull
    private String tableName;

    @JsonProperty("dbName")
    @Nonnull
    private String dbName;

    @JsonProperty("primaryKey")
    private String primaryKey;

    @JsonProperty("uri")
    private String uri;

    @JsonProperty("warehouse")
    private String warehouse;

    @JsonCreator
    public DLCIcebergLoadNode(@JsonProperty("id") String id,
            @JsonProperty("name") String name,
            @JsonProperty("fields") List<FieldInfo> fields,
            @JsonProperty("fieldRelationShips") List<FieldRelationShip> fieldRelationShips,
            @JsonProperty("filters") List<FilterFunction> filters,
            @JsonProperty("filterStrategy") FilterStrategy filterStrategy,
            @Nullable @JsonProperty("sinkParallelism") Integer sinkParallelism,
            @JsonProperty("properties") Map<String, String> properties,
            @Nonnull @JsonProperty("dbName") String dbName,
            @Nonnull @JsonProperty("tableName") String tableName,
            @JsonProperty("primaryKey") String primaryKey,
            @JsonProperty("uri") String uri,
            @JsonProperty("warehouse") String warehouse) {
        super(id, name, fields, fieldRelationShips, filters, filterStrategy, sinkParallelism, properties);
        this.tableName = Preconditions.checkNotNull(tableName, "table name is null");
        this.dbName = Preconditions.checkNotNull(dbName, "db name is null");
        this.primaryKey = primaryKey;
        this.uri = uri;
        this.warehouse = warehouse;
        validateAuth(properties);
    }

    @Override
    public Map<String, String> tableOptions() {
        Map<String, String> options = super.tableOptions();
        options.put("connector", "iceberg");
        options.put("catalog-database", dbName);
        options.put("catalog-table", tableName);
        options.put("default-database", dbName);
        options.put("catalog-name", CatalogType.HYBRIS.name());
        options.put("catalog-impl", "org.apache.inlong.sort.iceberg.catalog.hybris.DlcWrappedHybrisCatalog");
        if (null != uri) {
            options.put("uri", uri);
        }
        if (null != warehouse) {
            options.put("warehouse", warehouse);
        }
        options.put("fs.lakefs.impl", "org.apache.hadoop.fs.CosFileSystem");
        options.put("fs.cosn.impl", "org.apache.hadoop.fs.CosFileSystem");
        options.put("fs.cosn.credentials.provider", "org.apache.hadoop.fs.auth.SimpleCredentialProvider");
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

    private void validateAuth(Map<String, String> properties) {
        Preconditions.checkNotNull(properties);
        Preconditions.checkNotNull(properties.get("qcloud.dlc.secret-id"), "dlc secret-id is null");
        Preconditions.checkNotNull(properties.get("qcloud.dlc.secret-key"), "dlc secret-key is null");
        Preconditions.checkNotNull(properties.get("qcloud.dlc.region"), "dlc region is null");

        Preconditions.checkNotNull(properties.get("fs.cosn.userinfo.region"), "cos region is null");
        Preconditions.checkNotNull(properties.get("fs.cosn.userinfo.secretId"), "cos secret-id is null");
        Preconditions.checkNotNull(properties.get("fs.cosn.userinfo.secretKey"), "cos secret-key is null");
    }
}
