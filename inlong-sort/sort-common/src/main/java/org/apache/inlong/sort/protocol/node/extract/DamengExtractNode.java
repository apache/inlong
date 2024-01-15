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
import org.apache.inlong.sort.protocol.constant.DMConstant.ScanStartUpMode;
import org.apache.inlong.sort.protocol.node.ExtractNode;
import org.apache.inlong.sort.protocol.transformation.WatermarkField;

import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static org.apache.inlong.sort.protocol.constant.DMConstant.CONNECTOR;
import static org.apache.inlong.sort.protocol.constant.DMConstant.DATABASE_NAME;
import static org.apache.inlong.sort.protocol.constant.DMConstant.HOSTNAME;
import static org.apache.inlong.sort.protocol.constant.DMConstant.PASSWORD;
import static org.apache.inlong.sort.protocol.constant.DMConstant.PORT;
import static org.apache.inlong.sort.protocol.constant.DMConstant.SCAN_STARTUP_MODE;
import static org.apache.inlong.sort.protocol.constant.DMConstant.SCHEMA_NAME;
import static org.apache.inlong.sort.protocol.constant.DMConstant.TABLE_NAME;
import static org.apache.inlong.sort.protocol.constant.DMConstant.USERNAME;

@EqualsAndHashCode(callSuper = true)
@JsonTypeName("damengExtract")
@Data
public class DamengExtractNode extends ExtractNode implements Serializable {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    @JsonProperty("primaryKey")
    private String primaryKey;
    @Nonnull
    @JsonProperty("hostname")
    private String hostname;
    @Nonnull
    @JsonProperty("username")
    private String username;
    @Nonnull
    @JsonProperty("password")
    private String password;
    @Nonnull
    @JsonProperty("database")
    private String database;
    @Nonnull
    @JsonProperty("schemaName")
    private String schemaName;
    @Nonnull
    @JsonProperty("tableName")
    private String tableName;
    @Nonnull
    @JsonProperty("scanStartupMode")
    private ScanStartUpMode scanStartupMode;
    @Nullable
    @JsonProperty(value = "port", defaultValue = "5132")
    private Integer port;

    @JsonCreator
    public DamengExtractNode(@JsonProperty("id") String id,
            @JsonProperty("name") String name,
            @JsonProperty("fields") List<FieldInfo> fields,
            @Nullable @JsonProperty("watermark_field") WatermarkField watermarkField,
            @JsonProperty("properties") Map<String, String> properties,
            @Nullable @JsonProperty("primaryKey") String primaryKey,
            @JsonProperty("hostname") String hostname,
            @JsonProperty("username") String username,
            @JsonProperty("password") String password,
            @JsonProperty("database") String database,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty(value = "port", defaultValue = "1521") Integer port,
            @Nullable @JsonProperty("scanStartupMode") ScanStartUpMode scanStartupMode) {
        super(id, name, fields, watermarkField, properties);
        this.primaryKey = primaryKey;
        this.hostname = Preconditions.checkNotNull(hostname, "hostname is null");
        this.username = Preconditions.checkNotNull(username, "username is null");
        this.password = Preconditions.checkNotNull(password, "password is null");
        this.database = Preconditions.checkNotNull(database, "database is null");
        this.schemaName = Preconditions.checkNotNull(schemaName, "schemaName is null");
        this.tableName = Preconditions.checkNotNull(tableName, "tableName is null");
        this.port = port;
        this.scanStartupMode = scanStartupMode;
    }

    @Override
    public Map<String, String> tableOptions() {
        Map<String, String> options = super.tableOptions();
        // using oracle constants is ok, since dameng is essentially built upon oracle.
        options.put(CONNECTOR, "dm-cdc");
        options.put(HOSTNAME, hostname);
        options.put(USERNAME, username);
        options.put(PASSWORD, password);
        options.put(DATABASE_NAME, database);
        options.put(SCHEMA_NAME, schemaName);
        options.put(TABLE_NAME, tableName);
        options.put(SCAN_STARTUP_MODE, scanStartupMode.getValue());
        if (port != null) {
            options.put(PORT, port.toString());
        }
        return options;
    }

    @Override
    public String genTableName() {
        return String.format("table_%s", super.getId());
    }

}
