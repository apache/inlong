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

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude.Include;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.inlong.common.enums.MetaField;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.InlongMetric;
import org.apache.inlong.sort.protocol.Metadata;
import org.apache.inlong.sort.protocol.constant.TiDBConstant;
import org.apache.inlong.sort.protocol.enums.ExtractMode;
import org.apache.inlong.sort.protocol.node.ExtractNode;
import org.apache.inlong.sort.protocol.transformation.WatermarkField;

/**
 * Tidb extract node for extract data from tidb
 */
@EqualsAndHashCode(callSuper = true)
@JsonTypeName("tidbExtract")
@Data
public class TidbExtractNode extends ExtractNode implements Metadata, InlongMetric, Serializable {

    private static final long serialVersionUID = 1L;

    @JsonInclude(Include.NON_NULL)
    @JsonProperty("primaryKey")
    private String primaryKey;
    @JsonProperty("url")
    private String url;
    @JsonProperty("database")
    private String database;
    @JsonProperty("tableName")
    private String tableName;
    @JsonProperty("username")
    private String username;
    @JsonProperty("password")
    private String password;
    @JsonProperty("bootstrapServers")
    private String bootstrapServers;
    @JsonProperty("topic")
    private String topic;
    @JsonProperty("groupId")
    private String groupId;
    @JsonProperty("codec")
    private String codec;
    @JsonProperty("autoOffsetReset")
    private String autoOffsetReset;

    /**
     * Constructor
     *
     * @param id The id of node
     * @param name The name of node
     * @param fields The field list of node
     * @param watermarkField The watermark field of node only used for {@link ExtractMode#CDC}
     * @param properties The properties connect to tidb
     * @param primaryKey The primark key connect to tidb
     * @param tableName The table name connect to tidb
     * @param url The url connect to tidb
     * @param database The database connect to tidb
     * @param username The username  connect to tidb
     * @param password The password  connect to tidb
     * @param bootstrapServers The bootstrapServers connect to kafka
     * @param topic The topic connect to kafka
     * @param codec The codec is topic data of format
     * @param groupId The kafka topic groupId
     */
    @JsonCreator
    public TidbExtractNode(@Nonnull @JsonProperty("id") String id,
            @Nonnull @JsonProperty("name") String name,
            @Nonnull @JsonProperty("fields") List<FieldInfo> fields,
            @Nullable @JsonProperty("watermarkField") WatermarkField watermarkField,
            @JsonProperty("properties") Map<String, String> properties,
            @JsonProperty("primaryKey") String primaryKey,
            @Nullable @JsonProperty("url") String url,
            @Nonnull @JsonProperty("tableName") String tableName,
            @Nonnull @JsonProperty("database") String database,
            @Nonnull @JsonProperty("username") String username,
            @Nonnull @JsonProperty("password") String password,
            @Nonnull @JsonProperty("bootstrapServers") String bootstrapServers,
            @Nonnull @JsonProperty("topic") String topic,
            @Nonnull @JsonProperty("codec") String codec,
            @JsonProperty("groupId") String groupId,
            @JsonProperty("autoOffsetReset") String autoOffsetReset
    ) {
        super(id, name, fields, watermarkField, properties);
        this.url = Preconditions.checkNotNull(url, "url is null");
        this.tableName = Preconditions.checkNotNull(tableName, "tableName is null");
        this.database = Preconditions.checkNotNull(database, "database is null");
        this.username = Preconditions.checkNotNull(username, "username is null");
        this.password = Preconditions.checkNotNull(password, "password is null");
        this.bootstrapServers = Preconditions.checkNotNull(bootstrapServers, "bootstrapServers is null");
        this.topic = Preconditions.checkNotNull(topic, "topic is null");
        this.codec = Preconditions.checkNotNull(codec, "codec is null");
        this.groupId = groupId;
        this.primaryKey = primaryKey;
        this.autoOffsetReset = autoOffsetReset;
    }

    @Override
    public String genTableName() {
        return String.format("table_%s", super.getId());
    }

    @Override
    public String getPrimaryKey() {
        return primaryKey;
    }

    @Override
    public Map<String, String> tableOptions() {
        Map<String, String> options = super.tableOptions();

        options.put(TiDBConstant.CONNECTOR, TiDBConstant.CONNECTOR_NAME);
        options.put(TiDBConstant.URL, url);
        options.put(TiDBConstant.USERNAME, username);
        options.put(TiDBConstant.PASSWORD, password);
        options.put(TiDBConstant.DATABASE_NAME, database);
        options.put(TiDBConstant.TABLE_NAME, tableName);
        // current only support kafka streaming source
        options.put(TiDBConstant.STREAMING_SOURCE, TiDBConstant.STREAMING_SOURCE_KAFKA);
        options.put(TiDBConstant.STREAMING_CODEC, codec);
        options.put(TiDBConstant.BOOTSTRAP_SERVERS, bootstrapServers);
        options.put(TiDBConstant.TOPIC_NAME, topic);
        if (StringUtils.isNotBlank(groupId)) {
            options.put(TiDBConstant.GROUP_ID, groupId);
        }
        if (StringUtils.isNotBlank(autoOffsetReset)) {
            options.put(TiDBConstant.OFFSET_RESET, autoOffsetReset);
        }
        return options;
    }

    @Override
    public String getMetadataKey(MetaField metaField) {
        String metadataKey;
        switch (metaField) {
            case TABLE_NAME:
                metadataKey = "meta.table_name";
                break;
            case DATABASE_NAME:
                metadataKey = "meta.database_name";
                break;
            case OP_TS:
                metadataKey = "meta.op_ts";
                break;
            case OP_TYPE:
                metadataKey = "meta.op_type";
                break;
            case DATA:
                metadataKey = "meta.data";
                break;
            case IS_DDL:
                metadataKey = "meta.is_ddl";
                break;
            case TS:
                metadataKey = "meta.ts";
                break;
            case SQL_TYPE:
                metadataKey = "meta.sql_type";
                break;
            case PK_NAMES:
                metadataKey = "meta.pk_names";
                break;
            default:
                throw new UnsupportedOperationException(String.format("Unsupport meta field for %s: %s",
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
        return EnumSet.of(MetaField.PROCESS_TIME, MetaField.TABLE_NAME, MetaField.DATA, MetaField.DATABASE_NAME,
                MetaField.OP_TYPE, MetaField.OP_TS, MetaField.IS_DDL, MetaField.TS, MetaField.SQL_TYPE,
                MetaField.PK_NAMES, MetaField.BATCH_ID, MetaField.UPDATE_BEFORE);
    }
}
