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

package org.apache.inlong.sort.protocol.sink;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude.Include;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.inlong.sort.configuration.Constants.CompressionType;
import org.apache.inlong.sort.protocol.FieldInfo;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;

public class HiveSinkInfo extends SinkInfo {

    private static final long serialVersionUID = -333999329240274826L;

    @JsonProperty("hive_server_jdbc_url")
    private final String hiveServerJdbcUrl;

    @JsonProperty("database")
    private final String databaseName;

    @JsonProperty("table")
    private final String tableName;

    @JsonInclude(Include.NON_NULL)
    @JsonProperty("username")
    private final String username;

    @JsonInclude(Include.NON_NULL)
    @JsonProperty("password")
    private final String password;

    @JsonProperty("data_path")
    private final String dataPath;

    @JsonProperty("partitions")
    private final HivePartitionInfo[] partitions;

    @JsonProperty("file_format")
    private final HiveFileFormat hiveFileFormat;

    public HiveSinkInfo(
            @JsonProperty("fields") FieldInfo[] fields,
            @JsonProperty("hive_server_jdbc_url") String hiveServerJdbcUrl,
            @JsonProperty("database") String databaseName,
            @JsonProperty("table") String tableName,
            @JsonProperty("username") @Nullable String username,
            @JsonProperty("password") @Nullable String password,
            @JsonProperty("data_path") String dataPath,
            @JsonProperty("partitions") HivePartitionInfo[] partitions,
            @JsonProperty("file_format") HiveFileFormat hiveFileFormat) {
        super(fields);
        this.hiveServerJdbcUrl = checkNotNull(hiveServerJdbcUrl);
        this.databaseName = checkNotNull(databaseName);
        this.tableName = checkNotNull(tableName);
        this.username = username;
        this.password = password;
        this.dataPath = checkNotNull(dataPath);
        this.partitions = checkNotNull(partitions);
        this.hiveFileFormat = checkNotNull(hiveFileFormat);
    }

    @JsonProperty("hive_server_jdbc_url")
    public String getHiveServerJdbcUrl() {
        return hiveServerJdbcUrl;
    }

    @JsonProperty("database")
    public String getDatabaseName() {
        return databaseName;
    }

    @JsonProperty("table")
    public String getTableName() {
        return tableName;
    }

    @Nullable
    @JsonProperty("username")
    public String getUsername() {
        return username;
    }

    @Nullable
    @JsonProperty("password")
    public String getPassword() {
        return password;
    }

    @JsonProperty("data_path")
    public String getDataPath() {
        return dataPath;
    }

    @JsonProperty("partitions")
    public HivePartitionInfo[] getPartitions() {
        return partitions;
    }

    @JsonProperty("file_format")
    public HiveFileFormat getHiveFileFormat() {
        return hiveFileFormat;
    }

    /**
     * HivePartitionInfo.
     */
    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            property = "type")
    @JsonSubTypes({
            @Type(value = HiveTimePartitionInfo.class, name = "time"),
            @Type(value = HiveFieldPartitionInfo.class, name = "field")})
    public abstract static class HivePartitionInfo {
        @JsonProperty("field_name")
        private final String fieldName;

        public HivePartitionInfo(
                @JsonProperty("field_name") String fieldName) {
            this.fieldName = fieldName;
        }

        @JsonProperty("field_name")
        public String getFieldName() {
            return fieldName;
        }
    }

    public static class HiveTimePartitionInfo extends HivePartitionInfo {

        @JsonProperty("date_format")
        private final String format;

        public HiveTimePartitionInfo(
                @JsonProperty("field_name") String fieldName,
                @JsonProperty("date_format") String format) {
            super(fieldName);
            this.format = format;
        }

        @JsonProperty("date_format")
        public String getFormat() {
            return format;
        }
    }

    public static class HiveFieldPartitionInfo extends HivePartitionInfo {

        public HiveFieldPartitionInfo(
                @JsonProperty("field_name") String fieldName) {
            super(fieldName);
        }
    }

    /**
     * HiveFileFormat.
     */
    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            property = "type")
    @JsonSubTypes({
            @Type(value = TextFileFormat.class, name = "text"),
            @Type(value = OrcFileFormat.class, name = "orc"),
            @Type(value = SequenceFileFormat.class, name = "sequence"),
            @Type(value = ParquetFileFormat.class, name = "parquet"),})
    public interface HiveFileFormat {
    }

    public static class TextFileFormat implements HiveFileFormat {

        @JsonProperty("splitter")
        private final Character splitter;

        @JsonInclude(Include.NON_NULL)
        @JsonProperty("compression_type")
        private final CompressionType compressionType;

        @JsonCreator
        public TextFileFormat(
                @JsonProperty("splitter") Character splitter,
                @JsonProperty("compression_type") CompressionType compressionType) {
            this.splitter = splitter;
            this.compressionType = compressionType;
        }

        public TextFileFormat(@JsonProperty("splitter") Character splitter) {
            this(splitter, CompressionType.NONE);
        }

        @JsonProperty("splitter")
        public Character getSplitter() {
            return splitter;
        }

        @JsonProperty("compression_type")
        public CompressionType getCompressionType() {
            return compressionType;
        }
    }

    public static class OrcFileFormat implements HiveFileFormat {

        @JsonProperty("batch_size")
        private final int batchSize;

        public OrcFileFormat(
                @JsonProperty("batch_size") int batchSize) {
            this.batchSize = batchSize;
        }

        @JsonProperty("batch_size")
        public int getBatchSize() {
            return batchSize;
        }
    }

    public static class SequenceFileFormat implements HiveFileFormat {

        @JsonProperty("splitter")
        private final Character splitter;

        @JsonProperty("buffer_size")
        private final int bufferSize;

        public SequenceFileFormat(
                @JsonProperty("splitter") Character splitter,
                @JsonProperty("buffer_size") int bufferSize) {
            this.splitter = splitter;
            this.bufferSize = bufferSize;
        }

        @JsonProperty("splitter")
        public Character getSplitter() {
            return splitter;
        }

        @JsonProperty("buffer_size")
        public int getBufferSize() {
            return bufferSize;
        }
    }

    public static class ParquetFileFormat implements HiveFileFormat {

        public ParquetFileFormat() {
        }
    }
}
