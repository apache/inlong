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

package org.apache.inlong.sort.protocol;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.inlong.sort.formats.common.FormatInfo;

/**
 * built-in field info.
 */
public class BuiltInFieldInfo extends FieldInfo {

    private static final long serialVersionUID = -3436204467879205139L;

    @JsonProperty("builtinField")
    private final BuiltInField builtInField;

    @JsonCreator
    public BuiltInFieldInfo(
            @JsonProperty("name") String name,
            @JsonProperty("nodeId") String nodeId,
            @JsonProperty("formatInfo") FormatInfo formatInfo,
            @JsonProperty("builtinField") BuiltInField builtInField) {
        super(name, nodeId, formatInfo);
        this.builtInField = builtInField;
    }

    public BuiltInFieldInfo(
            @JsonProperty("name") String name,
            @JsonProperty("formatInfo") FormatInfo formatInfo,
            @JsonProperty("builtinField") BuiltInField builtInField) {
        super(name, formatInfo);
        this.builtInField = builtInField;
    }

    @JsonProperty("builtinField")
    public BuiltInField getBuiltInField() {
        return builtInField;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        BuiltInFieldInfo that = (BuiltInFieldInfo) o;
        return builtInField == that.builtInField
                && super.equals(that);
    }

    public enum BuiltInField {
        /**
         * The event time of flink
         */
        @Deprecated
        DATA_TIME,
        /**
         * The process time of flink
         */
        PROCESS_TIME,
        /**
         * The name of the database containing this Row
         * It is deprecated and can be replaced by ${@link BuiltInField#DATABASE_NAME}
         * and will be removed in a future version.
         */
        @Deprecated
        MYSQL_METADATA_DATABASE,
        /**
         * The name of the table containing this Row
         * It is deprecated and can be replaced by ${@link BuiltInField#TABLE_NAME}
         * and will be removed in a future version.
         */
        @Deprecated
        MYSQL_METADATA_TABLE,
        /**
         * The time when the Row made changes in the database.
         * It is deprecated and can be replaced by ${@link BuiltInField#OP_TS}
         * and will be removed in a future version.
         */
        @Deprecated
        MYSQL_METADATA_EVENT_TIME,
        /**
         * Name of the schema that contain the row.
         */
        SCHEMA_NAME,
        /**
         * Name of the database that contain the row.
         */
        DATABASE_NAME,
        /**
         * Name of the table that contain the row.
         */
        TABLE_NAME,
        /**
         * It indicates the time that the change was made in the database.
         * If the record is read from snapshot of the table instead of the change stream, the value is always 0
         */
        OP_TS,
        /**
         * Whether the DDL statement
         */
        IS_DDL,
        /**
         * Whether the DDL statement
         * It is deprecated and can be replaced by ${@link BuiltInField#IS_DDL}
         * and will be removed in a future version.
         */
        @Deprecated
        MYSQL_METADATA_IS_DDL,
        /**
         * Type of database operation, such as INSERT/DELETE, etc.
         */
        OP_TYPE,
        /**
         * Type of database operation, such as INSERT/DELETE, etc.
         * It is deprecated and can be replaced by ${@link BuiltInField#OP_TYPE}
         * and will be removed in a future version.
         */
        @Deprecated
        MYSQL_METADATA_EVENT_TYPE,
        /**
         * MySQL binlog data Row
         */
        MYSQL_METADATA_DATA,
        /**
         * The value of the field before update
         */
        METADATA_UPDATE_BEFORE,
        /**
         * Batch id of binlog
         */
        METADATA_BATCH_ID,
        /**
         * Mapping of sql_type table fields to java data type IDs
         */
        METADATA_SQL_TYPE,
        /**
         * The current time when the ROW was received and processed
         */
        METADATA_TS,
        /**
         * The table structure
         */
        METADATA_MYSQL_TYPE,
        /**
         * Primary key field name
         */
        METADATA_PK_NAMES
    }
}
