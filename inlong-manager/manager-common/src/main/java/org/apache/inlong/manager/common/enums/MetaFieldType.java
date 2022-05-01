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

package org.apache.inlong.manager.common.enums;

import lombok.Getter;

@Getter
public enum MetaFieldType {

    /**
     * database
     */
    DATABASE("database", "meta field database used in canal json or mysql binlong and so on"),

    /**
     * processing_time
     */
    PROCESSING_TIME("processing_time", "meta field processing_time describe such moment the record be processed"),

    /**
     * data_time
     */
    DATA_TIME("data_time", "meta field data_time used in canal json or mysql binlong and so on"),

    /**
     * table
     */
    TABLE("table", "meta field table used in canal json or mysql binlong and so on"),

    /**
     * event_time
     */
    EVENT_TIME("event_time", "meta field event_time used in canal json or mysql binlong and so on"),

    /**
     * is_ddl
     */
    IS_DDL("is_ddl", "meta field is_ddl used in canal json or mysql binlong and so on"),

    /**
     * event_type
     */
    EVENT_TYPE("event_type", "meta field event_type used in canal json or mysql binlong and so on"),

    /**
     * data
     */
    MYSQL_DATA("data", "MySQL binlog data Row"),

    /**
     * update_before
     */
    UPDATE_BEFORE("update_before", "The value of the field before update"),

    /**
     * Batch id of binlog
     */
    BATCH_ID("batch_id", "Batch id of binlog"),

    /**
     * sql_type
     */
    SQL_TYPE("sql_type", "Mapping of sql_type table fields to java data type IDs"),

    /**
     * ts
     */
    TS("ts", "The current time when the ROW was received and processed"),

    /**
     * mysql_type
     */
    MYSQL_TYPE("mysql_type", "The table structure"),

    /**
     * pk_names
     */
    PK_NAMES("pk_names", "Primary key field name");

    private final String name;

    private final String description;

    MetaFieldType(String name, String description) {
        this.name = name;
        this.description = description;
    }
}
