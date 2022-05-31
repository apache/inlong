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
    DATABASE("database", "Describe the database name of data Row"),

    /**
     * processing_time
     */
    PROCESSING_TIME("processing_time", "Describe such moment the event be processed"),

    /**
     * table
     */
    TABLE("table", "Describe the table name of data Row"),

    /**
     * event_time
     */
    EVENT_TIME("event_time", "Describe event change time"),

    /**
     * is_ddl
     */
    IS_DDL("is_ddl", "Describe whether it is a ddl"),

    /**
     * event_type
     */
    EVENT_TYPE("event_type", "Describe event operation type"),

    /**
     * data
     */
    DATA("data", "The event change data Row"),

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
    TS("ts", "The current time when the data ROW was received and processed"),

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
