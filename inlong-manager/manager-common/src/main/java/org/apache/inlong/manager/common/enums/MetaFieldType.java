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
    EVENT_TYPE("event_type", "meta field event_type used in canal json or mysql binlong and so on");

    private final String name;

    private final String description;

    MetaFieldType(String name, String description) {
        this.name = name;
        this.description = description;
    }
}
