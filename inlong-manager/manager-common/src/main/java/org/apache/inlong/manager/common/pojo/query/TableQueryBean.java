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

package org.apache.inlong.manager.common.pojo.query;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;
import org.apache.inlong.manager.common.pojo.query.hive.HiveTableQueryBean;

/**
 * Hive table common attributes
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "tableType", defaultImpl = TableQueryBean.class, visible = true)
@JsonSubTypes({
        @JsonSubTypes.Type(value = HiveTableQueryBean.class, name = "hive"),
})
@Data
public class TableQueryBean {

    private String objectId; // Used to modify
    // Basic attributes
    private String tableName;
    private String tableDesc; // Low-level description of the library table
    private String businessDesc; // Business description

    private String tableType;
    private String dbName;
    private String projectId;

    private String projectName; // Redundant display
    private String projectIdent; // Uniquely identifies projectIdentification
    private String ownerId;
    private String owner;

    // Enhanced attributes
    private String tableNameOfCN; // Table Chinese name
    private String tableNameOfEN; // Table English name

    private String dwTableType; // DW table type: dimension table, fact table

}
