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

import lombok.Data;

@Data
public class DatabaseQueryBean {

    private String objectId;
    private String dbType;
    private String dbName;
    private String dbDesc;
    private String businessDesc;
    private String owner;
    private String ownerId;
    private String projectId;
    private String projectIdent;
    private String projectName;
    private String jdbcUrl;
    private String userName;
    private String password;
    // Whether to create physical DB on hive and hbase, physical DB is created by default
    private boolean enableAllocateResource = true;
    // Whether to cascade to update the owner of the table
    private boolean updateOwnerCascade = false;

}
