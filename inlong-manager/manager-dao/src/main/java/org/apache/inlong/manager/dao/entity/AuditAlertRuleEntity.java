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

package org.apache.inlong.manager.dao.entity;

import lombok.Data;

import java.util.Date;

@Data
public class AuditAlertRuleEntity {

    private Integer id;
    private String inlongGroupId;
    private String inlongStreamId;
    private String auditId;
    private String alertName;
    private String condition; // Keep as String for database storage (JSON format)
    private String level;
    private String notifyType;
    private String receivers;
    private Boolean enabled;
    private Integer isDeleted; // Use Integer to match database int(11) type
    private String creator;
    private String modifier;
    private Date createTime;
    private Date modifyTime; // Modify time
    private Integer version; // Add version field
}