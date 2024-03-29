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

/**
 * Workflow task entity
 */
@Data
public class WorkflowTaskEntity {

    public static final String APPROVERS_DELIMITER = ",";
    public static final String EXT_TRANSFER_USER_KEY = "transferToUsers";

    private Integer id;
    private String type;
    private String name;
    private String displayName;
    private Integer processId;
    private String processName;
    private String processDisplayName;

    private String tenant;
    private String applicant;
    private String approvers;
    private String status;
    private String operator;
    private String remark;
    private String formData;
    private Date startTime;
    private Date endTime;
    private String extParams;

}
