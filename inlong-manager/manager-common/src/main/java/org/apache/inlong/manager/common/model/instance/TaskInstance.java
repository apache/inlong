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

package org.apache.inlong.manager.common.model.instance;

import java.util.Date;

/**
 * Task instance
 */
public class TaskInstance {

    public static final String APPROVERS_DELIMITER = ",";

    public static final String EXT_TRANSFER_USER_KEY = "transferToUsers";

    /**
     * Task ID
     */
    private Integer id;

    /**
     * Task type
     */
    private String type;

    /**
     * Task name
     */
    private String name;

    /**
     * Chinese name of the task
     */
    private String displayName;

    /**
     * Application form ID
     */
    private Integer processInstId;

    /**
     * Process name
     */
    private String processName;
    /**
     * Process display name
     */
    private String processDisplayName;

    /**
     * Applicant
     */
    private String applicant;

    /**
     * Approver
     */
    private String approvers;

    /**
     * Task status
     */
    private String state;

    /**
     * Task operator
     */
    private String operator;

    /**
     * Remarks information
     */
    private String remark;

    /**
     * Form information
     */
    private String formData;

    /**
     * Start time
     */
    private Date startTime;

    /**
     * End event
     */
    private Date endTime;

    /**
     * Extended Information
     */
    private String ext;

    public Integer getId() {
        return id;
    }

    public TaskInstance setId(Integer id) {
        this.id = id;
        return this;
    }

    public String getType() {
        return type;
    }

    public TaskInstance setType(String type) {
        this.type = type;
        return this;
    }

    public String getName() {
        return name;
    }

    public TaskInstance setName(String name) {
        this.name = name;
        return this;
    }

    public String getDisplayName() {
        return displayName;
    }

    public TaskInstance setDisplayName(String displayName) {
        this.displayName = displayName;
        return this;
    }

    public Integer getProcessInstId() {
        return processInstId;
    }

    public TaskInstance setProcessInstId(Integer processInstId) {
        this.processInstId = processInstId;
        return this;
    }

    public String getProcessName() {
        return processName;
    }

    public TaskInstance setProcessName(String processName) {
        this.processName = processName;
        return this;
    }

    public String getProcessDisplayName() {
        return processDisplayName;
    }

    public TaskInstance setProcessDisplayName(String processDisplayName) {
        this.processDisplayName = processDisplayName;
        return this;
    }

    public String getApplicant() {
        return applicant;
    }

    public TaskInstance setApplicant(String applicant) {
        this.applicant = applicant;
        return this;
    }

    public String getApprovers() {
        return approvers;
    }

    public TaskInstance setApprovers(String approvers) {
        this.approvers = approvers;
        return this;
    }

    public String getState() {
        return state;
    }

    public TaskInstance setState(String state) {
        this.state = state;
        return this;
    }

    public String getOperator() {
        return operator;
    }

    public TaskInstance setOperator(String operator) {
        this.operator = operator;
        return this;
    }

    public String getRemark() {
        return remark;
    }

    public TaskInstance setRemark(String remark) {
        this.remark = remark;
        return this;
    }

    public String getFormData() {
        return formData;
    }

    public TaskInstance setFormData(String formData) {
        this.formData = formData;
        return this;
    }

    public Date getStartTime() {
        return startTime;
    }

    public TaskInstance setStartTime(Date startTime) {
        this.startTime = startTime;
        return this;
    }

    public Date getEndTime() {
        return endTime;
    }

    public TaskInstance setEndTime(Date endTime) {
        this.endTime = endTime;
        return this;
    }

    public String getExt() {
        return ext;
    }

    public TaskInstance setExt(String ext) {
        this.ext = ext;
        return this;
    }
}
