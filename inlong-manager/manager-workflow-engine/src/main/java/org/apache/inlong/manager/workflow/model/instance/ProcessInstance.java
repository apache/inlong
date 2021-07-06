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

package org.apache.inlong.manager.workflow.model.instance;

import java.util.Date;

/**
 * Process instance
 */
public class ProcessInstance {

    /**
     * Application form ID
     */
    private Integer id;

    /**
     * Process name-English key
     */
    private String name;

    /**
     * Process display name-Chinese
     */
    private String displayName;

    /**
     * Process classification
     */
    private String type;

    /**
     * Process title
     */
    private String title;

    /**
     * Business ID
     */
    private String businessId;

    /**
     * applicant
     */
    private String applicant;

    /**
     * Process status
     */
    private String state;

    /**
     * Form information
     */
    private String formData;

    /**
     * application time
     */
    private Date startTime;

    /**
     * End Time
     */
    private Date endTime;

    /**
     * Extended Information
     */
    private String ext;

    /**
     * Whether to hide
     */
    private Boolean hidden;

    public Integer getId() {
        return id;
    }

    public ProcessInstance setId(Integer id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public ProcessInstance setName(String name) {
        this.name = name;
        return this;
    }

    public String getDisplayName() {
        return displayName;
    }

    public ProcessInstance setDisplayName(String displayName) {
        this.displayName = displayName;
        return this;
    }

    public String getType() {
        return type;
    }

    public ProcessInstance setType(String type) {
        this.type = type;
        return this;
    }

    public String getTitle() {
        return title;
    }

    public ProcessInstance setTitle(String title) {
        this.title = title;
        return this;
    }

    public String getBusinessId() {
        return businessId;
    }

    public ProcessInstance setBusinessId(String businessId) {
        this.businessId = businessId;
        return this;
    }

    public String getApplicant() {
        return applicant;
    }

    public ProcessInstance setApplicant(String applicant) {
        this.applicant = applicant;
        return this;
    }

    public String getState() {
        return state;
    }

    public ProcessInstance setState(String state) {
        this.state = state;
        return this;
    }

    public String getFormData() {
        return formData;
    }

    public ProcessInstance setFormData(String formData) {
        this.formData = formData;
        return this;
    }

    public Date getStartTime() {
        return startTime;
    }

    public ProcessInstance setStartTime(Date startTime) {
        this.startTime = startTime;
        return this;
    }

    public Date getEndTime() {
        return endTime;
    }

    public ProcessInstance setEndTime(Date endTime) {
        this.endTime = endTime;
        return this;
    }

    public String getExt() {
        return ext;
    }

    public ProcessInstance setExt(String ext) {
        this.ext = ext;
        return this;
    }

    public Boolean getHidden() {
        return hidden;
    }

    public ProcessInstance setHidden(Boolean hidden) {
        this.hidden = hidden;
        return this;
    }
}
