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

import lombok.ToString;

/**
 * Event log
 */
@ToString
public class EventLog {

    private Integer id;

    private Integer processInstId;

    private String processName;

    private String processDisplayName;

    private String inlongGroupId;

    private Integer taskInstId;

    private String elementName;

    private String elementDisplayName;

    private String eventType;

    private String event;

    private String listener;

    private Date startTime;

    private Date endTime;

    private Integer state;

    private Boolean async;

    private String ip;

    private String remark;

    private String exception;

    public Integer getId() {
        return id;
    }

    public EventLog setId(Integer id) {
        this.id = id;
        return this;
    }

    public Integer getProcessInstId() {
        return processInstId;
    }

    public EventLog setProcessInstId(Integer processInstId) {
        this.processInstId = processInstId;
        return this;
    }

    public String getProcessName() {
        return processName;
    }

    public EventLog setProcessName(String processName) {
        this.processName = processName;
        return this;
    }

    public String getProcessDisplayName() {
        return processDisplayName;
    }

    public EventLog setProcessDisplayName(String processDisplayName) {
        this.processDisplayName = processDisplayName;
        return this;
    }

    public String getBusinessId() {
        return inlongGroupId;
    }

    public EventLog setBusinessId(String groupId) {
        this.inlongGroupId = groupId;
        return this;
    }

    public Integer getTaskInstId() {
        return taskInstId;
    }

    public EventLog setTaskInstId(Integer taskInstId) {
        this.taskInstId = taskInstId;
        return this;
    }

    public String getElementDisplayName() {
        return elementDisplayName;
    }

    public EventLog setElementDisplayName(String elementDisplayName) {
        this.elementDisplayName = elementDisplayName;
        return this;
    }

    public String getElementName() {
        return elementName;
    }

    public EventLog setElementName(String elementName) {
        this.elementName = elementName;
        return this;
    }

    public String getEventType() {
        return eventType;
    }

    public EventLog setEventType(String eventType) {
        this.eventType = eventType;
        return this;
    }

    public String getEvent() {
        return event;
    }

    public EventLog setEvent(String event) {
        this.event = event;
        return this;
    }

    public String getListener() {
        return listener;
    }

    public EventLog setListener(String listener) {
        this.listener = listener;
        return this;
    }

    public Date getStartTime() {
        return startTime;
    }

    public EventLog setStartTime(Date startTime) {
        this.startTime = startTime;
        return this;
    }

    public Date getEndTime() {
        return endTime;
    }

    public EventLog setEndTime(Date endTime) {
        this.endTime = endTime;
        return this;
    }

    public Integer getState() {
        return state;
    }

    public EventLog setState(Integer state) {
        this.state = state;
        return this;
    }

    public Boolean getAsync() {
        return async;
    }

    public EventLog setAsync(Boolean async) {
        this.async = async;
        return this;
    }

    public String getIp() {
        return ip;
    }

    public EventLog setIp(String ip) {
        this.ip = ip;
        return this;
    }

    public String getRemark() {
        return remark;
    }

    public EventLog setRemark(String remark) {
        this.remark = remark;
        return this;
    }

    public String getException() {
        return exception;
    }

    public EventLog setException(String exception) {
        this.exception = exception;
        return this;
    }
}
