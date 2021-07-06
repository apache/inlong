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

package org.apache.inlong.manager.workflow.model.view;

import com.google.common.collect.Lists;

import org.apache.inlong.manager.workflow.model.TaskState;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.List;

/**
 * Workflow components
 */
@ApiModel("Workflow component node information")
public class ElementView {

    @ApiModelProperty(value = "Node name-English KEY")
    private String name;

    @ApiModelProperty(value = "Node display name")
    private String displayName;

    @ApiModelProperty(value = "Approver")
    private List<String> approvers;

    @ApiModelProperty(value = "The status of the current approval task node")
    private TaskState state;

    @ApiModelProperty(value = "Next approval node")
    private List<ElementView> next = Lists.newArrayList();

    public String getName() {
        return name;
    }

    public ElementView setName(String name) {
        this.name = name;
        return this;
    }

    public String getDisplayName() {
        return displayName;
    }

    public ElementView setDisplayName(String displayName) {
        this.displayName = displayName;
        return this;
    }

    public List<String> getApprovers() {
        return approvers;
    }

    public ElementView setApprovers(List<String> approvers) {
        this.approvers = approvers;
        return this;
    }

    public List<ElementView> getNext() {
        return next;
    }

    public ElementView setNext(List<ElementView> next) {
        this.next = next;
        return this;
    }

    public TaskState getState() {
        return state;
    }

    public ElementView setState(TaskState state) {
        this.state = state;
        return this;
    }
}
