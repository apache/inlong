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

package org.apache.inlong.manager.common.model.view;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

/**
 * Workflow definition
 */
@ApiModel("Workflow definition information")
public class WorkflowView {

    @ApiModelProperty(value = "process name-English key")
    private String name;

    @ApiModelProperty(value = "process display name")
    private String displayName;

    @ApiModelProperty(value = "process type")
    private String type;

    @ApiModelProperty(value = "approval process-start node")
    private ElementView startEvent;

    public String getName() {
        return name;
    }

    public WorkflowView setName(String name) {
        this.name = name;
        return this;
    }

    public String getDisplayName() {
        return displayName;
    }

    public WorkflowView setDisplayName(String displayName) {
        this.displayName = displayName;
        return this;
    }

    public String getType() {
        return type;
    }

    public WorkflowView setType(String type) {
        this.type = type;
        return this;
    }

    public ElementView getStartEvent() {
        return startEvent;
    }

    public WorkflowView setStartEvent(ElementView startEvent) {
        this.startEvent = startEvent;
        return this;
    }
}
