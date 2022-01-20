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
import java.util.Date;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;

/**
 * Workflow log query
 */
@Data
@EqualsAndHashCode(callSuper = false)
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Workflow event log query conditions")
public class EventLogQuery extends PageQuery {

    private Integer id;

    @ApiModelProperty("Process ID")
    private Integer processInstId;

    @ApiModelProperty("Process name")
    private String processName;

    @ApiModelProperty("Business group id")
    private String inlongGroupId;

    @ApiModelProperty("Task id")
    private Integer taskInstId;

    @ApiModelProperty("the name of the element that triggered the event")
    private String elementName;

    @ApiModelProperty("event type")
    private String eventType;

    @ApiModelProperty("event")
    private String event;

    @ApiModelProperty("Listener name")
    private String listener;

    @ApiModelProperty("status")
    private Integer state;

    @ApiModelProperty("Is it synchronized")
    private Boolean async;

    @ApiModelProperty("Execute IP")
    private String ip;

    @ApiModelProperty("start time-from")
    @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date startTimeBegin;

    @ApiModelProperty("start time-to")
    @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date startTimeEnd;

    @ApiModelProperty("end time-from")
    @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date endTimeBegin;

    @ApiModelProperty("end time-to")
    @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date endTimeEnd;

    @ApiModelProperty("Whether to include abnormal information")
    private boolean includeException = false;
}
