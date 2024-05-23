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

package org.apache.inlong.manager.pojo.schedule;

import org.apache.inlong.manager.common.validation.UpdateValidation;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import javax.validation.constraints.NotNull;

import java.sql.Timestamp;

@Data
@ApiModel("Schedule request")
public class ScheduleInfoRequest {

    @ApiModelProperty(value = "Primary key")
    @NotNull(groups = UpdateValidation.class)
    private Integer id;

    @ApiModelProperty("Inlong Group ID")
    @NotNull
    private String inlongGroupId;

    // schedule type, support [normal, crontab], 0 for normal and 1 for crontab
    @ApiModelProperty("Schedule type")
    private Integer scheduleType;

    // time unit for offline task schedule interval, support [month, week, day, hour, minute, oneway]
    // M=month, W=week, D=day, H=hour, M=minute, O=oneway
    @ApiModelProperty("TimeUnit for schedule interval")
    private String scheduleUnit;

    @ApiModelProperty("Schedule interval")
    private Integer scheduleInterval;

    @ApiModelProperty("Start time")
    private Timestamp startTime;

    @ApiModelProperty("End time")
    private Timestamp endTime;

    @ApiModelProperty("Delay time")
    private Integer delayTime;

    @ApiModelProperty("Self depend")
    private Integer selfDepend;

    @ApiModelProperty("Schedule task parallelism")
    private Integer taskParallelism;

    @ApiModelProperty("Schedule task parallelism")
    private Integer crontabExpression;

    @ApiModelProperty(value = "Version number")
    @NotNull(groups = UpdateValidation.class, message = "version cannot be null")
    private Integer version;

}
