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
import java.util.Objects;
import org.hibernate.validator.constraints.Length;

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

    // schedule engine type, support [Quartz, Airflow, Dolphinscheduler]
    @ApiModelProperty(value = "Schedule engine")
    private String scheduleEngine;

    // time unit for offline task schedule interval, support [month, week, day, hour, minute, oneround]
    // Y=year, M=month, W=week, D=day, H=hour, I=minute, O=oneround
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
    private String crontabExpression;

    @ApiModelProperty(value = "Version number")
    @NotNull(groups = UpdateValidation.class, message = "version cannot be null")
    private Integer version;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ScheduleInfoRequest that = (ScheduleInfoRequest) o;
        return Objects.equals(inlongGroupId, that.inlongGroupId)
                && Objects.equals(scheduleType, that.scheduleType)
                && Objects.equals(scheduleEngine, that.scheduleEngine)
                && Objects.equals(scheduleUnit, that.scheduleUnit)
                && Objects.equals(scheduleInterval, that.scheduleInterval)
                && Objects.equals(startTime, that.startTime)
                && Objects.equals(endTime, that.endTime)
                && Objects.equals(delayTime, that.delayTime)
                && Objects.equals(selfDepend, that.selfDepend)
                && Objects.equals(taskParallelism, that.taskParallelism)
                && Objects.equals(crontabExpression, that.crontabExpression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, inlongGroupId, scheduleType, scheduleEngine, scheduleUnit, scheduleInterval,
                startTime, endTime, delayTime, selfDepend, taskParallelism, crontabExpression, version);
    }
}
