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

package org.apache.inlong.manager.pojo.schedule.dolphinschedule;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@Data
public class DSTaskDefinition {

    @ApiModelProperty("DolphinScheduler task definition code")
    @JsonProperty("code")
    private long code;

    @ApiModelProperty("DolphinScheduler task definition code")
    @JsonProperty("delayTime")
    private String delayTime;

    @ApiModelProperty("DolphinScheduler task definition description")
    @JsonProperty("description")
    private String description;

    @ApiModelProperty("DolphinScheduler task definition environment code")
    @JsonProperty("environmentCode")
    private int environmentCode;

    @ApiModelProperty("DolphinScheduler task fail retry interval")
    @JsonProperty("failRetryInterval")
    private String failRetryInterval;

    @ApiModelProperty("DolphinScheduler task definition fail retry times")
    @JsonProperty("failRetryTimes")
    private String failRetryTimes;

    @ApiModelProperty("DolphinScheduler task definition flag")
    @JsonProperty("flag")
    private String flag;

    @ApiModelProperty("DolphinScheduler task definition isCache")
    @JsonProperty("isCache")
    private String isCache;

    @ApiModelProperty("DolphinScheduler task definition name")
    @JsonProperty("name")
    private String name;

    @ApiModelProperty("DolphinScheduler task definition params")
    @JsonProperty("taskParams")
    private DSTaskParams taskParams;

    @ApiModelProperty("DolphinScheduler task definition priority")
    @JsonProperty("taskPriority")
    private String taskPriority;

    @ApiModelProperty("DolphinScheduler task definition type")
    @JsonProperty("taskType")
    private String taskType;

    @ApiModelProperty("DolphinScheduler task definition timeout")
    @JsonProperty("timeout")
    private int timeout;

    @ApiModelProperty("DolphinScheduler task definition timeout flag")
    @JsonProperty("timeoutFlag")
    private String timeoutFlag;

    @ApiModelProperty("DolphinScheduler task definition timeout notify strategy")
    @JsonProperty("timeoutNotifyStrategy")
    private String timeoutNotifyStrategy;

    @ApiModelProperty("DolphinScheduler task definition worker group")
    @JsonProperty("workerGroup")
    private String workerGroup;

    @ApiModelProperty("DolphinScheduler task definition apu quota")
    @JsonProperty("cpuQuota")
    private int cpuQuota;

    @ApiModelProperty("DolphinScheduler task definition memory max")
    @JsonProperty("memoryMax")
    private int memoryMax;

    @ApiModelProperty("DolphinScheduler task definition execute type")
    @JsonProperty("taskExecuteType")
    private String taskExecuteType;

    public DSTaskDefinition() {
        this.delayTime = "0";
        this.description = "";
        this.environmentCode = -1;
        this.failRetryInterval = "1";
        this.failRetryTimes = "0";
        this.flag = "YES";
        this.isCache = "NO";
        this.taskPriority = "MEDIUM";
        this.taskType = "SHELL";
        this.timeoutFlag = "CLOSE";
        this.timeoutNotifyStrategy = "";
        this.workerGroup = "default";
        this.cpuQuota = -1;
        this.memoryMax = -1;
        this.taskExecuteType = "BATCH";
    }
}
