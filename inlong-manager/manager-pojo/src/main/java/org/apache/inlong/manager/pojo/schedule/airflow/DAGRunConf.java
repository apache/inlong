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

package org.apache.inlong.manager.pojo.schedule.airflow;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@ApiModel(description = "DAGRunConf Description Information.")
public class DAGRunConf {

    @JsonProperty("inlong_group_id")
    @ApiModelProperty("Specify the Inlong group ID")
    private String inlongGroupId;

    @JsonProperty("start_time")
    @ApiModelProperty("The start time of DAG scheduling.")
    private long startTime;

    @JsonProperty("end_time")
    @ApiModelProperty("The end time of DAG scheduling.")
    private long endTime;

    @JsonProperty("boundary_type")
    @ApiModelProperty("The offline task boundary type.")
    private String boundaryType;

    @JsonProperty("cron_expr")
    @ApiModelProperty("Cron expression.")
    private String cronExpr;

    @JsonProperty("seconds_interval")
    @ApiModelProperty("Time interval (in seconds).")
    private String secondsInterval;

    @JsonProperty("connection_id")
    @ApiModelProperty("Airflow Connection Id of Inlong Manager.")
    private String connectionId;

    @JsonProperty("timezone")
    @ApiModelProperty("The timezone.")
    private String timezone;
}
