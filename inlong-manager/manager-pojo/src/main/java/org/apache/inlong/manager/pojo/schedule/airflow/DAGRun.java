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
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
@ApiModel(description = "DAGRun Description Information.")
public class DAGRun {

    @JsonProperty("conf")
    @ApiModelProperty("JSON object describing additional configuration parameters.")
    private Object conf;

    @JsonProperty("dag_id")
    @ApiModelProperty("Airflow DAG id.")
    private String dagId;

    @JsonProperty("dag_run_id")
    @ApiModelProperty("Airflow DAGRun id (Nullable).")
    private String dagRunId;

    @JsonProperty("end_date")
    @ApiModelProperty("The end time of this DAGRun.")
    private String endDate;

    @JsonProperty("start_date")
    @ApiModelProperty("The start time of this DAGRun.")
    private String startDate;
}
