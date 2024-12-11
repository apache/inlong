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
@ApiModel(description = "DAG Description Information.")
public class DAG {

    @JsonProperty("dag_id")
    @ApiModelProperty("The ID of the DAG.")
    private String dagId;

    @JsonProperty("root_dag_id")
    @ApiModelProperty("If the DAG is SubDAG then it is the top level DAG identifier. Otherwise, null.")
    private String rootDagId;

    @JsonProperty("is_paused")
    @ApiModelProperty("Whether the DAG is paused.")
    private Boolean isPaused;

    @JsonProperty("is_active")
    @ApiModelProperty("Whether the DAG is currently seen by the scheduler(s).")
    private Boolean isActive;

    @JsonProperty("description")
    @ApiModelProperty("User-provided DAG description, which can consist of several sentences or paragraphs that describe DAG contents.")
    private String description;
}
