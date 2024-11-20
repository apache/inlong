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
public class DSTaskRelation {

    @ApiModelProperty("DolphinScheduler task relation name")
    @JsonProperty("name")
    private String name;

    @ApiModelProperty("DolphinScheduler task relation pre-task code")
    @JsonProperty("preTaskCode")
    private int preTaskCode;

    @ApiModelProperty("DolphinScheduler task relation pre-task version")
    @JsonProperty("preTaskVersion")
    private int preTaskVersion;

    @ApiModelProperty("DolphinScheduler task relation post-task code")
    @JsonProperty("postTaskCode")
    private long postTaskCode;

    @ApiModelProperty("DolphinScheduler task relation post-task version")
    @JsonProperty("postTaskVersion")
    private int postTaskVersion;

    @ApiModelProperty("DolphinScheduler task relation condition type")
    @JsonProperty("conditionType")
    private String conditionType;

    @ApiModelProperty("DolphinScheduler task relation condition params")
    @JsonProperty("conditionParams")
    private Object conditionParams;

    public DSTaskRelation() {
        this.name = "";
        this.conditionType = "NONE";
    }
}
