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

import java.util.ArrayList;
import java.util.List;
@Data
public class DSTaskParams {

    @ApiModelProperty("DolphinScheduler task params local params")
    @JsonProperty("localParams")
    private List<Object> localParams;

    @ApiModelProperty("DolphinScheduler task params raw script")
    @JsonProperty("rawScript")
    private String rawScript;

    @ApiModelProperty("DolphinScheduler task params resource list")
    @JsonProperty("resourceList")
    private List<Object> resourceList;

    public DSTaskParams() {
        this.localParams = new ArrayList<>();
        this.resourceList = new ArrayList<>();
        this.rawScript = "";
    }
}
