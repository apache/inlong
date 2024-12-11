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
public class DScheduleInfo {

    @ApiModelProperty("DolphinScheduler schedule start time")
    @JsonProperty("startTime")
    private String startTime;

    @ApiModelProperty("DolphinScheduler schedule end time")
    @JsonProperty("endTime")
    private String endTime;

    @ApiModelProperty("DolphinScheduler schedule crontab expression")
    @JsonProperty("crontab")
    private String crontab;

    @ApiModelProperty("DolphinScheduler schedule timezone id")
    @JsonProperty("timezoneId")
    private String timezoneId;

}
