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

package org.apache.inlong.manager.client.api;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Inlong group configuration")
public class InlongGroupConf {

    @ApiModelProperty(value = "Group name", required = true)
    private String groupName;

    @ApiModelProperty("Chinese display name")
    private String cnName;

    @ApiModelProperty("Group description")
    private String description;

    @ApiModelProperty("Message queue configuration")
    private MqBaseConf mqBaseConf;

    @ApiModelProperty("Sort configuration")
    private SortBaseConf sortBaseConf;

    @ApiModelProperty("Number of access items per day, unit: 10,000 items per day")
    private Integer dailyRecords;

    @ApiModelProperty("peak access per second, unit: bars per second")
    private Integer peakRecords;

    @ApiModelProperty("The maximum length of a single piece of data, unit: Byte")
    private Integer maxLength;

    @ApiModelProperty("The maximum length of a single piece of data, unit: Byte")
    private String operator;
}
