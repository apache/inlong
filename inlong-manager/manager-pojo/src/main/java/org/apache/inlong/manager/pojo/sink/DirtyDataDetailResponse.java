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

package org.apache.inlong.manager.pojo.sink;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

/**
 * Dirty data detail info.
 */
@Data
@ApiModel("Dirty data detail info")
public class DirtyDataDetailResponse {

    @ApiModelProperty(value = "Dirty data partition date")
    private String dirtyDataPartitionDate;

    @ApiModelProperty(value = "Data flow id")
    private String dataFlowId;

    @ApiModelProperty(value = "Inlong group id")
    private String groupId;

    @ApiModelProperty(value = "Inlong stream id")
    private String streamId;

    @ApiModelProperty(value = "Report time")
    private String reportTime;

    @ApiModelProperty(value = "Data time")
    private String dataTime;

    @ApiModelProperty(value = "Server type")
    private String serverType;

    @ApiModelProperty(value = "Dirty type")
    private String dirtyType;

    @ApiModelProperty(value = "Dirty message")
    private String dirtyMessage;

    @ApiModelProperty(value = "Ext info")
    private String extInfo;

    @ApiModelProperty(value = "Dirty data")
    private String dirtyData;
}
