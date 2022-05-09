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

package org.apache.inlong.manager.common.pojo.group;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

/**
 * Inlong group metric
 */
@Data
@ApiModel("Inlong group metric")
public class InlongGroupMetricResponse {

    @ApiModelProperty(value = "Total record num of read")
    private Integer totalRecordNumOfRead;

    @ApiModelProperty(value = "Total recordNum byte num of read")
    private Integer totalRecordByteNumOfRead;

    @ApiModelProperty(value = "Total record num of write")
    private Integer totalRecordNumOfWrite;

    @ApiModelProperty(value = "Total recordNum byte num of write")
    private Integer totalRecordByteNumOfWrite;

    @ApiModelProperty(value = "Total dirty recode num")
    private Integer totalDirtyRecordNum;

    @ApiModelProperty(value = "Total dirty recode byte")
    private Integer totalDirtyRecordByte;

    @ApiModelProperty(value = "Total speed")
    private Integer totalSpeed;

    @ApiModelProperty(value = "Total Through put")
    private Integer totalThroughput;

    @ApiModelProperty(value = "Total Through put")
    private Integer totalDuration;

}
