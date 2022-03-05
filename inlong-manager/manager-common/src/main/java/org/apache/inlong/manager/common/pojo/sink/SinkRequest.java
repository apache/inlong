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

package org.apache.inlong.manager.common.pojo.sink;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * Request of sink
 */
@Data
@ApiModel("Request of sink")
@JsonTypeInfo(use = Id.NAME, visible = true, property = "sinkType")
public class SinkRequest {

    private Integer id;

    @ApiModelProperty("Inlong group id")
    @NotNull
    private String inlongGroupId;

    @ApiModelProperty("Inlong stream id")
    @NotNull
    private String inlongStreamId;

    @ApiModelProperty("Sink type, including: HIVE, ES, etc.")
    @NotNull
    private String sinkType;

    @ApiModelProperty("Sink name, unique in one stream.")
    @NotNull
    private String sinkName;

    @ApiModelProperty("Data storage period, unit: day")
    private Integer storagePeriod;

    @ApiModelProperty(value = "Whether to enable create sink resource? 0: disable, 1: enable. default is 1")
    private Integer enableCreateResource = 1;

    @ApiModelProperty("Sink field list")
    private List<SinkFieldRequest> fieldList;

}
