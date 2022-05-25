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
import org.apache.inlong.manager.common.enums.DataSeparator;
import org.apache.inlong.manager.common.pojo.stream.StreamField;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Inlong stream config.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Inlong stream configuration")
public class InlongStreamConf {

    @ApiModelProperty(value = "Inlong stream ID", required = true)
    private String streamId;

    @ApiModelProperty(value = "Inlong stream name")
    private String name;

    @ApiModelProperty(value = "MQ resource for stream. Default: ${streamId}")
    private String mqResource;

    @ApiModelProperty("Inlong stream description")
    private String description;

    @ApiModelProperty("Is Inlong stream strictly ordered")
    private boolean strictlyOrdered = false;

    @ApiModelProperty("Stream source field list")
    private List<StreamField> streamFields;

    @ApiModelProperty("Data encoding format: UTF-8, GBK")
    private Charset charset = StandardCharsets.UTF_8;

    @ApiModelProperty("Data separator, stored as ASCII code")
    private DataSeparator dataSeparator = DataSeparator.VERTICAL_BAR;

    @ApiModelProperty(value = "Number of access items per day, unit: 10,000 items per day")
    private Integer dailyRecords;

    @ApiModelProperty(value = "peak access per second, unit: bars per second")
    private Integer peakRecords;

    @ApiModelProperty(value = "Access size per day, unit: GB per day")
    private Integer dailyStorage;

}
