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
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Data stream configuration")
public class DataStreamConf {

    @ApiModelProperty(
            value = "Data stream name",
            required = true
    )
    private String name;

    @ApiModelProperty("Data stream description")
    private String description;

    @ApiModelProperty("Field list")
    private List<StreamField> streamFields;

    @ApiModelProperty("Data source from which data is collected")
    private DataSource source;

    @ApiModelProperty("Data storage to which data is sinked")
    private DataStorage storage;

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
