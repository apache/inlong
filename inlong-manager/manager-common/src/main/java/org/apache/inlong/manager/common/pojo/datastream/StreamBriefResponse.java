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

package org.apache.inlong.manager.common.pojo.datastream;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.apache.inlong.manager.common.pojo.datastorage.StorageBriefResponse;

import java.util.Date;
import java.util.List;

/**
 * Summary response of the data stream
 */
@Data
@ApiModel("Summary response of the data stream")
public class StreamBriefResponse {

    @ApiModelProperty(value = "Primary key")
    private Integer id;

    @ApiModelProperty(value = "Business group id")
    private String inlongGroupId;

    @ApiModelProperty(value = "Data stream id")
    private String inlongStreamId;

    @ApiModelProperty(value = "Data stream name")
    private String name;

    @ApiModelProperty(value = "Data source type, support: FILE/DB/AUTO_PUSH")
    private String dataSourceType;

    @ApiModelProperty(value = "Storage summary list")
    private List<StorageBriefResponse> storageList;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date modifyTime;

}
