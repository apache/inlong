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
import java.util.Date;
import java.util.List;
import lombok.Data;

/**
 * Data stream list
 */
@Data
@ApiModel("Data stream list")
public class DataStreamListVO {

    @ApiModelProperty(value = "Primary key")
    private Integer id;

    @ApiModelProperty(value = "Data stream id")
    private String inlongStreamId;

    @ApiModelProperty(value = "Business group id")
    private String inlongGroupId;

    @ApiModelProperty(value = "Data stream name")
    private String name;

    @ApiModelProperty(value = "Data source type, FILE/DB/AUTO_PUSH")
    private String dataSourceType;

    @ApiModelProperty(value = "Data type, TEXT, KEY-VALUE, PB, BON")
    private String dataType;

    @ApiModelProperty(value = "Data storage destination collection")
    private List<String> storageTypeList;

    @ApiModelProperty(value = "Status")
    private Integer status;

    private String modifier;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date modifyTime;

}
