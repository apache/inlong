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
import lombok.EqualsAndHashCode;

/**
 * Data stream info
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ApiModel("Data stream info")
public class DataStreamInfo extends DataStreamBaseInfo {

    @ApiModelProperty(value = "Primary key")
    private Integer id;

    @ApiModelProperty(value = "Data stream name", required = true)
    private String name;

    @ApiModelProperty(value = "Data stream description")
    private String description;

    @ApiModelProperty(value = "Data source type, including: FILE, DB, AUTO_PUSH (DATA_PROXY_SDK, HTTP)")
    private String dataSourceType;

    @ApiModelProperty(value = "Data storage period in MQ, unit: day (required when dataSourceType=AUTO_PUSH)")
    private Integer storagePeriod;

    @ApiModelProperty(value = "Data type, only support: TEXT")
    private String dataType;

    @ApiModelProperty(value = "Data encoding format: UTF-8, GBK (required when dataSourceType=FILE, AUTO_PUSH)")
    private String dataEncoding;

    @ApiModelProperty(value = "Field delimiter, stored as ASCII code (required when dataSourceType=FILE, AUTO_PUSH)")
    private String fileDelimiter;

    @ApiModelProperty(value = "(File and DB access) Whether there are predefined fields, 0: no, 1: yes")
    private Integer havePredefinedFields;

    @ApiModelProperty(value = "Names of responsible persons, separated by commas")
    private String inCharges;

    @ApiModelProperty(value = "Status")
    private Integer status;

    @ApiModelProperty(value = "Previous status")
    private Integer previousStatus;

    @ApiModelProperty(value = "is deleted? 0: deleted, 1: not deleted")
    private Integer isDeleted = 0;

    private String creator;

    private String modifier;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date createTime;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date modifyTime;

    @ApiModelProperty(value = "Temporary view, string in JSON format")
    private String tempView;

    @ApiModelProperty(value = "Extended information list")
    private List<DataStreamExtInfo> extList;

    @ApiModelProperty(value = "Field list")
    private List<DataStreamFieldInfo> fieldList;

}