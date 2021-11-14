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

package org.apache.inlong.manager.common.pojo.datastorage;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.Date;
import lombok.Data;
import org.apache.inlong.manager.common.enums.BizConstant;

/**
 * Basic data storage information
 */
@Data
@ApiModel("Basic data storage information")
@JsonTypeInfo(use = Id.NAME, visible = true, property = "storageType")
@JsonSubTypes({
        @Type(value = StorageHiveInfo.class, name = BizConstant.STORAGE_TYPE_HIVE)
})
public class BaseStorageInfo {

    private Integer id;

    @ApiModelProperty("Business group id")
    private String inlongGroupId;

    @ApiModelProperty("Data stream id")
    private String inlongStreamId;

    @ApiModelProperty("Storage type, including: HDFS, HIVE, ES, etc.")
    private String storageType;

    @ApiModelProperty("Data storage period, unit: day")
    private Integer storagePeriod;

    @ApiModelProperty("Status")
    private Integer status;

    @ApiModelProperty("Previous State")
    private Integer previousStatus;

    @ApiModelProperty("is deleted? 0: deleted, 1: not deleted")
    private Integer isDeleted = 0;

    private String creator;

    private String modifier;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date createTime;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date modifyTime;

    @ApiModelProperty("Temporary view, string in JSON format")
    private String tempView;

}
