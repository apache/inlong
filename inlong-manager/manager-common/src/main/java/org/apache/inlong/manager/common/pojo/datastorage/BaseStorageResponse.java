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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.apache.inlong.manager.common.enums.BizConstant;

/**
 * Basic response of data storage
 */
@Data
@ApiModel("Basic response of data storage")
@JsonTypeInfo(use = Id.NAME, visible = true, property = "storageType")
@JsonSubTypes({
        @Type(value = StorageHiveResponse.class, name = BizConstant.STORAGE_HIVE)
})
public class BaseStorageResponse {

    private Integer id;

    @ApiModelProperty("Business group id")
    private String inlongGroupId;

    @ApiModelProperty("Data stream id")
    private String inlongStreamId;

    @ApiModelProperty("Storage type, including: HDFS, HIVE, ES, etc.")
    private String storageType;

    @ApiModelProperty("Data storage period, unit: day")
    private Integer storagePeriod;

}
