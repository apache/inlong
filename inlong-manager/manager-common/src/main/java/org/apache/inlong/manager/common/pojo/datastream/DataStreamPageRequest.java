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

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.inlong.manager.common.beans.PageRequest;

/**
 * Data stream paging query conditions
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ApiModel("Data flow paging query conditions")
public class DataStreamPageRequest extends PageRequest {

    @ApiModelProperty(value = "Business Identifier")
    private String bid;

    @ApiModelProperty(value = "Keyword")
    private String keyWord;

    @ApiModelProperty(value = "Data source type, including: FILE, DB, AUTO_PUSH (DATA_PROXY_SDK, HTTP)")
    private String dataSourceType;

    @ApiModelProperty(value = "The storage type to be created (which has no data stream of this type)")
    private String storageType;

    @ApiModelProperty(value = "Whether to get the storage type list, 0 (default): not get, 1: get")
    private Integer needStorageList = 0;

    @ApiModelProperty(value = "status")
    private Integer status;

    @ApiModelProperty(value = "Current user", hidden = true)
    private String currentUser;

}
