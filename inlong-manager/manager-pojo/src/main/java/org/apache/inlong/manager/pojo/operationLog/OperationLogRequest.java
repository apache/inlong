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

package org.apache.inlong.manager.pojo.operationLog;

import org.apache.inlong.manager.pojo.common.PageRequest;

import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Request of get operation records
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OperationLogRequest extends PageRequest {

    @ApiModelProperty("Inlong group id")
    private String inlongGroupId;

    @ApiModelProperty("Inlong stream id")
    private String inlongStreamId;

    @ApiModelProperty("Operation type")
    private String operationType;

    @ApiModelProperty("Operation target")
    private String operationTarget;

    @ApiModelProperty("Ip")
    private String ip;

    @ApiModelProperty("Port")
    private String port;

    @ApiModelProperty(value = "keyword")
    private String keyword;

    @ApiModelProperty(value = "status")
    private Boolean status;

    @ApiModelProperty(value = "query start date, format by 'yyyy-MM-dd'", required = false, example = "2022-01-01")
    private String startDate;

    @ApiModelProperty(value = "query end date, format by 'yyyy-MM-dd'", required = false, example = "2022-01-01")
    private String endDate;

}
