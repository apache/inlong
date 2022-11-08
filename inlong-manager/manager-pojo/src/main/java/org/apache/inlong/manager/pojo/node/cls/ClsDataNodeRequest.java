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

package org.apache.inlong.manager.pojo.node.cls;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.util.JsonTypeDefine;
import org.apache.inlong.manager.pojo.node.DataNodeRequest;

/**
 * Cls data node request
 */
@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@JsonTypeDefine(value = DataNodeType.CLS)
@ApiModel("Cls data node request")
public class ClsDataNodeRequest extends DataNodeRequest {

    @ApiModelProperty("Max send thread count, default is 50")
    private Integer maxSendThreadCount = 50;

    @ApiModelProperty("Max block time, default is 0 seconds")
    private Integer maxBlockSec = 0;

    @ApiModelProperty("Max message batch count, default is 4096")
    private Integer maxBatchCount = 4096;

    @ApiModelProperty("Linger, default is 2000ms")
    private Integer lingerMs = 2000;

    @ApiModelProperty("Retry times, default is 10")
    private Integer retries = 10;

    @ApiModelProperty("Max reserve attempts, default is 11")
    private Integer maxReservedAttempts = 11;

    @ApiModelProperty("Base retry backoff time, default is 100ms")
    private Integer baseRetryBackoffMs = 100;

    @ApiModelProperty("Max retry backoff time, default is 50ms")
    private Integer maxRetryBackoffMs = 50;

    public ClsDataNodeRequest() {
        this.setType(DataNodeType.CLS);
    }
}
