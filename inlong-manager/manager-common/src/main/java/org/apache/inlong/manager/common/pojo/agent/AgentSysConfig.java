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

package org.apache.inlong.manager.common.pojo.agent;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@ApiModel("Agent system config")
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
public class AgentSysConfig {

    @ApiModelProperty(value = "msgQueueSize")
    private long msgQueueSize;

    @ApiModelProperty(value = "clearDayOffset")
    private int clearDayOffset;

    @ApiModelProperty(value = "bufferSize")
    private int bufferSize;

    @ApiModelProperty(value = "flushEventTimeoutMillSec")
    private int flushEventTimeoutMillSec;

    @ApiModelProperty(value = "threadManagerSleepInterval")
    private int threadManagerSleepInterval;

    @ApiModelProperty(value = "sendRunnableSize")
    private int sendRunnableSize;

    @ApiModelProperty(value = "minRetryThreads")
    private int minRetryThreads;

    @ApiModelProperty(value = "clearIntervalSec")
    private long clearIntervalSec;

    @ApiModelProperty(value = "maxReaderCnt")
    private int maxReaderCnt;

    @ApiModelProperty(value = "sendTimeoutMillSec")
    private int sendTimeoutMillSec;

    @ApiModelProperty(value = "bufferSizeInBytes")
    private int bufferSizeInBytes;

    @ApiModelProperty(value = "maxRetryThreads")
    private int maxRetryThreads;

    @ApiModelProperty(value = "dbPath")
    private String dbPath;

    @ApiModelProperty(value = "scanIntervalSec")
    private int scanIntervalSec;

    @ApiModelProperty(value = "compress")
    private boolean compress;

    @ApiModelProperty(value = "isCalcMd5")
    private boolean isCalcMd5;

    @ApiModelProperty(value = "agent ip")
    private String ip;

    @ApiModelProperty(value = "flowSize")
    private long flowSize;

    @ApiModelProperty(value = "onelineSize")
    private int onelineSize;

    @ApiModelProperty(value = "eventCheckInterval")
    private int eventCheckInterval;

    @ApiModelProperty(value = "confRefreshIntervalSecs")
    private int confRefreshIntervalSecs;

    @ApiModelProperty(value = "statIntervalSec")
    private int statIntervalSec;

    @ApiModelProperty(value = "msgSize")
    private int msgSize;

    @ApiModelProperty(value = "md5")
    private String md5;

    @ApiModelProperty(value = "batchSize")
    private int batchSize;

    @ApiModelProperty(value = "agentRpcReconnectTime")
    private int agentRpcReconnectTime;

}
