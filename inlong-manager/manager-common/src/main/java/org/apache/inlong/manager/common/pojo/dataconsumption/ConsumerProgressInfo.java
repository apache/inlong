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

package org.apache.inlong.manager.common.pojo.dataconsumption;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.Date;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Data consumption progress
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Data consumption progress")
public class ConsumerProgressInfo {

    @ApiModelProperty("Report time")
    private Date repTime;

    @ApiModelProperty("Report date")
    private Long repDate;

    @ApiModelProperty("Consumer group")
    private String consumerGroup;

    @ApiModelProperty("Consumption TOPIC")
    private String topic;

    @ApiModelProperty("Broker IP")
    private String brokerIp;

    @ApiModelProperty("Broker Id")
    private Long brokerId;

    @ApiModelProperty("Partition ID")
    private Long part;

    @ApiModelProperty("Consumption ID")
    private String consumerId;

    @ApiModelProperty("Consumer IP")
    private String consumerIp;

    @ApiModelProperty("Consumer process ID")
    private String consumerPid;

    @ApiModelProperty("Is the heartbeat normal")
    private String isHeartbeatOk;

    @ApiModelProperty("heartbeat time")
    private String hartbeat;

    @ApiModelProperty("Is the storage normal")
    private String isStoreOk;

    @ApiModelProperty("Consumption difference")
    private Double fileConSize;

    @ApiModelProperty("Production speed")
    private Double fileSpeed;

    @ApiModelProperty("Consumption speed")
    private Double consumeSpeed;

    @ApiModelProperty("Consumption difference speed")
    private Double fileConSpeed;

    @ApiModelProperty("Status: 0 normal; 1 abnormal; 2 shielded; 3 not yet available")
    private String state;

    @ApiModelProperty("Middleware type")
    private String middleware;

    private Double tmpOffset;
    private Double minOffset;
    private Double fileOffset;
    private Double consumerOffset;
    private Double saveFileSize;
    private Integer intervalSec;
}
