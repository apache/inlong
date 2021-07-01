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
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.inlong.manager.common.beans.PageRequest;
import org.springframework.format.annotation.DateTimeFormat;

/**
 * Consumption progress query
 */
@EqualsAndHashCode(callSuper = false)
@Data
@ApiModel("Consumption progress query")
public class ConsumerProgressInfoQuery extends PageRequest {

    @ApiModelProperty(hidden = true, value = "Consumption ID")
    private Integer consumerId;

    @ApiModelProperty(value = "Consumer Group ID", required = true)
    @NotBlank(message = "consumerGroupId can't be null")
    private String consumerGroupId;

    @ApiModelProperty(value = "Reporting time 5 minutes interval: yyyy-MM-dd HH:mm", required = true)
    @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm")
    @NotNull(message = "repTime can't be null")
    private Date repTime;

    @ApiModelProperty(value = "Report date: yyyyMMdd")
    private Long repDate;

    @ApiModelProperty(value = "Consumption TOPIC")
    private String topic;

    @ApiModelProperty(value = "Broker Ip")
    private String brokerIp;

    @ApiModelProperty(value = "Consumer Group IP")
    private String consumerIp;

    @ApiModelProperty(value = "Is the heartbeat normal")
    private String isHeartbeatOk;

    @ApiModelProperty(value = "Is the storage normal")
    private String isStoreOk;

    @ApiModelProperty(value = "state")
    private String state;

    @ApiModelProperty(value = "Middleware Type")
    private String middleware;
}
