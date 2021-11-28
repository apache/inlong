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

package org.apache.inlong.manager.common.pojo.consumption;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.inlong.manager.common.beans.PageRequest;

/**
 * Data consumption query
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ApiModel("Data consumption query conditions")
public class ConsumptionQuery extends PageRequest {

    @ApiModelProperty(value = "Consumer Group Name")
    private String consumerGroupName;

    @ApiModelProperty(value = "Consumer Group ID")
    private String consumerGroupId;

    @ApiModelProperty(value = "Consumer Group Id is fuzzy")
    private String consumerGroupIdLike;

    @ApiModelProperty(value = "Person in charge of consumption")
    private String inCharges;

    @ApiModelProperty(value = "Consumption target business group id")
    private String inlongGroupId;

    @ApiModelProperty(value = "Middleware type, high throughput: TUBE, high consistency: PULSAR")
    private String middlewareType;

    @ApiModelProperty(value = "Consumption target TOPIC")
    private String topic;

    @ApiModelProperty(value = "Fuzzy matching of consumption target TOPIC")
    private String topicLike;

    @ApiModelProperty(value = "Whether to filter consumption")
    private Boolean filterEnabled;

    @ApiModelProperty(value = "Consumption target stream id")
    private String inlongStreamId;

    @ApiModelProperty(value = "Status: Draft: 0, Pending distribution: 10, "
            + "Pending approval: 11, Approval rejected: 20, Approved: 21")
    private Integer status;

    @ApiModelProperty(value = "Consumption status: normal: 0, abnormal: 1, shielded: 2, no: 3")
    private Integer lastConsumptionStatus;

    private String creator;

    private String modifier;

    @ApiModelProperty(value = "User")
    private String userName;

    @ApiModelProperty(value = "Fuzzy query keyword, fuzzy query topic, consumer group ID")
    private String keyword;
}
