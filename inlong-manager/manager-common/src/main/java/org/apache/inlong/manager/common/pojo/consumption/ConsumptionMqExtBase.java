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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.apache.inlong.manager.common.enums.BizConstant;

/**
 * Extended consumption information of different MQs
 */
@Data
@ApiModel("Extended consumption information of different MQs")
@JsonTypeInfo(use = Id.NAME, visible = true, property = "middlewareType", defaultImpl = ConsumptionMqExtBase.class)
@JsonSubTypes({
        @JsonSubTypes.Type(value = ConsumptionPulsarInfo.class, name = BizConstant.MIDDLEWARE_PULSAR)
})
public class ConsumptionMqExtBase {

    @ApiModelProperty(value = "Self-incrementing primary key")
    private Integer id;

    @ApiModelProperty(value = "Consumer information ID")
    private Integer consumptionId;

    @ApiModelProperty(value = "Consumer group")
    private String consumerGroup;

    @ApiModelProperty(value = "Consumption target inlong group id")
    private String inlongGroupId;

    @ApiModelProperty("Whether to delete, 0: not deleted, 1: deleted")
    private Integer isDeleted = 0;

    @ApiModelProperty(value = "Data storage middleware type, high throughput: TUBE, high consistency: PULSAR")
    private String middlewareType;

}
