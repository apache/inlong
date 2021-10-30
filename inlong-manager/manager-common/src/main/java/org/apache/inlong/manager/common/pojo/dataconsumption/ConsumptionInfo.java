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

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.Date;
import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

/**
 * Data consumption info
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Data consumption info")
public class ConsumptionInfo {

    @ApiModelProperty(value = "key id")
    private Integer id;

    @ApiModelProperty(value = "consumer group id")
    @NotBlank(message = "consumerGroupId can't be null")
    private String consumerGroupId;

    @ApiModelProperty(value = "consumer group name: only support [a-zA-Z0-9_]")
    @NotBlank(message = "consumerGroupName can't be null")
    private String consumerGroupName;

    @ApiModelProperty(value = "consumption in charge")
    @NotNull(message = "inCharges can't be null")
    private String inCharges;

    @ApiModelProperty(value = "consumption target business group id")
    @NotBlank(message = "business group id cannot be null")
    private String inlongGroupId;

    @ApiModelProperty(value = "middleware type for data storage, support Tube, which has high throughput")
    @NotBlank(message = "middlewareType can't be null")
    private String middlewareType;

    @ApiModelProperty(value = "consumption target topic")
    private String topic;

    @ApiModelProperty(value = "middleware cluster url")
    private String masterUrl;

    @ApiModelProperty(value = "whether to filter consumption, 0: not filter, 1: filter")
    private Integer filterEnabled = 0;

    @ApiModelProperty(value = "consumption target data stream identifier")
    private String inlongStreamId;

    @ApiModelProperty(value = "status, 10: pending assigned, 11: pending approval, "
            + "20: approval rejected, 20: approved")
    private Integer status;

    private String creator;

    private String modifier;

    private Date createTime;

    private Date modifyTime;

    @JsonIgnore
    @AssertTrue(message = "when filter enabled, data stream id cannot be null")
    public boolean isValidateFilter() {
        if (filterEnabled == 0) {
            return true;
        }
        return StringUtils.isNotBlank(inlongStreamId);
    }

}
