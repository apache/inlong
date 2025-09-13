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

package org.apache.inlong.audit.tool.dto;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Data
@ApiModel("Audit Alert Condition")
public class AuditAlertCondition {

    @ApiModelProperty(value = "Condition type (e.g., data_loss, delay, count)", required = true)
    @NotBlank(message = "Condition type cannot be blank")
    private String type;

    @ApiModelProperty(value = "Operator for comparison (e.g., >, <, >=, <=, ==, !=)", required = true)
    @NotBlank(message = "Operator cannot be blank")
    private String operator;

    @ApiModelProperty(value = "Value for comparison", required = true)
    @NotNull(message = "Value cannot be null")
    private Double value;

}