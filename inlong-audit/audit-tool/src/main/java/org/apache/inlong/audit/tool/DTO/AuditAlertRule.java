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

package org.apache.inlong.audit.tool.DTO;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import java.util.Date;

@Data
@ApiModel("Audit Alert Rule Configuration")
public class AuditAlertRule {
    @ApiModelProperty("Rule ID")
    private Integer id;

    @ApiModelProperty(value = "Associated InLong Group ID", required = true)
    @NotBlank(message = "InLong Group ID cannot be blank")
    private String inlongGroupId;

    @ApiModelProperty("Associated InLong Stream ID")
    private String inlongStreamId;

    @ApiModelProperty(value = "Audit ID (associated with specific audit metrics, such as send success count, delay, etc.)", required = true)
    @NotBlank(message = "Audit ID cannot be blank")
    private String auditId;

    @ApiModelProperty(value = "Alert name", required = true)
    @NotBlank(message = "Alert name cannot be blank")
    private String alertName;

    @ApiModelProperty(value = "Trigger condition (e.g., count>10000, delay>60000)", required = true)
    @NotBlank(message = "Trigger condition cannot be blank")
    private String condition; // Can use expressions like "count > 10000 && delay > 60000"

    @ApiModelProperty("Alert level (INFO/WARN/ERROR/CRITICAL)")
    @Pattern(regexp = "^(INFO|WARN|ERROR|CRITICAL)$", message = "Alert level must be one of INFO, WARN, ERROR, or CRITICAL")
    private String level;

    @ApiModelProperty("Notification type (EMAIL/SMS/HTTP)")
    @Pattern(regexp = "^(EMAIL|SMS|HTTP)$", message = "Notification type must be one of EMAIL, SMS, or HTTP")
    private String notifyType;

    @ApiModelProperty("Notification recipients (separated by commas for multiple recipients)")
    private String receivers;

    @ApiModelProperty(value = "Whether enabled", required = true)
    @NotNull(message = "Enabled status cannot be null")
    private Boolean enabled;

    @ApiModelProperty("Create time")
    private Date createTime;

    @ApiModelProperty("Update time")
    private Date updateTime;
}