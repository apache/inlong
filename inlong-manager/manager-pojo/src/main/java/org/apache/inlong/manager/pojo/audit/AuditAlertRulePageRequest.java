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

package org.apache.inlong.manager.pojo.audit;

import org.apache.inlong.manager.pojo.common.PageRequest;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * Audit alert rule paging query conditions
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@ApiModel("Audit alert rule paging query request")
public class AuditAlertRulePageRequest extends PageRequest {

    @ApiModelProperty("Rule ID")
    private Integer id;

    @ApiModelProperty(value = "Associated InLong Group ID")
    private String inlongGroupId;

    @ApiModelProperty("Associated InLong Stream ID")
    private String inlongStreamId;

    @ApiModelProperty(value = "Audit ID (associated with specific audit metrics)")
    private String auditId;

    @ApiModelProperty("Alert name")
    private String alertName;

    @ApiModelProperty("Alert level (INFO/WARN/ERROR/CRITICAL)")
    private String level;

    @ApiModelProperty("Whether enabled")
    private Boolean enabled;
}