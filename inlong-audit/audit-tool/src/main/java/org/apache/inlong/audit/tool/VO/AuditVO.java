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

package org.apache.inlong.audit.tool.VO;

import org.apache.inlong.audit.tool.DTO.AuditInfo;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.util.List;

/**
 * The VO of audit.
 */
@Data
public class AuditVO {

    @ApiModelProperty(value = "Audit id")
    private String auditId;
    @ApiModelProperty(value = "Audit name")
    private String auditName;
    @ApiModelProperty(value = "Audit set")
    private List<AuditInfo> auditSet;
    @ApiModelProperty(value = "Node type")
    private String nodeType;

    public AuditVO() {
    }

    public AuditVO(String auditId, String auditName, List<AuditInfo> auditSet, String nodeType) {
        this.auditId = auditId;
        this.auditName = auditName;
        this.auditSet = auditSet;
        this.nodeType = nodeType;
    }
}
