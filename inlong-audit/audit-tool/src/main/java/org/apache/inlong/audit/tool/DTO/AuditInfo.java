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

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

/**
 * Audit info, including audit count, audit log timestamp.
 */
@Data
public class AuditInfo {

    @ApiModelProperty(value = "inlong group id")
    private String inlongGroupId;

    @ApiModelProperty(value = "inlong stream id")
    private String inlongStreamId;

    @ApiModelProperty(value = "ip")
    private String ip;

    @ApiModelProperty(value = "Audit log timestamp")
    private String logTs;

    @ApiModelProperty(value = "Audit count")
    private long count;

    @ApiModelProperty(value = "Audit delay")
    private long delay;

    @ApiModelProperty(value = "Audit size")
    private long size;

    public AuditInfo() {
    }

    public AuditInfo(String logTs, long count, long delay, long size) {
        this.logTs = logTs;
        this.count = count;
        this.delay = delay;
        this.size = size;
    }
}