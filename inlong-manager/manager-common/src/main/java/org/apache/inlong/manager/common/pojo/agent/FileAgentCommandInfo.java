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

package org.apache.inlong.manager.common.pojo.agent;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.List;
import lombok.Data;

/**
 * File agent operation result
 */
@ApiModel("File agent operation result")
@Data
public class FileAgentCommandInfo {

    @ApiModelProperty(value = "agentIp")
    private String agentIp;

    @ApiModelProperty(value = "agent result details")
    private List<CommandInfoBean> commandInfo;

    @ApiModel("File agent command info")
    @Data
    public static class CommandInfoBean {

        @ApiModelProperty(value = "operation result")
        private int commandResult;

        @ApiModelProperty(value = "Command issuance time")
        private long deliveryTime;

        @ApiModelProperty(value = "task id")
        private int taskId;

        @ApiModelProperty(value = "operation type")
        private int op;

        @ApiModelProperty(value = "data time")
        private String dataTime;

        @ApiModelProperty(value = "operation id")
        private int id;

    }
}
