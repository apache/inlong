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

package org.apache.inlong.manager.pojo.source;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.hibernate.validator.constraints.Length;

import javax.validation.constraints.NotBlank;

import java.util.List;

/**
 * Data add task information
 */
@Data
@ApiModel("Data add task request")
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, visible = true, property = "sourceType")
public class DataAddTaskRequest {

    @ApiModelProperty(value = "Group Id")
    @NotBlank(message = "inlongGroupId cannot be blank")
    private String groupId;

    @ApiModelProperty(value = "Source ID", hidden = true)
    private Integer sourceId;

    @ApiModelProperty(value = "Agent ip List")
    private List<String> agentIpList;

    @ApiModelProperty("Source type, including: FILE, KAFKA, etc.")
    @NotBlank(message = "sourceType cannot be blank")
    @Length(min = 1, max = 20, message = "length must be between 1 and 20")
    private String sourceType;

    @ApiModelProperty(value = "Audit version", hidden = true)
    private String auditVersion;

}
