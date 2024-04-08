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

package org.apache.inlong.manager.pojo.module;

import org.apache.inlong.manager.common.validation.UpdateValidation;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import javax.validation.constraints.NotNull;

/**
 * Module request.
 */
@Data
@ApiModel("Module request")
public class ModuleRequest {

    @ApiModelProperty(value = "Primary key")
    @NotNull(groups = UpdateValidation.class)
    private Integer id;

    @ApiModelProperty("Module name")
    private String name;

    @ApiModelProperty("Module type")
    private String type;

    @ApiModelProperty("Module version")
    private String version;

    @ApiModelProperty("Start command")
    private String startCommand;

    @ApiModelProperty("Stop command")
    private String stopCommand;

    @ApiModelProperty("Check command")
    private String checkCommand;

    @ApiModelProperty("Install command")
    private String installCommand;

    @ApiModelProperty("Uninstall command")
    private String uninstallCommand;

    @ApiModelProperty("Package id")
    private Integer packageId;

    @ApiModelProperty("Extended params")
    private String extParams;

    @ApiModelProperty(value = "Current user", hidden = true)
    private String currentUser;

}
