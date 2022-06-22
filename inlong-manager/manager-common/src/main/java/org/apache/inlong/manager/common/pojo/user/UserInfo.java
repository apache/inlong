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

package org.apache.inlong.manager.common.pojo.user;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.inlong.manager.common.enums.UserTypeEnum;
import org.apache.inlong.manager.common.validation.InEnumInt;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

/**
 * User info, including username, password, etc.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("User info")
public class UserInfo {

    private Integer id;

    /**
     * user type
     * {@link UserTypeEnum}
     */
    @NotNull
    @InEnumInt(UserTypeEnum.class)
    @ApiModelProperty(value = "type: 0 - manager, 1 - operator", required = true)
    private Integer type;

    @NotBlank
    @ApiModelProperty(value = "username", required = true)
    private String username;

    @NotBlank
    @ApiModelProperty(value = "password", required = true)
    private String password;

    @NotNull
    @Min(1)
    @ApiModelProperty(value = "valid days", required = true)
    private Integer validDays;

}
