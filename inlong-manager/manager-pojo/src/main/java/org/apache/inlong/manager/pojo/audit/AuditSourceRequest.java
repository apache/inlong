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

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;

import javax.validation.constraints.NotBlank;

import java.util.Date;

@Data
@EqualsAndHashCode(callSuper = false)
@ApiModel("Audit source request")
public class AuditSourceRequest {

    @ApiModelProperty(name = "id")
    Integer id;

    @ApiModelProperty(value = "Old url that will be offline. It can be null.", name = "oldUrl")
    private String oldUrl;

    @NotBlank
    @ApiModelProperty(value = "MYSQL, CLICKHOUSE or ELASTICSEARCH", name = "sourceType", required = true)
    private String sourceType;

    @NotBlank
    @ApiModelProperty(name = "sourceUrl", required = true)
    private String sourceUrl;

    @ApiModelProperty(name = "authEnable")
    Integer authEnable;

    @NotBlank
    @ApiModelProperty(name = "userName", required = true)
    private String userName;

    @NotBlank
    @ApiModelProperty(name = "password", required = true)
    String password;

    @NotBlank
    @ApiModelProperty(name = "creator")
    String creator;

    @NotBlank
    @ApiModelProperty(name = "createTime")
    Date createTime;

    @NotBlank
    @ApiModelProperty(name = "modifier", required = true)
    String modifier;

    @NotBlank
    @ApiModelProperty(name = "modifyTime")
    Date modifyTime;

    @ApiModelProperty(name = "version")
    Integer version;

    public AuditSourceRequest() {
    }

    public AuditSourceRequest(String oldUrl, String sourceType, String sourceUrl, Integer authEnable, String userName,
            String password, String modifier) {
        this.oldUrl = oldUrl;
        this.sourceType = sourceType;
        this.sourceUrl = sourceUrl;
        this.authEnable = authEnable;
        this.userName = userName;
        this.password = password;
        this.modifier = modifier;
    }
}
