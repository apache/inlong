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

import com.fasterxml.jackson.annotation.JsonFormat;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotNull;

import java.util.Date;

/**
 * Package response
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Pacekag response")
public class PackageResponse {

    @ApiModelProperty(value = "Primary key")
    @NotNull(groups = UpdateValidation.class)
    private Integer id;

    @ApiModelProperty(value = "Md5")
    private String md5;

    @ApiModelProperty("Package type ")
    private String type;

    @ApiModelProperty(value = "File name")
    private String fileName;

    @ApiModelProperty(value = "Download url")
    private String downloadUrl;

    @ApiModelProperty(value = "Storage path")
    private String storagePath;

    @ApiModelProperty(value = "Name of in creator")
    private String creator;

    @ApiModelProperty(value = "Name of in modifier")
    private String modifier;

    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ", timezone = "GMT+8")
    private Date createTime;

    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ", timezone = "GMT+8")
    private Date modifyTime;

}
