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

package org.apache.inlong.manager.pojo.stream;

import org.apache.inlong.manager.common.validation.SaveValidation;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import javax.validation.constraints.NotBlank;

import java.util.Date;
import java.util.List;

/**
 * Inlong template info.
 */
@Data
@ApiModel("Inlong template info")
public class TemplateInfo {

    @ApiModelProperty(value = "Primary key")
    private Integer id;

    @ApiModelProperty(value = "Template name")
    @NotBlank(groups = SaveValidation.class, message = "template name cannot be blank")
    private String name;

    @ApiModelProperty(value = "Name of responsible person, separated by commas")
    private String inCharges;

    @ApiModelProperty(value = "Visible range for template")
    private String visibleRange;

    @ApiModelProperty(value = "Field list")
    private List<TemplateField> fieldList;

    @ApiModelProperty(value = "Bind tenants")
    private List<String> tenantList;

    @ApiModelProperty(value = "Name of in creator")
    private String creator;

    @ApiModelProperty(value = "Name of in modifier")
    private String modifier;

    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ", timezone = "GMT+8")
    private Date createTime;

    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ", timezone = "GMT+8")
    private Date modifyTime;

    @ApiModelProperty(value = "Version number")
    private Integer version;

}
