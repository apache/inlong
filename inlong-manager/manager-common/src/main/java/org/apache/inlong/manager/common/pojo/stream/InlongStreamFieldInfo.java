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

package org.apache.inlong.manager.common.pojo.stream;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

/**
 * Inlong stream field info
 */
@Data
@ApiModel("Inlong stream field info")
public class InlongStreamFieldInfo {

    private Integer id;

    @ApiModelProperty(value = "inlong group id", required = true)
    private String inlongGroupId;

    @ApiModelProperty(value = "inlong stream id", required = true)
    private String inlongStreamId;

    @ApiModelProperty(value = "is predefined field, 1: ye, 0: no")
    private Integer isPredefinedField;

    @ApiModelProperty(value = "Field name")
    private String fieldName;

    @ApiModelProperty(value = "Field value")
    private String fieldValue;

    @ApiModelProperty(value = "value expression of predefined field")
    private String preExpression;

    private String fieldType;

    private String fieldComment;

    @ApiModelProperty(value = "field rank num")
    private Short rankNum;

    @ApiModelProperty(value = "is deleted? 0: deleted, > 0: not deleted")
    private Integer isDeleted = 0;

}