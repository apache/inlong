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

package org.apache.inlong.manager.pojo.sink;

import org.apache.inlong.manager.common.validation.SaveValidation;
import org.apache.inlong.manager.common.validation.UpdateByKeyValidation;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.hibernate.validator.constraints.Length;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Pattern;

import java.util.List;

/**
 * Request for Dirty data
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ApiModel("Transform parse request")
public class TransformParseRequest {

    @ApiModelProperty("Inlong group id")
    @NotBlank(groups = {SaveValidation.class, UpdateByKeyValidation.class}, message = "inlongGroupId cannot be blank")
    @Length(min = 4, max = 256, message = "length must be between 4 and 200")
    @Pattern(regexp = "^[a-zA-Z0-9_.-]{4,200}$", message = "only supports letters, numbers, '.', '-', or '_'")
    private String inlongGroupId;

    @ApiModelProperty("Inlong stream id")
    @NotBlank(groups = {SaveValidation.class, UpdateByKeyValidation.class}, message = "inlongStreamId cannot be blank")
    @Length(min = 1, max = 256, message = "inlongStreamId length must be between 1 and 200")
    @Pattern(regexp = "^[a-zA-Z0-9_.-]{1,200}$", message = "inlongStreamId only supports letters, numbers, '.', '-', or '_'")
    private String inlongStreamId;

    @ApiModelProperty("Transform sql")
    private String transformSql;

    @ApiModelProperty("Data")
    private String data;

    @ApiModelProperty("Sink field list")
    private List<SinkField> sinkFieldList;

}
