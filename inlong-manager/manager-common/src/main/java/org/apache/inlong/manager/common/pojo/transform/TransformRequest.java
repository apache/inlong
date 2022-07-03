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

package org.apache.inlong.manager.common.pojo.transform;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.apache.inlong.manager.common.pojo.stream.StreamField;

import javax.validation.constraints.NotBlank;
import java.util.List;

/**
 * Request of transform
 */
@Data
@ApiModel("Request of stream transform")
public class TransformRequest {

    private int id;

    @NotBlank(message = "inlongGroupId cannot be blank")
    @ApiModelProperty("Inlong group id")
    private String inlongGroupId;

    @NotBlank(message = "inlongStreamId cannot be blank")
    @ApiModelProperty("Inlong stream id")
    private String inlongStreamId;

    @NotBlank(message = "transformName cannot be blank")
    @ApiModelProperty("Transform name, unique in one stream")
    private String transformName;

    @NotBlank(message = "transformType cannot be blank")
    @ApiModelProperty("Transform type, including: splitter, filter, joiner, etc.")
    private String transformType;

    @NotBlank(message = "preNodeNames cannot be blank")
    @ApiModelProperty("Pre node names of transform in this stream, join by ','")
    private String preNodeNames;

    @NotBlank(message = "postNodeNames cannot be blank")
    @ApiModelProperty("Post node names of transform in this stream, join by ','")
    private String postNodeNames;

    @NotBlank(message = "transformDefinition cannot be blank")
    @ApiModelProperty("Transform definition in json type")
    private String transformDefinition;

    @ApiModelProperty("Version of transform")
    private Integer version;

    @ApiModelProperty(value = "Field list")
    private List<StreamField> fieldList;
}

