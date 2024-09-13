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

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Query message request
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("query message request")
public class QueryMessageRequest {

    @ApiModelProperty(value = "Inlong group id")
    private String groupId;

    @ApiModelProperty(value = "Inlong stream id")
    private String streamId;

    @ApiModelProperty(value = "Message count")
    private Integer messageCount = 10;

    @ApiModelProperty(value = "Field name")
    private String fieldName;

    @ApiModelProperty(value = "Operation type, like !=, =, like")
    private String operationType;

    @ApiModelProperty(value = "TargetValue")
    private String targetValue;
}
