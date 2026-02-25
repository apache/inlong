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

package org.apache.inlong.manager.pojo.consume;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * Brief Message info for MQ
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Brief Message info for MQ")
public class BriefMQMessage {

    @ApiModelProperty(value = "Message index id")
    private Integer id;

    @ApiModelProperty(value = "Inlong group id")
    private String inlongGroupId;

    @ApiModelProperty(value = "Inlong stream id")
    private String inlongStreamId;

    @ApiModelProperty(value = "Message date")
    private Long dt;

    @ApiModelProperty(value = "Client ip")
    private String clientIp;

    @ApiModelProperty(value = "Message attribute")
    private String attribute;

    @ApiModelProperty(value = "Message header")
    private Map<String, String> headers;

    @ApiModelProperty(value = "Message body")
    private String body;

    @ApiModelProperty(value = "List of field info")
    private List<FieldInfo> fieldList;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FieldInfo {

        private String fieldName;

        private String fieldValue;

    }

}
