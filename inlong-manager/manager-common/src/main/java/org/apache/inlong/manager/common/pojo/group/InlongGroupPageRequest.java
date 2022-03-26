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

package org.apache.inlong.manager.common.pojo.group;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.inlong.manager.common.beans.PageRequest;

import java.util.List;

/**
 * Inlong group query request
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ApiModel("Inlong group query request")
public class InlongGroupPageRequest extends PageRequest {

    @ApiModelProperty(value = "Keywords")
    private String keyWord;

    @ApiModelProperty(value = "Inlong group name list")
    private List<String> nameList;

    @ApiModelProperty(value = "Inlong group id list")
    private List<String> groupIdList;

    @ApiModelProperty(value = "MQ resource object")
    private String middlewareType;

    @ApiModelProperty(value = "Status")
    private Integer status;

    @ApiModelProperty(value = "Current user", hidden = true)
    private String currentUser;

    @ApiModelProperty(value = "If list streamSource for group", hidden = true)
    private boolean listSources = false;
}
