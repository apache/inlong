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

package org.apache.inlong.manager.common.pojo.source;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamFieldInfo;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * Request of source
 */
@Data
@ApiModel("Request of source")
@JsonTypeInfo(use = Id.NAME, visible = true, property = "sourceType")
public class SourceRequest {

    private Integer id;

    @NotNull
    @ApiModelProperty("Inlong group id")
    private String inlongGroupId;

    @NotNull
    @ApiModelProperty("Inlong stream id")
    private String inlongStreamId;

    @NotNull
    @ApiModelProperty("Source type, including: FILE, KAFKA, etc.")
    private String sourceType;

    @NotNull
    @ApiModelProperty("Source name, unique in one stream")
    private String sourceName;

    @ApiModelProperty("Mac uuid of the agent running the task")
    private String uuid;

    @ApiModelProperty("Id of the source server")
    private Integer serverId = 0;

    @ApiModelProperty("Id of the cluster that collected this source")
    private Integer clusterId;

    @ApiModelProperty("Serialization type, support: csv, json, canal, avro, etc")
    private String serializationType;

    @ApiModelProperty("Snapshot of the source task")
    private String snapshot;

    @ApiModelProperty("Version")
    private Integer version;

    @ApiModelProperty("Field list, only support when inlong group in light weight mode")
    private List<InlongStreamFieldInfo> fieldList;

}
