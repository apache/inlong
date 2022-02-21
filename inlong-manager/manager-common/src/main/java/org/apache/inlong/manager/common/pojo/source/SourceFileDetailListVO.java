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

import com.fasterxml.jackson.annotation.JsonFormat;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.Date;
import lombok.Data;

/**
 * File source list
 */
@Data
@ApiModel("File source list")
public class SourceFileDetailListVO {

    @ApiModelProperty(value = "Primary key")
    private Integer id;

    @ApiModelProperty(value = "Inlong stream id")
    private String inlongStreamId;

    @ApiModelProperty(value = "Source IP address")
    private String ip;

    @ApiModelProperty(value = "Source port")
    private Integer port;

    @ApiModelProperty(value = "Issue type, including: SSH, TCS")
    private String issueType;

    @ApiModelProperty(value = "Access type, support: Agent, DataProxy Client, LoadProxy, the File can only be Agent")
    private String accessType = "Agent";

    @ApiModelProperty(value = "User name of the source IP host")
    private String username;

    @ApiModelProperty(value = "File path, supports regular matching")
    private String filePath;

    private Integer status;

    private String modifier;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date modifyTime;

    @ApiModelProperty(value = "Temporary view, string in JSON format")
    private String tempView;

    private String passWord;

}