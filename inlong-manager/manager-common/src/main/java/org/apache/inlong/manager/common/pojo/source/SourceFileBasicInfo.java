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
import lombok.Data;

import java.util.Date;

/**
 * Basic information of file source
 */
@Deprecated
@Data
@ApiModel("Basic information of file source")
public class SourceFileBasicInfo {

    private Integer id;

    @ApiModelProperty(value = "Inlong group id")
    private String inlongGroupId;

    @ApiModelProperty(value = "Inlong stream id")
    private String inlongStreamId;

    @ApiModelProperty(value = "Is a hybrid source, 0: no, 1: yes")
    private Integer isHybridSource;

    @ApiModelProperty(value = "Time offset")
    private Integer dateOffset;

    @ApiModelProperty(value = "Time offset unit, H, D")
    private String dateOffsetUnit;

    @ApiModelProperty(value = "File generation scroll mode, press H or D to scroll")
    private String fileRollingType;

    @ApiModelProperty(value = "Unit: MB, default 120MB")
    private Integer uploadMaxSize = 120;

    @ApiModelProperty(value = "need compress? 0: not compress, 1: compress")
    private Integer needCompress = 1;

    @ApiModelProperty(value = "is deleted? 0: deleted, 1: not deleted")
    private Integer isDeleted = 0;

    private String creator;

    private String modifier;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date createTime;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date modifyTime;

    @ApiModelProperty(value = "temp view")
    private String tempView;

}