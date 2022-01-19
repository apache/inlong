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

package org.apache.inlong.manager.common.pojo.sort;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
@ApiModel("sort cluster config")
public class SortClusterConfigResponse {

    public static final int RESP_CODE_SUCC = 0;

    public static final int RESP_CODE_NO_UPDATE = 1;

    public static final int RESP_CODE_FAIL = -1;

    public static final int RESP_CODE_REQ_PARAMS_ERROR = -101;

    @ApiModelProperty(value = "result msg")
    String msg;

    @ApiModelProperty(value = "result code")
    int code;

    @ApiModelProperty(value = "md5")
    String md5;

    @ApiModelProperty(value = "sort cluster config")
    SortClusterConfig clusterConfig;

    @Data
    public static class SortClusterConfig {

        @ApiModelProperty("clusterName")
        String clusterName;

        @ApiModelProperty("SortTaskConfig")
        List<SortTaskConfig> sortTaskConfig;
    }

    @Data
    public static class SortTaskConfig {

        @ApiModelProperty("name")
        String name;

        @ApiModelProperty("type")
        String type;

        @ApiModelProperty("idParams")
        List<Map<String, String>> idParams;

        @ApiModelProperty("sinkParams")
        Map<String, String> sinkParams;
    }
}
