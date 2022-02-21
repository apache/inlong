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

package org.apache.inlong.manager.client.api;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.Map;
import lombok.Data;
import org.apache.inlong.manager.client.api.auth.Authentication;

@Data
@ApiModel("Base configuration for flink cluster")
public class FlinkSortBaseConf extends SortBaseConf {

    @ApiModelProperty(value = "Sort type")
    private SortType type = SortType.FLINK;

    @ApiModelProperty("Authentication")
    private Authentication authentication;

    @ApiModelProperty("ServiceUrl for flink cluster, for example:ap-beijing|ap-chengdu|ap-chongqing")
    private String serviceUrl;

    @ApiModelProperty("Region for flink cluster, for example:ap-beijing|ap-chengdu|ap-chongqing or null if not exists")
    private String region;

    @ApiModelProperty("Other properties if need")
    private Map<String, String> properties;
}
