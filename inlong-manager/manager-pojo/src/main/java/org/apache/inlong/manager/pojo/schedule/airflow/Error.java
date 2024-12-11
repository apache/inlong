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

package org.apache.inlong.manager.pojo.schedule.airflow;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.math.BigDecimal;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
@ApiModel(description = "[RFC7807](https://tools.ietf.org/html/rfc7807) compliant response. ")
public class Error {

    @JsonProperty("detail")
    @ApiModelProperty("Error Details.")
    private String detail;

    @JsonProperty("instance")
    @ApiModelProperty("Error of the instance.")
    private String instance;

    @JsonProperty("status")
    @ApiModelProperty("Error of the status.")
    private BigDecimal status;

    @JsonProperty("title")
    @ApiModelProperty("Error of the title.")
    private String title;

    @JsonProperty("type")
    @ApiModelProperty("Error of the type.")
    private String type;
}
