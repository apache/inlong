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
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@ApiModel(description = "Full representation of the connection.")
public class AirflowConnection {

    @JsonProperty("connection_id")
    @ApiModelProperty("The connection ID.")
    private String connectionId;

    @JsonProperty("conn_type")
    @ApiModelProperty("The connection type.")
    private String connType;

    @JsonProperty("description")
    @ApiModelProperty("The description of the connection.")
    private String description;

    @JsonProperty("host")
    @ApiModelProperty("Host of the connection.")
    private String host;

    @JsonProperty("login")
    @ApiModelProperty("Login of the connection.")
    private String login;

    @JsonProperty("schema")
    @ApiModelProperty("Schema of the connection.")
    private String schema;

    @JsonProperty("port")
    @ApiModelProperty("Port of the connection.")
    private Integer port;

    @JsonProperty("password")
    @ApiModelProperty("Password of the connection.")
    private String password;

    @JsonProperty("extra")
    @ApiModelProperty("Additional information description of the connection.")
    private String extra;
}
