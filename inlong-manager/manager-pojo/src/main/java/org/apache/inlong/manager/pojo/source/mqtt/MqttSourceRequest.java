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

package org.apache.inlong.manager.pojo.source.mqtt;

import org.apache.inlong.manager.common.consts.SourceType;
import org.apache.inlong.manager.common.util.JsonTypeDefine;
import org.apache.inlong.manager.pojo.source.SourceRequest;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@ApiModel(value = "MQTT source request")
@JsonTypeDefine(value = SourceType.MQTT)
public class MqttSourceRequest extends SourceRequest {

    @ApiModelProperty("ServerURI of the Mqtt server")
    private String serverURI;

    @ApiModelProperty("Username of the Mqtt server")
    private String username;

    @ApiModelProperty("Password of the Mqtt server")
    private String password;

    @ApiModelProperty("Topic of the Mqtt server")
    private String topic;

    @ApiModelProperty("Mqtt qos")
    private int qos = 1;

    @ApiModelProperty("Client Id")
    private String clientId;

    @ApiModelProperty("Mqtt version")
    private String mqttVersion;

    public MqttSourceRequest() {
        this.setSourceType(SourceType.MQTT);
    }

}
