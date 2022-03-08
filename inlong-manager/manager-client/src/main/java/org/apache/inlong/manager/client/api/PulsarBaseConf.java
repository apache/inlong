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
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.inlong.manager.client.api.auth.Authentication;
import org.apache.inlong.manager.common.enums.MqType;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ApiModel("Base configuration for Pulsar")
public class PulsarBaseConf extends MqBaseConf {

    @ApiModelProperty("Message queue type")
    private MqType type = MqType.PULSAR;

    @ApiModelProperty("Pulsar admin URL")
    private String pulsarAdminUrl;

    @ApiModelProperty("Pulsar service URL")
    private String pulsarServiceUrl;

    @ApiModelProperty("Pulsar namespace")
    private String namespace;

    @ApiModelProperty("Authentication")
    private Authentication authentication;

    @ApiModelProperty(value = "Ledger's number of writable nodes")
    private Integer ensemble = 3;

    @ApiModelProperty(value = "Ledger's number of copies")
    private Integer writeQuorum = 3;

    @ApiModelProperty(value = "Number of responses requested")
    private Integer ackQuorum = 2;

    @ApiModelProperty(value = "Message storage time")
    private Integer retentionTime = 72;

    @ApiModelProperty(value = "The unit of the message storage time")
    private String retentionTimeUnit = "hours";

    @ApiModelProperty(value = "Message time-to-live duration")
    private Integer ttl = 24;

    @ApiModelProperty(value = "The unit of message's time-to-live duration")
    private String ttlUnit = "hours";

    @ApiModelProperty(value = "Message size")
    private Integer retentionSize = -1;

    @ApiModelProperty(value = "The unit of message size")
    private String retentionSizeUnit = "MB";
}
