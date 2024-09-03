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

package org.apache.inlong.manager.pojo.source.tubemq;

import org.apache.inlong.manager.common.consts.SourceType;
import org.apache.inlong.manager.common.util.JsonTypeDefine;
import org.apache.inlong.manager.pojo.source.SourceRequest;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.TreeSet;

/**
 * Request info of the TubeMQ source
 */
@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@ApiModel(value = "Request of the TubeMQ source")
@JsonTypeDefine(value = SourceType.TUBEMQ)
public class TubeMQSourceRequest extends SourceRequest {

    @ApiModelProperty("Master RPC of the TubeMQ,127.0.0.1:8715")
    private String masterRpc;

    @ApiModelProperty("Topic of the TubeMQ")
    private String topic;

    @ApiModelProperty("Group of the TubeMQ")
    private String consumeGroup;

    @ApiModelProperty("Session key of the TubeMQ")
    private String sessionKey;

    @ApiModelProperty(value = "Data encoding format: UTF-8, GBK")
    private String dataEncoding;

    @ApiModelProperty(value = "Data separator")
    private String dataSeparator;

    @ApiModelProperty(value = "KV separator")
    private String kvSeparator;

    @ApiModelProperty(value = "Data field escape symbol")
    private String dataEscapeChar;

    @ApiModelProperty(value = "The message body wrap  wrap type, including: RAW, INLONG_MSG_V0, INLONG_MSG_V1, etc")
    private String wrapType;

    /**
     * The TubeMQ consumers use this streamId set to filter records reading from server.
     */
    @ApiModelProperty("streamId of the TubeMQ")
    private TreeSet<String> streamId;

    public TubeMQSourceRequest() {
        this.setSourceType(SourceType.TUBEMQ);
    }

}
