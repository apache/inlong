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

import org.apache.inlong.common.enums.MessageWrapType;
import org.apache.inlong.manager.common.consts.SourceType;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonTypeDefine;
import org.apache.inlong.manager.pojo.source.SourceRequest;
import org.apache.inlong.manager.pojo.source.StreamSource;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.util.TreeSet;

/**
 * The TubeMQ source info
 */
@Data
@SuperBuilder
@AllArgsConstructor
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@ApiModel(value = "TubeMQ source info")
@JsonTypeDefine(value = SourceType.TUBEMQ)
public class TubeMQSource extends StreamSource {

    @ApiModelProperty("Master RPC of the TubeMQ")
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

    /**
     * The TubeMQ consumers use this streamId set to filter records reading from server.
     */
    @ApiModelProperty("StreamId of the TubeMQ")
    private TreeSet<String> streamId;

    @ApiModelProperty(value = "The message body wrap  wrap type, including: RAW, INLONG_MSG_V0, INLONG_MSG_V1, etc")
    @Builder.Default
    private String wrapType = MessageWrapType.INLONG_MSG_V0.getName();

    public TubeMQSource() {
        this.setSourceType(SourceType.TUBEMQ);
    }

    @Override
    public SourceRequest genSourceRequest() {
        return CommonBeanUtils.copyProperties(this, TubeMQSourceRequest::new);
    }

}
