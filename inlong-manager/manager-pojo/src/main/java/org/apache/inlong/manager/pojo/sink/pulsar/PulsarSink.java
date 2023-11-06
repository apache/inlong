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

package org.apache.inlong.manager.pojo.sink.pulsar;

import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonTypeDefine;
import org.apache.inlong.manager.pojo.sink.SinkRequest;
import org.apache.inlong.manager.pojo.sink.StreamSink;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Pulsar sink info
 */
@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@ApiModel(value = "Pulsar sink info")
@JsonTypeDefine(value = SinkType.PULSAR)
public class PulsarSink extends StreamSink {

    @ApiModelProperty("Pulsar service url")
    private String serviceUrl;

    @ApiModelProperty("Pulsar tenant")
    private String pulsarTenant;

    @ApiModelProperty("Pulsar namespace")
    private String namespace;

    @ApiModelProperty("Pulsar topic")
    private String topic;

    @ApiModelProperty("Pulsar token")
    private String token;

    @ApiModelProperty("Admin url")
    private String adminUrl;

    @ApiModelProperty("Pulsar partition number")
    private Integer partitionNum;

    public PulsarSink() {
        this.setSinkType(SinkType.PULSAR);
    }

    @Override
    public SinkRequest genSinkRequest() {
        return CommonBeanUtils.copyProperties(this, PulsarSinkRequest::new);
    }
}
