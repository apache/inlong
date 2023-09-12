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

package org.apache.inlong.manager.pojo.sink.cls;

import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonTypeDefine;
import org.apache.inlong.manager.pojo.sink.SinkRequest;
import org.apache.inlong.manager.pojo.sink.StreamSink;

import io.swagger.annotations.ApiModel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * Tencent cloud log service sink info
 */
@Data
@SuperBuilder
@AllArgsConstructor
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@ApiModel(value = "Tencent cloud log service sink info")
@JsonTypeDefine(value = SinkType.CLS)
public class TencentClsSink extends StreamSink {

    /**
     * Tencent cloud log service topic id
     */
    private String topicID;

    /**
     * Tencent cloud log service api send secretKey
     */
    private String sendSecretKey;

    /**
     * Tencent cloud log service api send secretId
     */
    private String sendSecretId;

    /**
     * Tencent cloud log service api manage secretKey
     */
    private String manageSecretKey;

    /**
     * Tencent cloud log service api manage secretId
     */
    private String manageSecretId;

    /**
     * Tencent cloud log service endpoint
     */
    private String endpoint;

    /**
     * Tencent cloud log service log set id
     */
    private String logSetID;

    /**
     * Tencent cloud log service tag name
     */
    private String tag;

    public TencentClsSink() {
        this.setSinkType(SinkType.CLS);
    }

    @Override
    public SinkRequest genSinkRequest() {
        return CommonBeanUtils.copyProperties(this, TencentClsSinkRequest::new);
    }
}
