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
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Cloud log service sink info
 */
@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@ApiModel(value = "Cloud log service sink info")
@JsonTypeDefine(value = SinkType.CLS)
public class ClsSink extends StreamSink {

    @ApiModelProperty("Cloud log service topic id")
    private String topicId;

    @ApiModelProperty("Cloud log service topic name")
    private String topicName;

    @ApiModelProperty("Cloud log service api send secretKey")
    private String sendSecretKey;

    @ApiModelProperty("Cloud log service api send secretId")
    private String sendSecretId;

    @ApiModelProperty("Cloud log service api manage secretKey")
    private String manageSecretKey;

    @ApiModelProperty("Cloud log service api manage secretId")
    private String manageSecretId;

    @ApiModelProperty("Cloud log service endpoint")
    private String endpoint;

    @ApiModelProperty("Cloud log service log set id")
    private String logSetId;

    @ApiModelProperty("Cloud log service tag name")
    private String tag;

    @ApiModelProperty("Cloud log service master account")
    private String mainAccountId;

    @ApiModelProperty("Cloud log service subAccount")
    private String subAccountId;

    @ApiModelProperty("Cloud log service index tokenizer")
    private String tokenizer;

    @ApiModelProperty("Cloud log service topic storage duration")
    private Integer storageDuration;

    @ApiModelProperty("contentOffset")
    private Integer contentOffset = 0;

    @ApiModelProperty("fieldOffset")
    private Integer fieldOffset;

    @ApiModelProperty("separator")
    private String separator;

    public ClsSink() {
        this.setSinkType(SinkType.CLS);
    }

    @Override
    public SinkRequest genSinkRequest() {
        return CommonBeanUtils.copyProperties(this, ClsSinkRequest::new);
    }
}
