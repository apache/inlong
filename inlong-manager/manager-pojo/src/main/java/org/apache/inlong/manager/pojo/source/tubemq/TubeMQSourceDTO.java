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

import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonUtils;

import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotNull;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeSet;

/**
 * TubeMQ source info
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TubeMQSourceDTO {

    @ApiModelProperty("Master RPC of the TubeMQ,127.0.0.1:8715")
    private String masterRpc;

    @ApiModelProperty("Topic of the TubeMQ")
    private String topic;

    @ApiModelProperty("Group of the TubeMQ")
    private String consumeGroup;

    @ApiModelProperty("Session key of the TubeMQ")
    private String sessionKey;

    @ApiModelProperty(value = "Data encoding format: UTF-8, GBK")
    private String dataEncoding = StandardCharsets.UTF_8.toString();

    @ApiModelProperty(value = "Data separator")
    private String dataSeparator = String.valueOf((int) '|');

    @ApiModelProperty(value = "KV separator")
    private String kvSeparator;

    @ApiModelProperty(value = "Data field escape symbol")
    private String dataEscapeChar;

    @ApiModelProperty(value = "The message body wrap  wrap type, including: RAW, INLONG_MSG_V0, INLONG_MSG_V1, etc")
    private String wrapType;

    /**
     * The tubemq consumers use this streamId set to filter records reading from server.
     */
    @ApiModelProperty("streamId of the TubeMQ")
    private TreeSet<String> streamId;

    @ApiModelProperty("Properties for TubeMQ")
    private Map<String, Object> properties;

    /**
     * Get the dto instance from the request
     */
    public static TubeMQSourceDTO getFromRequest(TubeMQSourceRequest request, String extParams) {
        TubeMQSourceDTO dto = StringUtils.isNotBlank(extParams)
                ? TubeMQSourceDTO.getFromJson(extParams)
                : new TubeMQSourceDTO();
        return CommonBeanUtils.copyProperties(request, dto, true);
    }

    /**
     * Get the dto instance from the JSON string
     */
    public static TubeMQSourceDTO getFromJson(@NotNull String extParams) {
        try {
            return JsonUtils.parseObject(extParams, TubeMQSourceDTO.class);
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT,
                    String.format("Parse extParams of TubeMQSource failure: %s", e.getMessage()));
        }
    }
}
