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

import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.pojo.sink.BaseStreamSink;

import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotNull;

/**
 * Sink info of Cloud log service
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ClsSinkDTO extends BaseStreamSink {

    @ApiModelProperty("Cloud log service topic id")
    private String topicId;

    @ApiModelProperty("Cloud log service topic name")
    private String topicName;

    @ApiModelProperty("Cloud log service topic storage duration")
    private Integer storageDuration;

    @ApiModelProperty("Cloud log service tag name")
    private String tag;

    @ApiModelProperty("Cloud log service index tokenizer")
    private String tokenizer;

    @ApiModelProperty("contentOffset")
    private Integer contentOffset = 0;

    @ApiModelProperty("fieldOffset")
    private Integer fieldOffset;

    @ApiModelProperty("separator")
    private String separator;

    /**
     * Get the dto instance from the request
     */
    public static ClsSinkDTO getFromRequest(ClsSinkRequest request, String extParams) {
        ClsSinkDTO dto = StringUtils.isNotBlank(extParams)
                ? ClsSinkDTO.getFromJson(extParams)
                : new ClsSinkDTO();
        return CommonBeanUtils.copyProperties(request, dto, true);
    }

    public static ClsSinkDTO getFromJson(@NotNull String extParams) {
        try {
            return JsonUtils.parseObject(extParams, ClsSinkDTO.class);
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SINK_INFO_INCORRECT,
                    String.format("parse extParams of CLS SinkDTO failure: %s", e.getMessage()));
        }
    }
}
