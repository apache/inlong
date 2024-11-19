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

package org.apache.inlong.manager.pojo.sink;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.JsonUtils;

import javax.validation.constraints.NotNull;

/**
 * The base parameter class of StreamSink, support user extend their own business params.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@ApiModel("Base info of stream sink")
public class BaseStreamSink {

    @ApiModelProperty("Enable data archiving")
    private Boolean enableDataArchiving;

    @ApiModelProperty("Transform sql")
    private String transformSql;

    @ApiModelProperty("Start consume time, yyyy-MM-dd HH:mm:ss format")
    private String startConsumeTime;

    @ApiModelProperty("Stop consume time, yyyy-MM-dd HH:mm:ss format")
    private String stopConsumeTime;

    public static BaseStreamSink getFromJson(@NotNull String extParams) {
        try {
            return JsonUtils.parseObject(extParams, BaseStreamSink.class);
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SINK_INFO_INCORRECT.getMessage() + ": " + e.getMessage());
        }
    }
}
