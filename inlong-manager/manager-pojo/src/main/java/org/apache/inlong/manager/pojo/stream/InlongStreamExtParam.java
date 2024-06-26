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

package org.apache.inlong.manager.pojo.stream;

import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonUtils;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

/**
 * Extended params, will be saved as JSON string
 */
@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ApiModel("Inlong stream ext param info")
public class InlongStreamExtParam implements Serializable {

    @ApiModelProperty(value = "Whether to ignore the parse errors of field value")
    private Boolean ignoreParseError;

    @ApiModelProperty(value = "Kv separator")
    private String kvSeparator;

    @ApiModelProperty(value = "Line separator")
    private String lineSeparator;

    @ApiModelProperty(value = "If use extended fields")
    private Boolean useExtendedFields = false;

    @ApiModelProperty(value = "Predefined fields")
    private String predefinedFields;

    @ApiModelProperty("The multiple enable of sink")
    private Boolean sinkMultipleEnable = false;

    @ApiModelProperty(value = "Extended field size")
    private Integer extendedFieldSize = 0;

    /**
     * Pack extended attributes into ExtParams
     *
     * @param request the request
     * @return the packed extParams
     */
    public static String packExtParams(InlongStreamRequest request) {
        InlongStreamExtParam extParam = CommonBeanUtils.copyProperties(request, InlongStreamExtParam::new,
                true);
        return JsonUtils.toJsonString(extParam);
    }

    /**
     * Unpack extended attributes from {@link InlongStreamExtInfo}, will remove target attributes from it.
     *
     * @param extParams the extParams value load from db
     * @param targetObject the targetObject with to fill up
     */
    public static void unpackExtParams(
            String extParams,
            Object targetObject) {
        if (StringUtils.isNotBlank(extParams)) {
            InlongStreamExtParam inlongStreamExtParam = JsonUtils.parseObject(extParams, InlongStreamExtParam.class);
            if (inlongStreamExtParam != null) {
                CommonBeanUtils.copyProperties(inlongStreamExtParam, targetObject, true);
            }
        }
    }

    /**
     * Expand extParam filed, and fill in {@link InlongStreamInfo}
     *
     * @param streamInfo the InlongStreamInfo need to filled
     */
    public static void unpackExtParams(InlongStreamInfo streamInfo) {
        unpackExtParams(streamInfo.getExtParams(), streamInfo);
    }
}
