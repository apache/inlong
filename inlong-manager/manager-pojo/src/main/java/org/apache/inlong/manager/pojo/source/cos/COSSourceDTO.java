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

package org.apache.inlong.manager.pojo.source.cos;

import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonUtils;

import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotNull;

import java.util.List;

/**
 * COS source information data transfer object
 */
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
@Slf4j
public class COSSourceDTO {

    @ApiModelProperty(value = "Path regex pattern for file, such as /a/b/*.txt", required = true)
    private String pattern;

    @ApiModelProperty("Cycle unit")
    private String cycleUnit = "D";

    @ApiModelProperty("Whether retry")
    private Boolean retry = false;;

    @ApiModelProperty("Column separator of data source ")
    private String dataSeparator;

    @ApiModelProperty(value = "Data start time")
    private String dataTimeFrom;

    @ApiModelProperty(value = "Data end time")
    private String dataTimeTo;

    @ApiModelProperty("TimeOffset for collection, "
            + "'1m' means from one minute after, '-1m' means from one minute before, "
            + "'1h' means from one hour after, '-1h' means from one minute before, "
            + "'1d' means from one day after, '-1d' means from one minute before, "
            + "Null or blank means from current timestamp")
    private String timeOffset;

    @ApiModelProperty("Max file count")
    private String maxFileCount;

    @ApiModelProperty(" Type of data result for column separator"
            + "         CSV format, set this parameter to a custom separator: , | : "
            + "         Json format, set this parameter to json ")
    private String contentStyle;

    @ApiModelProperty(value = "Audit version")
    private String auditVersion;

    @ApiModelProperty("filterStreams")
    private List<String> filterStreams;

    @ApiModelProperty(value = "COS bucket name")
    private String bucketName;

    @ApiModelProperty(value = "COS secret id")
    private String credentialsId;

    @ApiModelProperty(value = "COS secret key")
    private String credentialsKey;

    @ApiModelProperty(value = "COS region")
    private String region;

    public static COSSourceDTO getFromRequest(@NotNull COSSourceRequest cosSourceRequest, String extParams) {
        COSSourceDTO dto = StringUtils.isNotBlank(extParams)
                ? COSSourceDTO.getFromJson(extParams)
                : new COSSourceDTO();
        return CommonBeanUtils.copyProperties(cosSourceRequest, dto, true);
    }

    public static COSSourceDTO getFromJson(@NotNull String extParams) {
        try {
            log.info("teste extparmas={}", extParams);
            return JsonUtils.parseObject(extParams, COSSourceDTO.class);
        } catch (Exception e) {
            log.info("teste extparmas=eoor:", e);
            throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT,
                    String.format("parse extParams of COSSource failure: %s", e.getMessage()));
        }
    }

}
