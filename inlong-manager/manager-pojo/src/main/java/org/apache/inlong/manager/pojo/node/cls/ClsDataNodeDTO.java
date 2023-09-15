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

package org.apache.inlong.manager.pojo.node.cls;

import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonUtils;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotNull;

/**
 * Cloud log service data node info
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Cloud log service data node info")
public class ClsDataNodeDTO {

    @ApiModelProperty("Cloud log service master account")
    private String mainAccountId;

    @ApiModelProperty("Cloud log service sub account")
    private String subAccountId;

    @ApiModelProperty("Cloud log service send api secretKey")
    private String sendSecretKey;

    @ApiModelProperty("Cloud log service send api secretId")
    private String sendSecretId;

    @ApiModelProperty("Cloud log service manage api secretKey")
    private String manageSecretKey;

    @ApiModelProperty("Cloud log service manage api secretId")
    private String manageSecretId;

    @ApiModelProperty("Cloud log service endpoint")
    private String endpoint;

    @ApiModelProperty("Cloud log service region")
    private String region;

    @ApiModelProperty("Cloud log service log set id")
    private String logSetId;

    /**
     * Get the dto instance from the request
     */
    public static ClsDataNodeDTO getFromRequest(ClsDataNodeRequest request, String extParams) {
        ClsDataNodeDTO dto = StringUtils.isNotBlank(extParams)
                ? ClsDataNodeDTO.getFromJson(extParams)
                : new ClsDataNodeDTO();
        return CommonBeanUtils.copyProperties(request, dto, true);
    }

    /**
     * Get the dto instance from the JSON string.
     */
    public static ClsDataNodeDTO getFromJson(@NotNull String extParams) {
        try {
            return JsonUtils.parseObject(extParams, ClsDataNodeDTO.class);
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.GROUP_INFO_INCORRECT,
                    String.format("Failed to parse extParams for Cloud log service node: %s", e.getMessage()));
        }
    }

}
