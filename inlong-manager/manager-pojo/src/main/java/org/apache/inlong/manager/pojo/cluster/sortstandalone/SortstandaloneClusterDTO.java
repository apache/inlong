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

package org.apache.inlong.manager.pojo.cluster.sortstandalone;

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

import java.util.Set;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Sortstandalone cluster info")
public class SortstandaloneClusterDTO {

    @ApiModelProperty(value = "Supported sink types")
    private Set<String> supportedSinkTypes;

    public static SortstandaloneClusterDTO getFromRequest(SortstandaloneClusterRequest request, String extParams) {
        SortstandaloneClusterDTO dto = StringUtils.isNotBlank(extParams)
                ? SortstandaloneClusterDTO.getFromJson(extParams)
                : new SortstandaloneClusterDTO();
        return CommonBeanUtils.copyProperties(request, dto, true);
    }

    /**
     * Get the dto instance from the JSON string.
     */
    public static SortstandaloneClusterDTO getFromJson(@NotNull String extParams) {
        try {
            return JsonUtils.parseObject(extParams, SortstandaloneClusterDTO.class);
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.CLUSTER_INFO_INCORRECT,
                    String.format("parse extParams of Sortstandalone Cluster failure: %s", e.getMessage()));
        }
    }
}
