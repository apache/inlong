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

package org.apache.inlong.manager.pojo.sort;

import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonUtils;

import io.swagger.annotations.ApiModel;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotNull;

/**
 * Base sort cluster DTO
 */
@Data
@ApiModel("Base sort cluster info")
public class BaseSortClusterDTO {

    /**
     * Get the dto instance from the request
     */
    public static BaseSortClusterDTO getFromRequest(BaseSortClusterRequest request, String extParams) throws Exception {
        BaseSortClusterDTO dto = StringUtils.isNotBlank(extParams)
                ? BaseSortClusterDTO.getFromJson(extParams)
                : new BaseSortClusterDTO();
        return CommonBeanUtils.copyProperties(request, dto, true);
    }

    /**
     * Get the dto instance from the JSON string.
     */
    public static BaseSortClusterDTO getFromJson(@NotNull String extParams) {
        return JsonUtils.parseObject(extParams, BaseSortClusterDTO.class);
    }
}
