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

package org.apache.inlong.manager.pojo.node.sql;

import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonUtils;

import io.swagger.annotations.ApiModel;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotNull;

/**
 * Sql data node info
 */
@Data
@NoArgsConstructor
@ApiModel("Sql data node info")
public class SqlDataNodeDTO {

    /**
     * Get the dto instance from the request
     */
    public static SqlDataNodeDTO getFromRequest(SqlDataNodeRequest request, String extParams) {
        SqlDataNodeDTO dto = StringUtils.isNotBlank(extParams)
                ? SqlDataNodeDTO.getFromJson(extParams)
                : new SqlDataNodeDTO();
        return CommonBeanUtils.copyProperties(request, dto, true);
    }

    /**
     * Get the dto instance from the JSON string.
     */
    public static SqlDataNodeDTO getFromJson(@NotNull String extParams) {
        try {
            return JsonUtils.parseObject(extParams, SqlDataNodeDTO.class);
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.CLUSTER_INFO_INCORRECT,
                    String.format("Failed to parse extParams for Sql node: %s", e.getMessage()));
        }
    }
}
