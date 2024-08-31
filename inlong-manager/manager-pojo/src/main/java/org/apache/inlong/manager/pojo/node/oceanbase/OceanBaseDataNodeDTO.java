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

package org.apache.inlong.manager.pojo.node.oceanbase;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

/**
 * OceanBase data node info
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("OceanBase data node info")
public class OceanBaseDataNodeDTO {

    private static final Logger LOGGER = LoggerFactory.getLogger(OceanBaseDataNodeDTO.class);
    private static final String OCEANBASE_JDBC_PREFIX = "jdbc:oceanbase://";

    @ApiModelProperty("URL of backup DB server")
    private String backupUrl;

    /**
     * Get the dto instance from the request
     */
    public static OceanBaseDataNodeDTO getFromRequest(OceanBaseDataNodeRequest request, String extParams) {
        OceanBaseDataNodeDTO dto = StringUtils.isNotBlank(extParams)
                ? OceanBaseDataNodeDTO.getFromJson(extParams)
                : new OceanBaseDataNodeDTO();
        return CommonBeanUtils.copyProperties(request, dto, true);
    }

    /**
     * Get the dto instance from the JSON string.
     */
    public static OceanBaseDataNodeDTO getFromJson(@NotNull String extParams) {
        try {
            return JsonUtils.parseObject(extParams, OceanBaseDataNodeDTO.class);
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.CLUSTER_INFO_INCORRECT,
                    String.format("Failed to parse extParams for OceanBase node: %s", e.getMessage()));
        }
    }

    /**
     * Convert ip:post to jdbcurl.
     */
    public static String convertToJdbcurl(String url) {
        String jdbcUrl = url;
        if (StringUtils.isNotBlank(jdbcUrl) && !jdbcUrl.startsWith(OCEANBASE_JDBC_PREFIX)) {
            jdbcUrl = OCEANBASE_JDBC_PREFIX + jdbcUrl;
        }
        return jdbcUrl;
    }
}
