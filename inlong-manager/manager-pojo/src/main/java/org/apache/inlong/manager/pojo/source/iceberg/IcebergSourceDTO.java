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

package org.apache.inlong.manager.pojo.source.iceberg;

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

import java.util.Map;

/**
 * Iceberg source info
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class IcebergSourceDTO {

    @ApiModelProperty("Iceberg data uri")
    private String uri;

    @ApiModelProperty("Iceberg data warehouse dir")
    private String warehouse;

    @ApiModelProperty("Database name")
    private String database;

    @ApiModelProperty("Table name")
    private String tableName;

    @ApiModelProperty("PrimaryKey")
    private String primaryKey;

    @ApiModelProperty("Properties for iceberg")
    private Map<String, Object> properties;

    /**
     * Get the dto instance from the request
     */
    public static IcebergSourceDTO getFromRequest(IcebergSourceRequest request, String extParams) {
        IcebergSourceDTO dto = StringUtils.isNotBlank(extParams)
                ? IcebergSourceDTO.getFromJson(extParams)
                : new IcebergSourceDTO();
        return CommonBeanUtils.copyProperties(request, dto, true);
    }

    /**
     * Get the dto instance from the JSON string
     */
    public static IcebergSourceDTO getFromJson(@NotNull String extParams) {
        try {
            return JsonUtils.parseObject(extParams, IcebergSourceDTO.class);
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT,
                    String.format("parse extParams of IcebergSource failure: %s", e.getMessage()));
        }
    }

}
