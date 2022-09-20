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

package org.apache.inlong.manager.pojo.node.mysql;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

/**
 * MySQL data node info
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("MySQL data node info")
public class MySQLDataNodeDTO {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLDataNodeDTO.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(); // thread safe

    @ApiModelProperty("The url of the backup DB service")
    private String backupUrl;

    @ApiModelProperty("Include region ID")
    private Integer isRegionId;

    /**
     * Get the dto instance from the request
     */
    public static MySQLDataNodeDTO getFromRequest(MySQLDataNodeRequest request) throws Exception {
        return MySQLDataNodeDTO.builder()
                .backupUrl(request.getBackupUrl())
                .isRegionId(request.getIsRegionId())
                .build();
    }

    /**
     * Get the dto instance from the JSON string.
     */
    public static MySQLDataNodeDTO getFromJson(@NotNull String extParams) {
        try {
            OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            return OBJECT_MAPPER.readValue(extParams, MySQLDataNodeDTO.class);
        } catch (Exception e) {
            LOGGER.error("Failed to extract additional parameters for MySQL data node: ", e);
            throw new BusinessException(ErrorCodeEnum.GROUP_INFO_INCORRECT.getMessage());
        }
    }
}
