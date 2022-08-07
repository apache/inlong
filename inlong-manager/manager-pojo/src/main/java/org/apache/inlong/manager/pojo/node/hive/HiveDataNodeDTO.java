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

package org.apache.inlong.manager.pojo.node.hive;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.AESUtils;

import javax.validation.constraints.NotNull;
import java.nio.charset.StandardCharsets;

/**
 * hive data node info
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Hive data node info")
public class HiveDataNodeDTO {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(); // thread safe

    @ApiModelProperty("Hive JDBC URL, such as jdbc:hive2://${ip}:${port}")
    private String jdbcUrl;

    @ApiModelProperty("Username of the Hive server")
    private String username;

    @ApiModelProperty("User password of the Hive server")
    private String password;

    @ApiModelProperty("Version for Hive, such as: 3.2.1")
    private String hiveVersion;

    @ApiModelProperty("Config directory of Hive on HDFS, needed by sort in light mode, must include hive-site.xml")
    private String hiveConfDir;

    @ApiModelProperty("Password encrypt version")
    private Integer encryptVersion;

    /**
     * Get the dto instance from the request
     */
    public static HiveDataNodeDTO getFromRequest(HiveDataNodeRequest request) throws Exception {

        Integer encryptVersion = AESUtils.getCurrentVersion(null);
        String passwd = null;
        if (StringUtils.isNotEmpty(request.getToken())) {
            passwd = AESUtils.encryptToString(request.getToken().getBytes(StandardCharsets.UTF_8),
                    encryptVersion);
        }
        return HiveDataNodeDTO.builder()
                .jdbcUrl(request.getJdbcUrl())
                .username(request.getUsername())
                .password(passwd)
                .hiveVersion(request.getHiveVersion())
                .hiveConfDir(request.getHiveConfDir())
                .encryptVersion(encryptVersion)
                .build();
    }

    /**
     * Get the dto instance from the JSON string.
     */
    public static HiveDataNodeDTO getFromJson(@NotNull String extParams) {
        try {
            OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            return OBJECT_MAPPER.readValue(extParams, HiveDataNodeDTO.class).decryptPassword();
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.GROUP_INFO_INCORRECT.getMessage());
        }
    }

    private HiveDataNodeDTO decryptPassword() throws Exception {
        if (StringUtils.isNotEmpty(this.password)) {
            byte[] passwordBytes = AESUtils.decryptAsString(this.password, this.encryptVersion);
            this.password = new String(passwordBytes, StandardCharsets.UTF_8);
        }
        return this;
    }
}
