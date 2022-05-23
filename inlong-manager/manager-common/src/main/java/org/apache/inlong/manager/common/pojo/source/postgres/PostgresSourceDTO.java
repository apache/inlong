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

package org.apache.inlong.manager.common.pojo.source.postgres;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.ApiModelProperty;
import java.util.List;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;

/**
 * Postgres source info
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PostgresSourceDTO {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(); // thread safe

    @ApiModelProperty("Username of the DB server")
    private String username;

    @ApiModelProperty("Password of the DB server")
    private String password;

    @ApiModelProperty("Hostname of the DB server")
    private String hostname;

    @ApiModelProperty("Exposed port of the DB server")
    private int port;

    @ApiModelProperty("schema")
    private String schema;

    @ApiModelProperty(value = "database name")
    private String database;

    @ApiModelProperty(value = "List of tables to be collected")
    private List<String> tableNameList;

    @ApiModelProperty(value = "Primary key must be shared by all tables", required = false)
    private String primaryKey;

    @ApiModelProperty(value = "decoding pulgin name")
    private String decodingPluginName;

    /**
     * Get the dto instance from the request
     */
    public static PostgresSourceDTO getFromRequest(PostgresSourceRequest request) {
        return PostgresSourceDTO.builder()
                .username(request.getUsername())
                .password(request.getPassword())
                .hostname(request.getHostname())
                .port(request.getPort())
                .schema(request.getSchema())
                .database(request.getDatabase())
                .tableNameList(request.getTableNameList())
                .primaryKey(request.getPrimaryKey())
                .decodingPluginName(request.getDecodingPluginName())
                .build();
    }

    public static PostgresSourceDTO getFromJson(@NotNull String extParams) {
        try {
            OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            return OBJECT_MAPPER.readValue(extParams, PostgresSourceDTO.class);
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT.getMessage());
        }
    }

}
