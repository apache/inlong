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

package org.apache.inlong.manager.pojo.sink.oceanbase;

import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.pojo.sink.BaseStreamSink;

import com.google.common.base.Strings;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * OceanBase sink info
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OceanBaseSinkDTO extends BaseStreamSink {

    // The protocol of using mysql in sink
    private static final Logger LOGGER = LoggerFactory.getLogger(OceanBaseSinkDTO.class);
    private static final String OCEANBASE_JDBC_PREFIX = "jdbc:oceanbase://";
    private static final String OCEANBASE_JDBC_PREFIX_CDC = "jdbc:mysql://";

    @ApiModelProperty("OceanBase JDBC URL, such as jdbc:oceanbase://host:port")
    private String jdbcUrl;

    @ApiModelProperty("Username for JDBC URL")
    private String username;

    @ApiModelProperty("User password")
    private String password;

    @ApiModelProperty("Target database name")
    private String databaseName;

    @ApiModelProperty("Target table name")
    private String tableName;

    @ApiModelProperty("Primary key")
    private String primaryKey;

    @ApiModelProperty("Properties for OceanBase")
    private Map<String, Object> properties;

    /**
     * Get the dto instance from the request
     *
     * @param request OceanBaseSinkRequest
     * @return {@link OceanBaseSinkDTO}
     * @apiNote The config here will be saved to the database, so filter sensitive params before saving.
     */
    public static OceanBaseSinkDTO getFromRequest(OceanBaseSinkRequest request, String extParams) {
        OceanBaseSinkDTO dto =
                StringUtils.isNotBlank(extParams) ? OceanBaseSinkDTO.getFromJson(extParams) : new OceanBaseSinkDTO();
        CommonBeanUtils.copyProperties(request, dto, true);
        return dto;
    }

    /**
     * Get OceanBase sink info from JSON string
     *
     * @param extParams string ext params
     * @return {@link OceanBaseSinkDTO}
     */
    public static OceanBaseSinkDTO getFromJson(@NotNull String extParams) {
        try {
            return JsonUtils.parseObject(extParams, OceanBaseSinkDTO.class);
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SINK_INFO_INCORRECT,
                    String.format("parse extParams of OceanBase SinkDTO failure: %s", e.getMessage()));
        }
    }

    /**
     * Get OceanBase table info
     *
     * @param OceanBaseSink OceanBase sink dto,{@link OceanBaseSinkDTO}
     * @param columnList OceanBase column info list,{@link OceanBaseColumnInfo}
     * @return {@link OceanBaseTableInfo}
     */
    public static OceanBaseTableInfo getTableInfo(OceanBaseSinkDTO OceanBaseSink,
            List<OceanBaseColumnInfo> columnList) {
        OceanBaseTableInfo tableInfo = new OceanBaseTableInfo();
        tableInfo.setDbName(OceanBaseSink.getDatabaseName());
        tableInfo.setTableName(OceanBaseSink.getTableName());
        tableInfo.setPrimaryKey(OceanBaseSink.getPrimaryKey());
        tableInfo.setColumns(columnList);
        return tableInfo;
    }

    /**
     * Get DbName from jdbcUrl
     *
     * @param jdbcUrl OceanBase JDBC url, such as jdbc:oceanbase://host:port/database
     * @return database name
     */
    private static String getDbNameFromUrl(String jdbcUrl) {
        String database = null;

        if (Strings.isNullOrEmpty(jdbcUrl)) {
            throw new IllegalArgumentException("Invalid JDBC url.");
        }

        jdbcUrl = jdbcUrl.toLowerCase();
        if (jdbcUrl.startsWith("jdbc:impala")) {
            jdbcUrl = jdbcUrl.replace(":impala", "");
        }

        int pos1;
        if (!jdbcUrl.startsWith("jdbc:")
                || (pos1 = jdbcUrl.indexOf(':', 5)) == -1) {
            throw new IllegalArgumentException("Invalid JDBC url.");
        }

        String connUri = jdbcUrl.substring(pos1 + 1);
        if (connUri.startsWith("//")) {
            int pos = connUri.indexOf('/', 2);
            if (pos != -1) {
                database = connUri.substring(pos + 1);
            }
        } else {
            database = connUri;
        }

        if (Strings.isNullOrEmpty(database)) {
            throw new IllegalArgumentException("Invalid JDBC URL: " + jdbcUrl);
        }

        if (database.contains(InlongConstants.QUESTION_MARK)) {
            database = database.substring(0, database.indexOf(InlongConstants.QUESTION_MARK));
        }
        if (database.contains(InlongConstants.SEMICOLON)) {
            database = database.substring(0, database.indexOf(InlongConstants.SEMICOLON));
        }
        return database;
    }

    public static String setDbNameToUrl(String jdbcUrl, String databaseName) {
        if (StringUtils.isBlank(jdbcUrl)) {
            return jdbcUrl;
        }
        String pattern = "jdbc:oceanbase://(?<host>[a-zA-Z0-9-//.]+):(?<port>[0-9]+)?(?<ext>)";
        Pattern namePattern = Pattern.compile(pattern);
        Matcher dataMatcher = namePattern.matcher(jdbcUrl);
        StringBuilder resultUrl;
        if (dataMatcher.find()) {
            String host = dataMatcher.group("host");
            String port = dataMatcher.group("port");
            resultUrl = new StringBuilder().append(OCEANBASE_JDBC_PREFIX)
                    .append(host)
                    .append(InlongConstants.COLON)
                    .append(port)
                    .append(InlongConstants.SLASH)
                    .append(databaseName);
        } else {
            throw new BusinessException(ErrorCodeEnum.SINK_INFO_INCORRECT,
                    "OceanBase JDBC URL was invalid, it should like jdbc:mysql://host:port");
        }
        if (jdbcUrl.contains(InlongConstants.QUESTION_MARK)) {
            resultUrl.append(jdbcUrl.substring(jdbcUrl.indexOf(InlongConstants.QUESTION_MARK)));
        }
        return resultUrl.toString();
    }
    public static String setDbNameToUrlWithCdc(String jdbcUrl, String databaseName) {
        if (StringUtils.isBlank(jdbcUrl)) {
            return jdbcUrl;
        }
        String pattern = "jdbc:oceanbase://(?<host>[a-zA-Z0-9-//.]+):(?<port>[0-9]+)?(?<ext>)";
        Pattern namePattern = Pattern.compile(pattern);
        Matcher dataMatcher = namePattern.matcher(jdbcUrl);
        StringBuilder resultUrl;
        if (dataMatcher.find()) {
            String host = dataMatcher.group("host");
            String port = dataMatcher.group("port");
            resultUrl = new StringBuilder().append(OCEANBASE_JDBC_PREFIX_CDC)
                    .append(host)
                    .append(InlongConstants.COLON)
                    .append(port)
                    .append(InlongConstants.SLASH)
                    .append(databaseName);
        } else {
            throw new BusinessException(ErrorCodeEnum.SINK_INFO_INCORRECT,
                    "OceanBase JDBC URL was invalid, it should like jdbc:mysql://host:port");
        }
        if (jdbcUrl.contains(InlongConstants.QUESTION_MARK)) {
            resultUrl.append(jdbcUrl.substring(jdbcUrl.indexOf(InlongConstants.QUESTION_MARK)));
        }
        return resultUrl.toString();
    }
}
