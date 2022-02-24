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

package org.apache.inlong.manager.common.pojo.source.binlog;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;

import javax.validation.constraints.NotNull;

/**
 * Binlog source info
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BinlogSourceDTO {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(); // thread safe

    @ApiModelProperty("Source database name")
    private String dbName;

    @ApiModelProperty("Source table name")
    private String tableName;

    @ApiModelProperty("Data charset")
    private String charset;

    @ApiModelProperty(value = "Table fields, separated by commas")
    private String tableFields;

    @ApiModelProperty(value = "Data separator, default is 0x01")
    private String dataSeparator = "0x01";

    @ApiModelProperty(value = "Middleware type, such as: TUBE, PULSAR")
    private String middlewareType;

    @ApiModelProperty(value = "Topic of Tube")
    private String tubeTopic;

    @ApiModelProperty(value = "Cluster address of Tube")
    private String tubeCluster;

    @ApiModelProperty(value = "Namespace of Pulsar")
    private String pulsarNamespace;

    @ApiModelProperty(value = "Topic of Pulsar")
    private String pulsarTopic;

    @ApiModelProperty(value = "Cluster address of Pulsar")
    private String pulsarCluster;

    @ApiModelProperty(value = "Whether to skip delete events in binlog, default: 1, that is skip")
    private Integer skipDelete;

    @ApiModelProperty(value = "Collect starts from the specified binlog location, and it is modified after delivery."
            + "If it is empty, an empty string is returned")
    private String startPosition;

    @ApiModelProperty(value = "When the field value is null, the replaced field defaults to 'null'")
    private String nullFieldChar;

    /**
     * Get the dto instance from the request
     */
    public static BinlogSourceDTO getFromRequest(BinlogSourceRequest request) {
        return BinlogSourceDTO.builder()
                .dbName(request.getDbName())
                .tableName(request.getTableName())
                .charset(request.getCharset())
                .dbName(request.getDbName())
                .tableName(request.getTableName())
                .tableFields(request.getTableFields())
                .dataSeparator(request.getDataSeparator())
                .skipDelete(request.getSkipDelete())
                .startPosition(request.getStartPosition())
                .nullFieldChar(request.getNullFieldChar())
                .build();
    }

    public static BinlogSourceDTO getFromJson(@NotNull String extParams) {
        try {
            OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            return OBJECT_MAPPER.readValue(extParams, BinlogSourceDTO.class);
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT.getMessage());
        }
    }

}
