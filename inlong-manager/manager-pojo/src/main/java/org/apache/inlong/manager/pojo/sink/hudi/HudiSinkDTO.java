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

package org.apache.inlong.manager.pojo.sink.hudi;

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
import java.util.List;
import java.util.Map;

/**
 * Hudi sink info
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class HudiSinkDTO {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @ApiModelProperty("path of hudi table")
    private String path;

    @ApiModelProperty("table type, like MERGE_ON_READ, COPY_ON_WRITE, default is COPY_ON_WRITE")
    private String tableType;

    @ApiModelProperty("Primary key must be shared by all tables")
    private String primaryKey;

    @ApiModelProperty("Target table name")
    private String tableName;

    @ApiModelProperty("Properties for Hudi")
    private Map<String, Object> properties;

    /**
     * Get the dto instance from the request
     */
    public static HudiSinkDTO getFromRequest(HudiSinkRequest request) {
        return HudiSinkDTO.builder()
                .path(request.getPath())
                .tableType(request.getTableType())
                .primaryKey(request.getPrimaryKey())
                .tableName(request.getTableName())
                .properties(request.getProperties())
                .build();
    }

    /**
     * Get the dto instance from the json
     */
    public static HudiSinkDTO getFromJson(@NotNull String extParams) {
        try {
            OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            return OBJECT_MAPPER.readValue(extParams, HudiSinkDTO.class);
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SINK_INFO_INCORRECT.getMessage() + ": " + e.getMessage());
        }
    }

    /**
     * Get Hudi table info
     *
     * @param HudiSink Hudi sink dto,{@link HudiSinkDTO}
     * @param columnList Hudi column info list,{@link HudiColumnInfo}
     * @return {@link HudiTableInfo}
     */
    public static HudiTableInfo getTableInfo(HudiSinkDTO HudiSink,
                                                  List<HudiColumnInfo> columnList) {
        HudiTableInfo tableInfo = new HudiTableInfo();
        tableInfo.setTableName(HudiSink.getTableName());
        tableInfo.setPath(HudiSink.getPath());
        tableInfo.setPrimaryKey(HudiSink.getPrimaryKey());
        tableInfo.setColumns(columnList);
        return tableInfo;
    }
}
