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

package org.apache.inlong.manager.common.pojo.sink.es;

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
 * Sink info of ElasticSearch
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ElasticSearchSinkDTO {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @ApiModelProperty("ElasticSearch URL")
    private String url;

    @ApiModelProperty("ElasticSearch Port")
    private Integer port;

    @ApiModelProperty("Username for JDBC URL")
    private String username;

    @ApiModelProperty("User password")
    private String password;

    @ApiModelProperty("ElasticSearch index name")
    private String indexName;

    @ApiModelProperty("Flush interval, unit: second, default is 1s")
    private Integer flushInterval;

    @ApiModelProperty("Flush when record number reaches flushRecord")
    private Integer flushRecord;

    @ApiModelProperty("Write max retry times, default is 3")
    private Integer retryTimes;

    @ApiModelProperty("Properties for elasticsearch")
    private Map<String, Object> properties;

    /**
     * Get the dto instance from the request
     */
    public static ElasticSearchSinkDTO getFromRequest(ElasticSearchSinkRequest request) {
        return ElasticSearchSinkDTO.builder()
                .url(request.getUrl())
                .username(request.getUsername())
                .password(request.getPassword())
                .indexName(request.getIndexName())
                .flushInterval(request.getFlushInterval())
                .flushRecord(request.getFlushRecord())
                .retryTimes(request.getRetryTimes())
                .properties(request.getProperties())
                .build();
    }

    public static ElasticSearchSinkDTO getFromJson(@NotNull String extParams) {
        try {
            OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            return OBJECT_MAPPER.readValue(extParams, ElasticSearchSinkDTO.class);
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SINK_INFO_INCORRECT.getMessage());
        }
    }

    public static ElasticSearchTableInfo getElasticSearchTableInfo(ElasticSearchSinkDTO ckInfo,
            List<ElasticSearchColumnInfo> columnList) {
        ElasticSearchTableInfo tableInfo = new ElasticSearchTableInfo();
        tableInfo.setIndexName(ckInfo.getIndexName());
        tableInfo.setColumns(columnList);

        return tableInfo;
    }

}
