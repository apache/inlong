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

package org.apache.inlong.sort.standalone.sink.http;

import org.apache.inlong.common.pojo.sort.dataflow.DataFlowConfig;
import org.apache.inlong.common.pojo.sort.dataflow.dataType.CsvConfig;
import org.apache.inlong.common.pojo.sort.dataflow.dataType.DataTypeConfig;
import org.apache.inlong.common.pojo.sort.dataflow.dataType.KvConfig;
import org.apache.inlong.common.pojo.sort.dataflow.field.FieldConfig;
import org.apache.inlong.common.pojo.sort.dataflow.sink.HttpSinkConfig;
import org.apache.inlong.sort.standalone.config.pojo.IdConfig;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
@Slf4j
public class HttpIdConfig extends IdConfig {

    public static final String DEFAULT_SEPARATOR = "|";

    private String path;
    private String method;
    private Map<String, String> headers;
    private Integer maxRetryTimes;
    private String separator;
    private DataTypeConfig dataTypeConfig;
    private List<String> fieldList;
    private Charset sourceCharset;
    private Charset sinkCharset;
    private DataFlowConfig dataFlowConfig;

    public static HttpIdConfig create(DataFlowConfig dataFlowConfig) {
        HttpSinkConfig sinkConfig = (HttpSinkConfig) dataFlowConfig.getSinkConfig();
        List<String> fields = sinkConfig.getFieldConfigs()
                .stream()
                .map(FieldConfig::getName)
                .collect(Collectors.toList());
        Charset sourceCharset, sinkCharset;
        try {
            sinkCharset = Charset.forName(sinkConfig.getEncodingType());
        } catch (Throwable t) {
            log.warn("do not support field encoding type={}, dataflow id={}",
                    sinkConfig.getEncodingType(), dataFlowConfig.getDataflowId());
            sinkCharset = Charset.defaultCharset();
        }
        try {
            sourceCharset = Charset.forName(dataFlowConfig.getSourceConfig().getEncodingType());
        } catch (Throwable t) {
            log.warn("do not support context encoding type={}, dataflow id={}",
                    dataFlowConfig.getSourceConfig().getEncodingType(), dataFlowConfig.getDataflowId());
            sourceCharset = Charset.defaultCharset();
        }
        DataTypeConfig dataTypeConfig = dataFlowConfig.getSourceConfig().getDataTypeConfig();
        String separator = DEFAULT_SEPARATOR;
        if (dataTypeConfig instanceof CsvConfig) {
            separator = String.valueOf(((CsvConfig) dataTypeConfig).getDelimiter());
        } else if (dataTypeConfig instanceof KvConfig) {
            separator = String.valueOf(((KvConfig) dataTypeConfig).getEntrySplitter());
        }
        return HttpIdConfig.builder()
                .inlongGroupId(dataFlowConfig.getInlongGroupId())
                .inlongStreamId(dataFlowConfig.getInlongStreamId())
                .path(sinkConfig.getPath())
                .method(sinkConfig.getMethod())
                .headers(sinkConfig.getHeaders())
                .maxRetryTimes(sinkConfig.getMaxRetryTimes())
                .separator(separator)
                .fieldList(fields)
                .sinkCharset(sinkCharset)
                .sourceCharset(sourceCharset)
                .dataTypeConfig(dataTypeConfig)
                .dataFlowConfig(dataFlowConfig)
                .build();
    }
}
