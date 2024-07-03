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

package org.apache.inlong.sort.standalone.sink.elasticsearch;

import org.apache.inlong.common.pojo.sort.dataflow.DataFlowConfig;
import org.apache.inlong.common.pojo.sort.dataflow.field.FieldConfig;
import org.apache.inlong.common.pojo.sort.dataflow.sink.EsSinkConfig;
import org.apache.inlong.sort.standalone.config.pojo.IdConfig;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
public class EsIdConfig extends IdConfig {

    public static final String PATTERN_DAY = "{yyyyMMdd}";
    public static final String PATTERN_HOUR = "{yyyyMMddHH}";
    public static final String PATTERN_MINUTE = "{yyyyMMddHHmm}";
    public static final String REGEX_DAY = "\\{yyyyMMdd\\}";
    public static final String REGEX_HOUR = "\\{yyyyMMddHH\\}";
    public static final String REGEX_MINUTE = "\\{yyyyMMddHHmm\\}";
    private static ThreadLocal<SimpleDateFormat> FORMAT_DAY = new ThreadLocal<SimpleDateFormat>() {

        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyyMMdd");
        }
    };
    private static ThreadLocal<SimpleDateFormat> FORMAT_HOUR = new ThreadLocal<SimpleDateFormat>() {

        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyyMMddHH");
        }
    };
    private static ThreadLocal<SimpleDateFormat> FORMAT_MINUTE = new ThreadLocal<SimpleDateFormat>() {

        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyyMMddHHmm");
        }
    };

    private String separator = "|";
    private String indexNamePattern;
    private String fieldNames;
    private int fieldOffset = 2; // for ftime,extinfo
    private int contentOffset = 0;// except for boss + tab(1)
    private List<String> fieldList;

    public static EsIdConfig create(DataFlowConfig dataFlowConfig) {
        EsSinkConfig sinkConfig = (EsSinkConfig) dataFlowConfig.getSinkConfig();
        List<String> fields = sinkConfig.getFieldConfigs()
                .stream()
                .map(FieldConfig::getName)
                .collect(Collectors.toList());
        return EsIdConfig.builder()
                .inlongGroupId(dataFlowConfig.getInlongGroupId())
                .inlongStreamId(dataFlowConfig.getInlongStreamId())
                .contentOffset(sinkConfig.getContentOffset())
                .fieldOffset(sinkConfig.getFieldOffset())
                .separator(sinkConfig.getSeparator())
                .indexNamePattern(sinkConfig.getIndexNamePattern())
                .fieldList(fields)
                .build();
    }

    public String parseIndexName(long msgTime) {
        Date dtDate = new Date(msgTime);
        String result = indexNamePattern;
        if (result.contains(PATTERN_MINUTE)) {
            String strHour = FORMAT_MINUTE.get().format(dtDate);
            result = result.replaceAll(REGEX_MINUTE, strHour);
        }
        if (result.contains(PATTERN_HOUR)) {
            String strHour = FORMAT_HOUR.get().format(dtDate);
            result = result.replaceAll(REGEX_HOUR, strHour);
        }
        if (result.contains(PATTERN_DAY)) {
            String strHour = FORMAT_DAY.get().format(dtDate);
            result = result.replaceAll(REGEX_DAY, strHour);
        }
        return result;
    }
}
