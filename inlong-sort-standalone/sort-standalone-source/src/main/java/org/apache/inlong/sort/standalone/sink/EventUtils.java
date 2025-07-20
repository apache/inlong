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

package org.apache.inlong.sort.standalone.sink;

import org.apache.inlong.common.pojo.sort.dataflow.dataType.CsvConfig;
import org.apache.inlong.common.pojo.sort.dataflow.dataType.KvConfig;
import org.apache.inlong.sdk.commons.protocol.EventConstants;
import org.apache.inlong.sdk.transform.process.TransformProcessor;
import org.apache.inlong.sort.formats.util.StringUtils;
import org.apache.inlong.sort.standalone.channel.ProfileEvent;
import org.apache.inlong.sort.standalone.sink.http.HttpIdConfig;
import org.apache.inlong.sort.standalone.sink.http.HttpSinkContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * EventUtils
 */
public class EventUtils {

    public static final String KEY_FTIME = "ftime";
    public static final String KEY_EXTINFO = "extinfo";

    public static List<Map<String, Object>> decodeTransform(HttpSinkContext context, ProfileEvent event,
            TransformProcessor<String, Map<String, Object>> processor) {
        // build
        Map<String, Object> extParams = new ConcurrentHashMap<>();
        extParams.putAll(context.getSinkContext().getParameters());
        event.getHeaders().forEach((k, v) -> extParams.put(k, v));
        List<Map<String, Object>> fieldMaps = processor.transformForBytes(event.getBody(), extParams);
        return fieldMaps;
    }

    public static List<Map<String, String>> decodeKv(HttpSinkContext context, ProfileEvent event, HttpIdConfig idConfig,
            KvConfig kvConfig, String strContent) {
        List<Map<String, String>> fieldMaps = StringUtils.splitKv(strContent, kvConfig.getEntrySplitter(),
                kvConfig.getKvSplitter(), kvConfig.getEscapeChar(), null, kvConfig.getLineSeparator(), true);
        return fieldMaps;
    }

    public static List<Map<String, String>> decodeCsv(HttpSinkContext context, ProfileEvent event,
            HttpIdConfig idConfig, CsvConfig csvConfig, String strContent) {
        String[][] columns = StringUtils.splitCsv(strContent,
                csvConfig.getDelimiter(),
                csvConfig.getEscapeChar(),
                null, '\n', false, true, null);
        List<Map<String, String>> fieldMaps = new ArrayList<>();
        for (int i = 0; i < columns.length; i++) {
            String[] columnValues = columns[i];
            // unescape
            int valueLength = columnValues.length;
            List<String> fieldList = idConfig.getFieldList();
            // get field value
            Map<String, String> fieldMap = new HashMap<>();
            for (int columnIndex = 0; columnIndex < fieldList.size(); columnIndex++) {
                String fieldName = fieldList.get(columnIndex);
                String fieldValue = columnIndex < valueLength ? columnValues[columnIndex] : "";
                fieldMap.put(fieldName, fieldValue);
            }
            fieldMaps.add(fieldMap);
        }
        return fieldMaps;
    }

    public static String prepareStringContent(HttpIdConfig idConfig, ProfileEvent event) {
        byte[] bodyBytes = event.getBody();
        String strContent = new String(bodyBytes, idConfig.getSourceCharset());
        return strContent;
    }

    /**
     * getExtInfo
     * 
     * @param  event
     * @return
     */
    public static String getExtInfo(ProfileEvent event) {
        String extinfoValue = event.getHeaders().get(KEY_EXTINFO);
        if (extinfoValue != null) {
            return KEY_EXTINFO + "=" + extinfoValue;
        }
        extinfoValue = KEY_EXTINFO + "=" + event.getHeaders().get(EventConstants.HEADER_KEY_SOURCE_IP);
        return extinfoValue;
    }
}
