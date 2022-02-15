/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.inlong.sort.standalone.channel.ProfileEvent;

/**
 * 
 * DefaultEvent2IndexRequestHandler
 */
public class DefaultEvent2IndexRequestHandler implements IEvent2IndexRequestHandler {

    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private AtomicLong esIndexIndex = new AtomicLong(System.currentTimeMillis());

    /**
     * parse
     * 
     * @param  context
     * @param  event
     * @return
     */
    @Override
    public EsIndexRequest parse(EsSinkContext context, ProfileEvent event) {
        String uid = event.getUid();
        EsIdConfig idConfig = context.getIdConfig(uid);
        if (idConfig == null) {
            context.addSendResultMetric(event, context.getTaskName(), false, System.currentTimeMillis());
            return null;
        }
        // parse fields
        String delimeter = idConfig.getSeparator();
        char cDelimeter = delimeter.charAt(0);
        String strContext = null;
        // for tab separator
        byte[] bodyBytes = event.getBody();
        int msgLength = event.getBody().length;
        int contentOffset = idConfig.getContentOffset();
        if (contentOffset > 0 && msgLength >= 1) {
            strContext = new String(bodyBytes, contentOffset, msgLength - contentOffset, Charset.defaultCharset());
        } else {
            strContext = new String(bodyBytes, Charset.defaultCharset());
        }
        // unescape
        List<String> columnVlues = unescapeFields(strContext, cDelimeter);
        int valueLength = columnVlues.size();
        List<String> fieldList = idConfig.getFieldList();
        int columnLength = fieldList.size();
        // field offset
        int fieldOffset = idConfig.getFieldOffset();
        // get field value
        Map<String, String> fieldMap = new HashMap<>();
        for (int i = fieldOffset; i < columnLength; ++i) {
            String fieldName = fieldList.get(i);
            int columnIndex = i - fieldOffset;
            String fieldValue = columnIndex < valueLength ? columnVlues.get(columnIndex) : "";
            byte[] fieldBytes = fieldValue.getBytes(Charset.defaultCharset());
            if (fieldBytes.length > context.getKeywordMaxLength()) {
                fieldValue = new String(fieldBytes, 0, context.getKeywordMaxLength());
            }
            fieldMap.put(fieldName, fieldValue);
        }

        // ftime
        String ftime = dateFormat.format(new Date(event.getRawLogTime()));
        fieldMap.put("ftime", ftime);
        // extinfo
        String extinfo = getExtInfo(event);
        fieldMap.put("extinfo", extinfo);
        String indexName = idConfig.parseIndexName(event.getRawLogTime());
        // build
        EsIndexRequest indexRequest = new EsIndexRequest(indexName, event);
        if (context.isUseIndexId()) {
            String esIndexId = uid + delimeter + event.getRawLogTime() + delimeter + esIndexIndex.incrementAndGet();
            indexRequest.id(esIndexId);
        }
        indexRequest.source(fieldMap);
        return indexRequest;
    }

    /**
     * unescapeFields
     * 
     * @param  fieldValues
     * @param  separator
     * @return
     */
    public static List<String> unescapeFields(String fieldValues, char separator) {
        List<String> fields = new ArrayList<String>();
        if (fieldValues.length() <= 0) {
            return fields;
        }

        int fieldLen = fieldValues.length();
        StringBuilder builder = new StringBuilder();
        int i = 0;
        for (; i < fieldLen - 1; i++) {
            char value = fieldValues.charAt(i);
            switch (value) {
                case '\\' :
                    char nextValue = fieldValues.charAt(i + 1);
                    switch (nextValue) {
                        case '0' :
                            builder.append(0x00);
                            i++;
                            break;
                        case 'n' :
                            builder.append('\n');
                            i++;
                            break;
                        case 'r' :
                            builder.append('\r');
                            i++;
                            break;
                        case '\\' :
                            builder.append('\\');
                            i++;
                            break;
                        default :
                            if (nextValue == separator) {
                                builder.append(separator);
                                i++;
                            } else {
                                builder.append(value);
                            }
                            break;
                    }
                    if (i == fieldLen - 1) {
                        fields.add(builder.toString());
                    }
                    break;
                default :
                    if (value == separator) {
                        fields.add(builder.toString());
                        builder.delete(0, builder.length());
                    } else {
                        builder.append(value);
                    }
                    break;
            }
        }

        if (i == fieldLen - 1) {
            char value = fieldValues.charAt(i);
            if (value == separator) {
                fields.add(builder.toString());
                fields.add("");
            } else {
                builder.append(value);
                fields.add(builder.toString());
            }
        }
        return fields;
    }

    /**
     * getExtInfo
     * 
     * @param  event
     * @return
     */
    public static String getExtInfo(ProfileEvent event) {
        if (event.getHeaders().size() > 0) {
            StringBuilder sBuilder = new StringBuilder();
            for (Entry<String, String> extInfo : event.getHeaders().entrySet()) {
                String key = extInfo.getKey();
                String value = extInfo.getValue();
                sBuilder.append(key).append('=').append(value).append('&');
            }
            String extinfo = sBuilder.substring(0, sBuilder.length() - 1);
            return extinfo;
        }
        return "";
    }
}
