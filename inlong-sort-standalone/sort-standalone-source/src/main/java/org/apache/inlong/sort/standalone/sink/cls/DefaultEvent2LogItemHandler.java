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

package org.apache.inlong.sort.standalone.sink.cls;

import org.apache.inlong.common.pojo.sort.dataflow.dataType.CsvConfig;
import org.apache.inlong.common.pojo.sort.dataflow.dataType.DataTypeConfig;
import org.apache.inlong.common.pojo.sort.dataflow.dataType.KvConfig;
import org.apache.inlong.sdk.transform.process.TransformProcessor;
import org.apache.inlong.sort.formats.util.StringUtils;
import org.apache.inlong.sort.standalone.channel.ProfileEvent;
import org.apache.inlong.sort.standalone.utils.InlongLoggerFactory;
import org.apache.inlong.sort.standalone.utils.UnescapeHelper;

import com.tencentcloudapi.cls.producer.common.LogItem;
import org.slf4j.Logger;

import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default event to logItem handler.
 */
public class DefaultEvent2LogItemHandler implements IEvent2LogItemHandler {

    private static final Logger LOG = InlongLoggerFactory.getLogger(DefaultEvent2LogItemHandler.class);
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    /**
     * Parse event to {@literal List<LogItem>} format.
     *
     * @param context Context of CLS sink.
     * @param event Event to be pares to {@literal List<LogItem>}
     * @return Prepared data structure to send.
     */
    @Override
    public List<LogItem> parse(ClsSinkContext context, ProfileEvent event) {
        String uid = event.getUid();
        ClsIdConfig idConfig = context.getIdConfig(uid);
        if (idConfig == null) {
            LOG.error("There is no cls id config for uid {}, discard it", uid);
            context.addSendResultMetric(event, context.getTaskName(), false, System.currentTimeMillis());
            return null;
        }

        // prepare values
        String stringValues = this.getStringValues(event, idConfig);
        List<String> listValues = null;
        if (idConfig.getDataTypeConfig() == null) {
            char delimiter = idConfig.getSeparator().charAt(0);
            listValues = UnescapeHelper.toFiledList(stringValues, delimiter);
        } else {
            DataTypeConfig dataTypeConfig = idConfig.getDataTypeConfig();
            if (dataTypeConfig instanceof CsvConfig) {
                CsvConfig csvConfig = (CsvConfig) dataTypeConfig;
                String[] csvArray = StringUtils.splitCsv(stringValues, csvConfig.getDelimiter(),
                        csvConfig.getEscapeChar(), null);
                listValues = Arrays.asList(csvArray);
            } else if (dataTypeConfig instanceof KvConfig) {
                KvConfig kvConfig = (KvConfig) dataTypeConfig;
                Map<String, String> kvMap = StringUtils.splitKv(stringValues, kvConfig.getEntrySplitter(),
                        kvConfig.getKvSplitter(), kvConfig.getEscapeChar(), null);
                List<String> listKeys = idConfig.getFieldList();
                final List<String> finalListValues = new ArrayList<>(listKeys.size());
                listKeys.forEach(v -> finalListValues.add(kvMap.get(v)));
                listValues = finalListValues;
            } else {
                char delimiter = idConfig.getSeparator().charAt(0);
                listValues = UnescapeHelper.toFiledList(stringValues, delimiter);
            }
        }
        listValues.forEach(value -> this.truncateSingleValue(value, context.getKeywordMaxLength()));
        // prepare keys
        List<String> listKeys = idConfig.getFieldList();
        // prepare offset
        int fieldOffset = idConfig.getFieldOffset();
        // convert to LogItem format
        LogItem item = this.parseToLogItem(listKeys, listValues, event.getRawLogTime(), fieldOffset);
        // add ftime
        String ftime = dateFormat.format(new Date(event.getRawLogTime()));
        item.PushBack("ftime", ftime);
        // add extinfo
        String extinfo = this.getExtInfo(event);
        item.PushBack("extinfo", extinfo);

        List<LogItem> itemList = new ArrayList<>();
        itemList.add(item);
        return itemList;
    }

    private String getStringValues(ProfileEvent event, ClsIdConfig idConfig) {
        byte[] bodyBytes = event.getBody();
        int msgLength = event.getBody().length;
        int contentOffset = idConfig.getContentOffset();
        if (contentOffset > 0 && msgLength >= 1) {
            return new String(bodyBytes, contentOffset, msgLength - contentOffset, Charset.defaultCharset());
        } else {
            return new String(bodyBytes, Charset.defaultCharset());
        }
    }

    private LogItem parseToLogItem(List<String> listKeys, List<String> listValues, long time, int fieldOffset) {
        LogItem logItem = new LogItem(time);
        for (int i = fieldOffset; i < listKeys.size(); ++i) {
            String key = listKeys.get(i);
            int columnIndex = i - fieldOffset;
            String value = columnIndex < listValues.size() ? listValues.get(columnIndex) : "";
            logItem.PushBack(key, value);
        }
        return logItem;
    }

    private String truncateSingleValue(String value, int limit) {
        byte[] inBytes = value.getBytes(Charset.defaultCharset());
        if (inBytes.length > limit) {
            value = new String(inBytes, 0, limit);
        }
        return value;
    }

    private String getExtInfo(ProfileEvent event) {
        if (event.getHeaders().size() > 0) {
            StringBuilder sBuilder = new StringBuilder();
            for (Map.Entry<String, String> extInfo : event.getHeaders().entrySet()) {
                String key = extInfo.getKey();
                String value = extInfo.getValue();
                sBuilder.append(key).append('=').append(value).append('&');
            }
            return sBuilder.substring(0, sBuilder.length() - 1);
        }
        return "";
    }

    @Override
    public List<LogItem> parse(
            ClsSinkContext context,
            ProfileEvent event,
            TransformProcessor<String, Map<String, Object>> processor) {
        String uid = event.getUid();
        ClsIdConfig idConfig = context.getIdConfig(uid);
        if (idConfig == null) {
            LOG.error("There is no cls id config for uid {}, discard it", uid);
            context.addSendResultMetric(event, context.getTaskName(), false, System.currentTimeMillis());
            return null;
        }

        // prepare values
        String stringValues = this.getStringValues(event, idConfig);
        Map<String, Object> extParams = new ConcurrentHashMap<>();
        extParams.putAll(context.getSinkContext().getParameters());
        event.getHeaders().forEach((k, v) -> extParams.put(k, v));
        List<Map<String, Object>> resultList = processor.transform(stringValues, extParams);
        if (resultList == null) {
            context.addSendFilterMetric(event, context.getTaskName());
            return null;
        }
        List<LogItem> itemList = new ArrayList<>();
        for (Map<String, Object> result : resultList) {
            // prepare keys
            List<String> listKeys = idConfig.getFieldList();
            final List<String> listValues = new ArrayList<>(listKeys.size());
            listKeys.forEach(v -> listValues.add(String.valueOf(result.get(v))));
            listValues.forEach(value -> this.truncateSingleValue(value, context.getKeywordMaxLength()));
            // prepare offset
            int fieldOffset = idConfig.getFieldOffset();
            // convert to LogItem format
            LogItem item = this.parseToLogItem(listKeys, listValues, event.getRawLogTime(), fieldOffset);
            // add ftime
            String ftime = dateFormat.format(new Date(event.getRawLogTime()));
            item.PushBack("ftime", ftime);
            // add extinfo
            String extinfo = this.getExtInfo(event);
            item.PushBack("extinfo", extinfo);
            itemList.add(item);
        }
        return itemList;
    }
}
