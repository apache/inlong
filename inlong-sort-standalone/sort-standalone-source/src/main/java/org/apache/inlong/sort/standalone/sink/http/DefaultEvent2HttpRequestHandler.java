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

import org.apache.inlong.common.pojo.sort.dataflow.dataType.CsvConfig;
import org.apache.inlong.common.pojo.sort.dataflow.dataType.DataTypeConfig;
import org.apache.inlong.common.pojo.sort.dataflow.dataType.KvConfig;
import org.apache.inlong.sdk.transform.process.TransformProcessor;
import org.apache.inlong.sort.standalone.channel.ProfileEvent;
import org.apache.inlong.sort.standalone.dispatch.DispatchProfile;
import org.apache.inlong.sort.standalone.sink.EventUtils;
import org.apache.inlong.sort.standalone.utils.InlongLoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultEvent2HttpRequestHandler implements IEvent2HttpRequestHandler {

    public static final Logger LOG = InlongLoggerFactory.getLogger(DefaultEvent2HttpRequestHandler.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    public static final String HTTP_PREFIX = "http://";
    public static final String HTTPS_PREFIX = "https://";
    public static final String INLONG_GROUP_ID_HEADER = "inlongGroupId";
    public static final String INLONG_STREAM_ID_HEADER = "inlongStreamId";

    public static final String KEY_FTIME = "ftime";
    public static final String KEY_EXTINFO = "extinfo";

    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public List<HttpRequest> parse(HttpSinkContext context, ProfileEvent event)
            throws URISyntaxException, JsonProcessingException {
        String uid = event.getUid();
        HttpIdConfig idConfig = context.getIdConfig(uid);
        if (idConfig == null) {
            return null;
        }
        List<Map<String, Object>> fieldMaps = decodeEvent(context, event, idConfig);

        // build
        String uriString = context.getBaseUrl() + idConfig.getPath();
        if (!uriString.startsWith(HTTP_PREFIX) && !uriString.startsWith(HTTPS_PREFIX)) {
            uriString = HTTP_PREFIX + uriString;
        }
        URI uri = new URI(uriString);
        String jsonData;
        String requestMethod = idConfig.getMethod().toUpperCase();
        List<HttpRequest> requests = new ArrayList<>();
        switch (requestMethod) {
            case "GET":
                for (Map<String, Object> fieldMap : fieldMaps) {
                    List<String> columnPairs = new ArrayList<>(fieldMap.size());
                    for (Entry<String, Object> entry : fieldMap.entrySet()) {
                        String key = entry.getKey();
                        String value = "";
                        try {
                            value = URLEncoder.encode(String.valueOf(entry.getValue()),
                                    idConfig.getSinkCharset().name());
                        } catch (Throwable t) {
                        }
                        columnPairs.add(String.join("=", key, value));
                    }
                    String params = String.join("&", columnPairs);
                    HttpUriRequest request = new HttpGet(uri + "?" + params);
                    for (Map.Entry<String, String> entry : idConfig.getHeaders().entrySet()) {
                        request.setHeader(entry.getKey(), entry.getValue());
                    }
                    request.setHeader(INLONG_GROUP_ID_HEADER, idConfig.getInlongGroupId());
                    request.setHeader(INLONG_STREAM_ID_HEADER, idConfig.getInlongStreamId());
                    requests.add(new HttpRequest(request, event, null, idConfig.getMaxRetryTimes()));
                }
                break;
            case "POST":
                for (Map<String, Object> fieldMap : fieldMaps) {
                    HttpPost request = new HttpPost(uri);
                    for (Map.Entry<String, String> entry : idConfig.getHeaders().entrySet()) {
                        request.setHeader(entry.getKey(), entry.getValue());
                    }
                    request.setHeader(INLONG_GROUP_ID_HEADER, idConfig.getInlongGroupId());
                    request.setHeader(INLONG_STREAM_ID_HEADER, idConfig.getInlongStreamId());
                    jsonData = objectMapper.writeValueAsString(fieldMap);
                    setEntity((HttpEntityEnclosingRequestBase) request, jsonData);
                    requests.add(new HttpRequest(request, event, null, idConfig.getMaxRetryTimes()));
                }
                break;
            case "PUT":
                for (Map<String, Object> fieldMap : fieldMaps) {
                    HttpPut request = new HttpPut(uri);
                    for (Map.Entry<String, String> entry : idConfig.getHeaders().entrySet()) {
                        request.setHeader(entry.getKey(), entry.getValue());
                    }
                    request.setHeader(INLONG_GROUP_ID_HEADER, idConfig.getInlongGroupId());
                    request.setHeader(INLONG_STREAM_ID_HEADER, idConfig.getInlongStreamId());
                    jsonData = objectMapper.writeValueAsString(fieldMap);
                    setEntity((HttpEntityEnclosingRequestBase) request, jsonData);
                    requests.add(new HttpRequest(request, event, null, idConfig.getMaxRetryTimes()));
                }
                break;
            default:
                LOG.error("Unsupported request method: {}", requestMethod);
                return null;
        }
        return requests;
    }

    private List<Map<String, Object>> decodeEvent(HttpSinkContext context, ProfileEvent event, HttpIdConfig idConfig) {
        TransformProcessor<String, Map<String, Object>> processor = context.getTransformProcessor(event.getUid());
        if (processor != null) {
            return EventUtils.decodeTransform(context, event, processor);
        }
        // parse fields
        String strContent = EventUtils.prepareStringContent(idConfig, event);
        List<Map<String, String>> fieldMaps = null;
        if (idConfig.getDataTypeConfig() == null) {
            CsvConfig csvConfig = new CsvConfig();
            csvConfig.setDelimiter(idConfig.getSeparator().charAt(0));
            csvConfig.setEscapeChar('\\');
            fieldMaps = EventUtils.decodeCsv(context, event, idConfig, csvConfig, strContent);
        } else {
            DataTypeConfig dataTypeConfig = idConfig.getDataTypeConfig();
            if (dataTypeConfig instanceof CsvConfig) {
                CsvConfig csvConfig = (CsvConfig) dataTypeConfig;
                fieldMaps = EventUtils.decodeCsv(context, event, idConfig, csvConfig, strContent);
            } else if (dataTypeConfig instanceof KvConfig) {
                KvConfig kvConfig = (KvConfig) dataTypeConfig;
                fieldMaps = EventUtils.decodeKv(context, event, idConfig, kvConfig, strContent);
            } else {
                CsvConfig csvConfig = new CsvConfig();
                csvConfig.setDelimiter(idConfig.getSeparator().charAt(0));
                csvConfig.setEscapeChar('\\');
                fieldMaps = EventUtils.decodeCsv(context, event, idConfig, csvConfig, strContent);
            }
        }
        // ftime
        String ftime = dateFormat.format(new Date(event.getRawLogTime()));
        // extinfo
        String extinfo = EventUtils.getExtInfo(event);
        List<Map<String, Object>> results = new ArrayList<>();
        for (Map<String, String> fieldMap : fieldMaps) {
            Map<String, Object> result = new ConcurrentHashMap<>();
            result.putAll(fieldMap);
            if (!result.containsKey(KEY_FTIME)) {
                result.put(KEY_FTIME, ftime);
            }

            if (!result.containsKey(KEY_EXTINFO)) {
                result.put(KEY_EXTINFO, extinfo);
            }
        }
        return results;
    }

    private static void setEntity(HttpEntityEnclosingRequestBase request, String jsonData) {
        StringEntity requestEntity = new StringEntity(jsonData, ContentType.APPLICATION_JSON);
        request.setEntity(requestEntity);
    }

    @Override
    public List<HttpRequest> parse(HttpSinkContext context, DispatchProfile dispatchProfile)
            throws URISyntaxException, JsonProcessingException {
        List<HttpRequest> requests = new ArrayList<>();
        for (ProfileEvent profileEvent : dispatchProfile.getEvents()) {
            List<HttpRequest> request = this.parse(context, profileEvent);
            requests.addAll(request);
        }
        return requests;
    }
}
