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

import org.apache.inlong.sort.standalone.channel.ProfileEvent;
import org.apache.inlong.sort.standalone.dispatch.DispatchProfile;
import org.apache.inlong.sort.standalone.utils.InlongLoggerFactory;
import org.apache.inlong.sort.standalone.utils.UnescapeHelper;

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

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DefaultEvent2HttpRequestHandler implements IEvent2HttpRequestHandler {

    public static final Logger LOG = InlongLoggerFactory.getLogger(DefaultEvent2HttpRequestHandler.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    public static final String HTTP_PREFIX = "http://";
    public static final String HTTPS_PREFIX = "https://";
    public static final String INLONG_GROUP_ID_HEADER = "inlongGroupId";
    public static final String INLONG_STREAM_ID_HEADER = "inlongStreamId";

    @Override
    public HttpRequest parse(HttpSinkContext context, ProfileEvent event)
            throws URISyntaxException, JsonProcessingException {
        String uid = event.getUid();
        HttpIdConfig idConfig = context.getIdConfig(uid);
        if (idConfig == null) {
            return null;
        }
        // get the delimiter
        String delimiter = idConfig.getSeparator();
        char cDelimiter = delimiter.charAt(0);
        // for tab separator
        byte[] bodyBytes = event.getBody();
        String strContext = new String(bodyBytes, idConfig.getSourceCharset());
        // unescape
        List<String> columnValues = UnescapeHelper.toFiledList(strContext, cDelimiter);
        int valueLength = columnValues.size();
        List<String> fieldList = idConfig.getFieldList();
        int columnLength = fieldList.size();
        // get field value
        Map<String, String> fieldMap = new HashMap<>();
        for (int i = 0; i < columnLength; ++i) {
            String fieldName = fieldList.get(i);
            String fieldValue = i < valueLength ? columnValues.get(i) : "";
            fieldMap.put(fieldName, fieldValue);
        }

        // build
        String uriString = context.getBaseUrl() + idConfig.getPath();
        if (!uriString.startsWith(HTTP_PREFIX) && !uriString.startsWith(HTTPS_PREFIX)) {
            uriString = HTTP_PREFIX + uriString;
        }
        URI uri = new URI(uriString);
        String jsonData;
        HttpUriRequest request;
        String requestMethod = idConfig.getMethod().toUpperCase();
        switch (requestMethod) {
            case "GET":
                String params = fieldMap.entrySet().stream()
                        .map(entry -> {
                            try {
                                return entry.getKey() + "="
                                        + URLEncoder.encode(entry.getValue(), idConfig.getSinkCharset().name());
                            } catch (UnsupportedEncodingException e) {
                                throw new RuntimeException(e);
                            }
                        })
                        .collect(Collectors.joining("&"));
                request = new HttpGet(uri + "?" + params);
                for (Map.Entry<String, String> entry : idConfig.getHeaders().entrySet()) {
                    request.setHeader(entry.getKey(), entry.getValue());
                }
                request.setHeader(INLONG_GROUP_ID_HEADER, idConfig.getInlongGroupId());
                request.setHeader(INLONG_STREAM_ID_HEADER, idConfig.getInlongStreamId());
                break;
            case "POST":
                request = new HttpPost(uri);
                for (Map.Entry<String, String> entry : idConfig.getHeaders().entrySet()) {
                    request.setHeader(entry.getKey(), entry.getValue());
                }
                request.setHeader(INLONG_GROUP_ID_HEADER, idConfig.getInlongGroupId());
                request.setHeader(INLONG_STREAM_ID_HEADER, idConfig.getInlongStreamId());
                jsonData = objectMapper.writeValueAsString(fieldMap);
                setEntity((HttpEntityEnclosingRequestBase) request, jsonData);
                break;
            case "PUT":
                request = new HttpPut(uri);
                for (Map.Entry<String, String> entry : idConfig.getHeaders().entrySet()) {
                    request.setHeader(entry.getKey(), entry.getValue());
                }
                request.setHeader(INLONG_GROUP_ID_HEADER, idConfig.getInlongGroupId());
                request.setHeader(INLONG_STREAM_ID_HEADER, idConfig.getInlongStreamId());
                jsonData = objectMapper.writeValueAsString(fieldMap);
                setEntity((HttpEntityEnclosingRequestBase) request, jsonData);
                break;
            default:
                LOG.error("Unsupported request method: {}", requestMethod);
                return null;
        }
        return new HttpRequest(request, event, null, idConfig.getMaxRetryTimes());
    }

    private static void setEntity(HttpEntityEnclosingRequestBase request, String jsonData) {
        StringEntity requestEntity = new StringEntity(jsonData, ContentType.APPLICATION_JSON);
        request.setEntity(requestEntity);
    }

    public List<HttpRequest> parse(HttpSinkContext context, DispatchProfile dispatchProfile)
            throws URISyntaxException, JsonProcessingException {
        List<HttpRequest> requests = new ArrayList<>();
        for (ProfileEvent profileEvent : dispatchProfile.getEvents()) {
            HttpRequest request = this.parse(context, profileEvent);
            requests.add(request);
        }
        return requests;
    }
}
