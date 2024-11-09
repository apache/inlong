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

package org.apache.inlong.manager.schedule.airflow.api;

import org.apache.inlong.manager.schedule.exception.AirflowScheduleException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.springframework.http.HttpMethod;

import java.util.List;
import java.util.Map;

import static org.apache.inlong.manager.schedule.exception.AirflowScheduleException.AirflowErrorCode.BUILD_REQUEST_BODY_FAILED;

/**
 * The basic implementation of Airflow API interface.
 *
 * @param <T> the type of the response expected from the API, allowing flexibility for various response types.
 */

@Slf4j
public abstract class BaseAirflowApi<T> implements AirflowApi<T> {

    protected static final ObjectMapper objectMapper = new ObjectMapper();
    protected Map<String, String> pathParams = Maps.newHashMap();
    protected Map<String, Object> queryParams = Maps.newHashMap();
    protected Map<String, Object> requestBodyParams = Maps.newHashMap();

    @Override
    public abstract HttpMethod getMethod();

    @Override
    public abstract String getPath();

    @Override
    public abstract Class<T> getResponseType();

    @Override
    public Map<String, String> getPathParams() {
        return pathParams;
    }

    @Override
    public Map<String, Object> getQueryParams() {
        return queryParams;
    }

    /**
     * Create JSON request body
     * @return RequestBody Object
     */
    @Override
    public RequestBody getRequestBody() {
        try {
            return RequestBody.create(MediaType.parse("application/json; charset=utf-8"),
                    objectMapper.writeValueAsString(requestBodyParams));
        } catch (Exception e) {
            log.error("Airflow request body construction failed: {}", e.getMessage(), e);
            throw new AirflowScheduleException(BUILD_REQUEST_BODY_FAILED,
                    String.format("Airflow request body construction failed: %s", e.getMessage()));
        }
    }

    @Override
    public Request buildRequest(String baseUrl) {
        // Build a complete URL
        String path = buildPathParams(getPath(), getPathParams());
        String url = baseUrl + path;

        // Add query parameters
        if (!getQueryParams().isEmpty()) {
            String queryString = buildQueryString(getQueryParams());
            url += "?" + queryString;
        }

        // Build Request Builder
        Request.Builder builder = new Request.Builder().url(url);

        // Set requests based on HTTP methods
        switch (getMethod()) {
            case GET:
                builder.get();
                break;
            case POST:
                builder.post(getRequestBody());
                break;
            case PATCH:
                builder.patch(getRequestBody());
                break;
            case PUT:
                builder.put(getRequestBody());
                break;
            case DELETE:
                if (!requestBodyParams.isEmpty()) {
                    builder.delete(getRequestBody());
                } else {
                    builder.delete();
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported HTTP method: " + getMethod());
        }
        return builder.build();
    }

    private String buildPathParams(String path, Map<String, String> pathParams) {
        for (Map.Entry<String, String> entry : pathParams.entrySet()) {
            path = path.replace("{" + entry.getKey() + "}", entry.getValue());
        }
        return path;
    }

    private String buildQueryString(Map<String, Object> queryParams) {
        StringBuilder sb = new StringBuilder();
        // Multiple values can be specified for the same parameter name in the Get parameter.
        // (e.g. "?Key=value1&Key=value2")
        queryParams.forEach((key, value) -> {
            if (value instanceof List) {
                ((List) value).forEach(item -> sb.append(key).append("=").append(item).append("&"));
            } else {
                sb.append(key).append("=").append(value).append("&");
            }
        });
        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }
}
