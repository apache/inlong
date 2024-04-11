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

package org.apache.inlong.audit.service;

import org.apache.inlong.audit.cache.DayCache;
import org.apache.inlong.audit.cache.HalfHourCache;
import org.apache.inlong.audit.cache.HourCache;
import org.apache.inlong.audit.cache.TenMinutesCache;
import org.apache.inlong.audit.config.Configuration;
import org.apache.inlong.audit.entities.AuditCycle;
import org.apache.inlong.audit.entities.StatData;
import org.apache.inlong.audit.utils.CacheUtils;

import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import static org.apache.inlong.audit.config.OpenApiConstants.AUDIT_ID;
import static org.apache.inlong.audit.config.OpenApiConstants.AUDIT_TAG;
import static org.apache.inlong.audit.config.OpenApiConstants.BIND_PORT;
import static org.apache.inlong.audit.config.OpenApiConstants.DEFAULT_API_BACKLOG_SIZE;
import static org.apache.inlong.audit.config.OpenApiConstants.DEFAULT_API_DAY_PATH;
import static org.apache.inlong.audit.config.OpenApiConstants.DEFAULT_API_HOUR_PATH;
import static org.apache.inlong.audit.config.OpenApiConstants.DEFAULT_API_MINUTE_10_PATH;
import static org.apache.inlong.audit.config.OpenApiConstants.DEFAULT_API_MINUTE_30_PATH;
import static org.apache.inlong.audit.config.OpenApiConstants.DEFAULT_API_REAL_LIMITER_QPS;
import static org.apache.inlong.audit.config.OpenApiConstants.DEFAULT_AUDIT_TAG;
import static org.apache.inlong.audit.config.OpenApiConstants.DEFAULT_POOL_SIZE;
import static org.apache.inlong.audit.config.OpenApiConstants.END_TIME;
import static org.apache.inlong.audit.config.OpenApiConstants.HTTP_RESPOND_CODE;
import static org.apache.inlong.audit.config.OpenApiConstants.INLONG_GROUP_Id;
import static org.apache.inlong.audit.config.OpenApiConstants.INLONG_STREAM_Id;
import static org.apache.inlong.audit.config.OpenApiConstants.KEY_API_BACKLOG_SIZE;
import static org.apache.inlong.audit.config.OpenApiConstants.KEY_API_DAY_PATH;
import static org.apache.inlong.audit.config.OpenApiConstants.KEY_API_HOUR_PATH;
import static org.apache.inlong.audit.config.OpenApiConstants.KEY_API_MINUTE_10_PATH;
import static org.apache.inlong.audit.config.OpenApiConstants.KEY_API_MINUTE_30_PATH;
import static org.apache.inlong.audit.config.OpenApiConstants.KEY_API_POOL_SIZE;
import static org.apache.inlong.audit.config.OpenApiConstants.KEY_API_REAL_LIMITER_QPS;
import static org.apache.inlong.audit.config.OpenApiConstants.KEY_HTTP_BODY_ERR_DATA;
import static org.apache.inlong.audit.config.OpenApiConstants.KEY_HTTP_BODY_ERR_MSG;
import static org.apache.inlong.audit.config.OpenApiConstants.KEY_HTTP_BODY_SUCCESS;
import static org.apache.inlong.audit.config.OpenApiConstants.KEY_HTTP_HEADER_CONTENT_TYPE;
import static org.apache.inlong.audit.config.OpenApiConstants.START_TIME;
import static org.apache.inlong.audit.config.OpenApiConstants.VALUE_HTTP_HEADER_CONTENT_TYPE;

public class ApiService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApiService.class);

    public void start() {
        initHttpServer();
    }

    public void stop() {

    }

    private void initHttpServer() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(BIND_PORT),
                    Configuration.getInstance().get(KEY_API_BACKLOG_SIZE, DEFAULT_API_BACKLOG_SIZE));
            server.setExecutor(Executors.newFixedThreadPool(
                    Configuration.getInstance().get(KEY_API_POOL_SIZE, DEFAULT_POOL_SIZE)));
            server.createContext(Configuration.getInstance().get(KEY_API_DAY_PATH, DEFAULT_API_DAY_PATH),
                    new AuditHandler(AuditCycle.DAY));
            server.createContext(Configuration.getInstance().get(KEY_API_HOUR_PATH, DEFAULT_API_HOUR_PATH),
                    new AuditHandler(AuditCycle.HOUR));
            server.createContext(
                    Configuration.getInstance().get(KEY_API_MINUTE_10_PATH, DEFAULT_API_MINUTE_10_PATH),
                    new AuditHandler(AuditCycle.MINUTE_10));
            server.createContext(Configuration.getInstance().get(KEY_API_MINUTE_30_PATH, DEFAULT_API_MINUTE_30_PATH),
                    new AuditHandler(AuditCycle.MINUTE_30));
            server.start();
        } catch (Exception e) {
            LOGGER.error("Init http server has exception!", e);
        }
    }

    static class AuditHandler implements HttpHandler, AutoCloseable {

        private final AuditCycle apiType;
        private final RateLimiter limiter;

        public AuditHandler(AuditCycle apiType) {
            this.apiType = apiType;
            limiter = RateLimiter.create(Configuration.getInstance().get(KEY_API_REAL_LIMITER_QPS,
                    DEFAULT_API_REAL_LIMITER_QPS));
        }

        @Override
        public void handle(HttpExchange exchange) {
            if (null != limiter) {
                limiter.acquire();
            }

            try (OutputStream os = exchange.getResponseBody()) {
                JsonObject responseJson = new JsonObject();

                Map<String, String> params = parseRequestURI(exchange.getRequestURI().getQuery());
                if (!checkParams(params)) {
                    handleInvalidParams(responseJson, exchange);
                } else {
                    handleLegalParams(responseJson, params);
                }

                byte[] bytes = responseJson.toString().getBytes();

                exchange.getResponseHeaders().set(KEY_HTTP_HEADER_CONTENT_TYPE,
                        VALUE_HTTP_HEADER_CONTENT_TYPE);
                exchange.sendResponseHeaders(HTTP_RESPOND_CODE, bytes.length);
                os.write(bytes);
            } catch (Exception e) {
                LOGGER.error("Audit handler has exception!", e);
            }
        }

        private Map<String, String> parseRequestURI(String query) {
            Map<String, String> params = new HashMap<>();
            if (query != null) {
                String[] pairs = query.split("&");
                for (String pair : pairs) {
                    String[] keyValue = pair.split("=");
                    if (keyValue.length == 2) {
                        String key = keyValue[0];
                        String value = keyValue[1];
                        params.put(key, value);
                    }
                }
            }
            params.putIfAbsent(AUDIT_TAG, DEFAULT_AUDIT_TAG);
            return params;
        }

        private boolean checkParams(Map<String, String> params) {
            return params.containsKey(START_TIME)
                    && params.containsKey(END_TIME)
                    && params.containsKey(AUDIT_ID)
                    && params.containsKey(INLONG_GROUP_Id)
                    && params.containsKey(INLONG_STREAM_Id);
        }

        private void handleInvalidParams(JsonObject responseJson, HttpExchange exchange) {
            responseJson.addProperty(KEY_HTTP_BODY_SUCCESS, false);
            responseJson.addProperty(KEY_HTTP_BODY_ERR_MSG, "Invalid params! " + exchange.getRequestURI());
            Gson gson = new Gson();
            responseJson.add(KEY_HTTP_BODY_ERR_DATA, gson.toJsonTree(new LinkedList<>()));
        }

        private void handleLegalParams(JsonObject responseJson, Map<String, String> params) {
            String cacheKey = CacheUtils.buildCacheKey(params.get(START_TIME), params.get(INLONG_GROUP_Id),
                    params.get(INLONG_STREAM_Id), params.get(AUDIT_ID), params.get(AUDIT_TAG));
            LOGGER.info("handleLegalParams cacheKey {}", cacheKey);
            List<StatData> statData = null;
            switch (apiType) {
                case MINUTE_10:
                    statData = TenMinutesCache.getInstance().getData(cacheKey);
                    break;
                case MINUTE_30:
                    statData = HalfHourCache.getInstance().getData(cacheKey);
                    break;
                case HOUR:
                    statData = HourCache.getInstance().getData(cacheKey);
                    break;
                case DAY:
                    statData = DayCache.getInstance().getData(
                            params.get(START_TIME),
                            params.get(END_TIME),
                            params.get(INLONG_GROUP_Id),
                            params.get(INLONG_STREAM_Id),
                            params.get(AUDIT_ID),
                            params.get(AUDIT_TAG));
                    break;
                default:
                    LOGGER.error("Unsupported interface type! type is {}", apiType);
            }
            if (null == statData)
                statData = new LinkedList<>();

            responseJson.addProperty(KEY_HTTP_BODY_SUCCESS, true);
            responseJson.addProperty(KEY_HTTP_BODY_ERR_MSG, "");
            Gson gson = new Gson();
            responseJson.add(KEY_HTTP_BODY_ERR_DATA, gson.toJsonTree(statData));
        }

        @Override
        public void close() throws Exception {

        }
    }
}
