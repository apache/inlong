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

package org.apache.inlong.audit.service.node;

import org.apache.inlong.audit.entity.AuditProxy;
import org.apache.inlong.audit.entity.AuditRoute;
import org.apache.inlong.audit.service.auditor.Audit;
import org.apache.inlong.audit.service.cache.AuditProxyCache;
import org.apache.inlong.audit.service.cache.AuditRouteCache;
import org.apache.inlong.audit.service.cache.DayCache;
import org.apache.inlong.audit.service.cache.HalfHourCache;
import org.apache.inlong.audit.service.cache.HourCache;
import org.apache.inlong.audit.service.cache.RealTimeQuery;
import org.apache.inlong.audit.service.cache.TenMinutesCache;
import org.apache.inlong.audit.service.config.Configuration;
import org.apache.inlong.audit.service.entities.ApiType;
import org.apache.inlong.audit.service.entities.AuditCycle;
import org.apache.inlong.audit.service.entities.StatData;
import org.apache.inlong.audit.service.metric.MetricsManager;

import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.inlong.audit.consts.ConfigConstants.DEFAULT_AUDIT_TAG;
import static org.apache.inlong.audit.consts.OpenApiConstants.DEFAULT_API_BACKLOG_SIZE;
import static org.apache.inlong.audit.consts.OpenApiConstants.DEFAULT_API_DAY_PATH;
import static org.apache.inlong.audit.consts.OpenApiConstants.DEFAULT_API_GET_AUDIT_PROXY_PATH;
import static org.apache.inlong.audit.consts.OpenApiConstants.DEFAULT_API_GET_AUDIT_ROUTE_PATH;
import static org.apache.inlong.audit.consts.OpenApiConstants.DEFAULT_API_GET_IDS_PATH;
import static org.apache.inlong.audit.consts.OpenApiConstants.DEFAULT_API_GET_IPS_PATH;
import static org.apache.inlong.audit.consts.OpenApiConstants.DEFAULT_API_HOUR_PATH;
import static org.apache.inlong.audit.consts.OpenApiConstants.DEFAULT_API_MINUTES_PATH;
import static org.apache.inlong.audit.consts.OpenApiConstants.DEFAULT_API_REAL_LIMITER_QPS;
import static org.apache.inlong.audit.consts.OpenApiConstants.DEFAULT_API_RECONCILIATION_PATH;
import static org.apache.inlong.audit.consts.OpenApiConstants.DEFAULT_API_THREAD_POOL_SIZE;
import static org.apache.inlong.audit.consts.OpenApiConstants.DEFAULT_HTTP_SERVER_BIND_PORT;
import static org.apache.inlong.audit.consts.OpenApiConstants.HTTP_RESPOND_CODE;
import static org.apache.inlong.audit.consts.OpenApiConstants.KEY_API_BACKLOG_SIZE;
import static org.apache.inlong.audit.consts.OpenApiConstants.KEY_API_DAY_PATH;
import static org.apache.inlong.audit.consts.OpenApiConstants.KEY_API_GET_AUDIT_PROXY_PATH;
import static org.apache.inlong.audit.consts.OpenApiConstants.KEY_API_GET_AUDIT_ROUTE_PATH;
import static org.apache.inlong.audit.consts.OpenApiConstants.KEY_API_GET_IDS_PATH;
import static org.apache.inlong.audit.consts.OpenApiConstants.KEY_API_GET_IPS_PATH;
import static org.apache.inlong.audit.consts.OpenApiConstants.KEY_API_HOUR_PATH;
import static org.apache.inlong.audit.consts.OpenApiConstants.KEY_API_MINUTES_PATH;
import static org.apache.inlong.audit.consts.OpenApiConstants.KEY_API_REAL_LIMITER_QPS;
import static org.apache.inlong.audit.consts.OpenApiConstants.KEY_API_RECONCILIATION_PATH;
import static org.apache.inlong.audit.consts.OpenApiConstants.KEY_API_THREAD_POOL_SIZE;
import static org.apache.inlong.audit.consts.OpenApiConstants.KEY_HTTP_BODY_DATA;
import static org.apache.inlong.audit.consts.OpenApiConstants.KEY_HTTP_BODY_ERR_MSG;
import static org.apache.inlong.audit.consts.OpenApiConstants.KEY_HTTP_BODY_SUCCESS;
import static org.apache.inlong.audit.consts.OpenApiConstants.KEY_HTTP_HEADER_CONTENT_TYPE;
import static org.apache.inlong.audit.consts.OpenApiConstants.KEY_HTTP_SERVER_BIND_PORT;
import static org.apache.inlong.audit.consts.OpenApiConstants.PARAMS_AUDIT_COMPONENT;
import static org.apache.inlong.audit.consts.OpenApiConstants.PARAMS_AUDIT_CYCLE;
import static org.apache.inlong.audit.consts.OpenApiConstants.PARAMS_AUDIT_HOST;
import static org.apache.inlong.audit.consts.OpenApiConstants.PARAMS_AUDIT_ID;
import static org.apache.inlong.audit.consts.OpenApiConstants.PARAMS_AUDIT_TAG;
import static org.apache.inlong.audit.consts.OpenApiConstants.PARAMS_END_TIME;
import static org.apache.inlong.audit.consts.OpenApiConstants.PARAMS_INLONG_GROUP_ID;
import static org.apache.inlong.audit.consts.OpenApiConstants.PARAMS_INLONG_STREAM_ID;
import static org.apache.inlong.audit.consts.OpenApiConstants.PARAMS_IP;
import static org.apache.inlong.audit.consts.OpenApiConstants.PARAMS_START_TIME;
import static org.apache.inlong.audit.consts.OpenApiConstants.VALUE_HTTP_HEADER_CONTENT_TYPE;
import static org.apache.inlong.audit.service.entities.ApiType.DAY;
import static org.apache.inlong.audit.service.entities.ApiType.GET_AUDIT_PROXY;
import static org.apache.inlong.audit.service.entities.ApiType.GET_AUDIT_ROUTE;
import static org.apache.inlong.audit.service.entities.ApiType.GET_IDS;
import static org.apache.inlong.audit.service.entities.ApiType.GET_IPS;
import static org.apache.inlong.audit.service.entities.ApiType.HOUR;
import static org.apache.inlong.audit.service.entities.ApiType.MINUTES;
import static org.apache.inlong.audit.service.entities.ApiType.RECONCILIATION;

public class ApiService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApiService.class);

    public void start() {
        if (!AuditProxyCache.getInstance().init()) {
            LOGGER.error("Audit Proxy cache init failed! exit...");
            System.exit(1);
        }

        initHttpServer();
    }

    public void stop() {

    }

    private void initHttpServer() {
        int bindPort = Configuration.getInstance().get(KEY_HTTP_SERVER_BIND_PORT, DEFAULT_HTTP_SERVER_BIND_PORT);
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(bindPort),
                    Configuration.getInstance()
                            .get(KEY_API_BACKLOG_SIZE, DEFAULT_API_BACKLOG_SIZE));
            server.setExecutor(Executors.newFixedThreadPool(
                    Configuration.getInstance().get(KEY_API_THREAD_POOL_SIZE, DEFAULT_API_THREAD_POOL_SIZE)));
            server.createContext(Configuration.getInstance().get(KEY_API_DAY_PATH, DEFAULT_API_DAY_PATH),
                    new AuditHandler(DAY));
            server.createContext(Configuration.getInstance().get(KEY_API_HOUR_PATH, DEFAULT_API_HOUR_PATH),
                    new AuditHandler(HOUR));
            server.createContext(Configuration.getInstance().get(KEY_API_MINUTES_PATH, DEFAULT_API_MINUTES_PATH),
                    new AuditHandler(MINUTES));
            server.createContext(Configuration.getInstance().get(KEY_API_GET_IDS_PATH, DEFAULT_API_GET_IDS_PATH),
                    new AuditHandler(GET_IDS));
            server.createContext(Configuration.getInstance().get(KEY_API_GET_IPS_PATH, DEFAULT_API_GET_IPS_PATH),
                    new AuditHandler(GET_IPS));
            server.createContext(
                    Configuration.getInstance().get(KEY_API_GET_AUDIT_PROXY_PATH, DEFAULT_API_GET_AUDIT_PROXY_PATH),
                    new AuditHandler(GET_AUDIT_PROXY));
            server.createContext(
                    Configuration.getInstance().get(KEY_API_GET_AUDIT_PROXY_PATH, DEFAULT_API_GET_AUDIT_PROXY_PATH),
                    new AuditHandler(GET_AUDIT_PROXY));
            server.createContext(
                    Configuration.getInstance().get(KEY_API_RECONCILIATION_PATH,
                            DEFAULT_API_RECONCILIATION_PATH),
                    new AuditHandler(RECONCILIATION));
            server.createContext(
                    Configuration.getInstance().get(KEY_API_GET_AUDIT_ROUTE_PATH,
                            DEFAULT_API_GET_AUDIT_ROUTE_PATH),
                    new AuditHandler(GET_AUDIT_ROUTE));

            server.start();
            LOGGER.info("Init http server success. Bind port is: {}", bindPort);
        } catch (Exception e) {
            LOGGER.error("Init http server has exception!", e);
        }
    }

    class AuditHandler implements HttpHandler, AutoCloseable {

        private final ApiType apiType;
        private final RateLimiter limiter;
        private final ExecutorService executorService =
                Executors.newFixedThreadPool(
                        Configuration.getInstance().get(KEY_API_THREAD_POOL_SIZE, DEFAULT_API_THREAD_POOL_SIZE));

        public AuditHandler(ApiType apiType) {
            this.apiType = apiType;
            limiter = RateLimiter.create(Configuration.getInstance().get(KEY_API_REAL_LIMITER_QPS,
                    DEFAULT_API_REAL_LIMITER_QPS));
        }

        @Override
        public void handle(HttpExchange exchange) {
            long currentTimeMillis = System.currentTimeMillis();

            if (null != limiter) {
                limiter.acquire();
            }
            executorService.execute(new Runnable() {

                @Override
                public void run() {
                    try (OutputStream os = exchange.getResponseBody()) {
                        JsonObject responseJson = handleRequest(exchange);
                        sendResponse(exchange, os, responseJson);
                    } catch (Exception e) {
                        LOGGER.error("Audit handler has exception!", e);
                    } finally {
                        exchange.close();
                    }
                }
            });

            MetricsManager.getInstance().addApiMetric(apiType, System.currentTimeMillis() - currentTimeMillis);
        }

        private JsonObject handleRequest(HttpExchange exchange) throws IOException {
            String requestMethod = exchange.getRequestMethod();
            switch (requestMethod.toUpperCase()) {
                case "GET":
                    return handleGetRequest(exchange);
                case "POST":
                    return handlePostRequest(exchange);
                default:
                    return buildErrorResponse("Unsupported request method: " + requestMethod);
            }
        }

        private JsonObject handleGetRequest(HttpExchange exchange) {
            JsonObject responseJson = new JsonObject();
            Map<String, String> params = parseRequestURI(exchange.getRequestURI().getQuery());
            if (checkNecessaryParams(params)) {
                handleLegalParams(responseJson, params);
            } else {
                handleInvalidParams(responseJson, exchange);
            }
            return responseJson;
        }

        private JsonObject handlePostRequest(HttpExchange exchange) throws IOException {
            StringBuilder requestInfo = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    requestInfo.append(line);
                }
            }
            return Audit.getInstance().getData(requestInfo.toString());
        }

        private void sendResponse(HttpExchange exchange, OutputStream os, JsonObject responseJson) throws IOException {
            byte[] bytes = responseJson.toString().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set(KEY_HTTP_HEADER_CONTENT_TYPE, VALUE_HTTP_HEADER_CONTENT_TYPE);
            exchange.sendResponseHeaders(HTTP_RESPOND_CODE, bytes.length);
            os.write(bytes);
        }

        private JsonObject buildErrorResponse(String errorMessage) {
            Gson gson = new Gson();
            JsonObject response = new JsonObject();
            response.addProperty(KEY_HTTP_BODY_SUCCESS, false);
            response.addProperty(KEY_HTTP_BODY_ERR_MSG, errorMessage);
            response.add(KEY_HTTP_BODY_DATA, gson.toJsonTree(new LinkedList<>()));
            return response;
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
            params.putIfAbsent(PARAMS_AUDIT_TAG, DEFAULT_AUDIT_TAG);
            return params;
        }

        private boolean checkNecessaryParams(Map<String, String> params) {
            switch (apiType) {
                case HOUR:
                case DAY:
                case GET_IPS:
                    return params.containsKey(PARAMS_START_TIME)
                            && params.containsKey(PARAMS_END_TIME)
                            && params.containsKey(PARAMS_AUDIT_ID)
                            && params.containsKey(PARAMS_INLONG_GROUP_ID)
                            && params.containsKey(PARAMS_INLONG_STREAM_ID);
                case MINUTES:
                    return params.containsKey(PARAMS_START_TIME)
                            && params.containsKey(PARAMS_END_TIME)
                            && params.containsKey(PARAMS_AUDIT_ID)
                            && params.containsKey(PARAMS_INLONG_GROUP_ID)
                            && params.containsKey(PARAMS_INLONG_STREAM_ID)
                            && params.containsKey(PARAMS_AUDIT_CYCLE);
                case GET_IDS:
                    return params.containsKey(PARAMS_START_TIME)
                            && params.containsKey(PARAMS_END_TIME)
                            && params.containsKey(PARAMS_AUDIT_ID)
                            && params.containsKey(PARAMS_IP);
                case GET_AUDIT_PROXY:
                    return params.containsKey(PARAMS_AUDIT_COMPONENT);
                case GET_AUDIT_ROUTE:
                    return params.containsKey(PARAMS_AUDIT_HOST);
                default:
                    return false;
            }
        }

        private void handleInvalidParams(JsonObject responseJson, HttpExchange exchange) {
            responseJson.addProperty(KEY_HTTP_BODY_SUCCESS, false);
            responseJson.addProperty(KEY_HTTP_BODY_ERR_MSG, "Invalid params! " + exchange.getRequestURI());
            Gson gson = new Gson();
            responseJson.add(KEY_HTTP_BODY_DATA, gson.toJsonTree(new LinkedList<>()));
        }

        private void handleLegalParams(JsonObject responseJson, Map<String, String> params) {
            responseJson.addProperty(KEY_HTTP_BODY_SUCCESS, true);
            responseJson.addProperty(KEY_HTTP_BODY_ERR_MSG, "");
            Gson gson = new Gson();
            List<StatData> statData;
            try {
                switch (apiType) {
                    case MINUTES:
                        responseJson.add(KEY_HTTP_BODY_DATA, gson.toJsonTree(handleMinutesApi(params)));
                        break;
                    case HOUR:
                        statData = HourCache.getInstance().getData(params.get(PARAMS_START_TIME),
                                params.get(PARAMS_END_TIME),
                                params.get(PARAMS_INLONG_GROUP_ID),
                                params.get(PARAMS_INLONG_STREAM_ID),
                                params.get(PARAMS_AUDIT_ID),
                                params.get(PARAMS_AUDIT_TAG));
                        responseJson.add(KEY_HTTP_BODY_DATA, gson.toJsonTree(statData));
                        break;
                    case DAY:
                        statData = DayCache.getInstance().getData(
                                params.get(PARAMS_START_TIME),
                                params.get(PARAMS_END_TIME),
                                params.get(PARAMS_INLONG_GROUP_ID),
                                params.get(PARAMS_INLONG_STREAM_ID),
                                params.get(PARAMS_AUDIT_ID));
                        responseJson.add(KEY_HTTP_BODY_DATA, gson.toJsonTree(statData));
                        break;
                    case GET_IDS:
                        statData = RealTimeQuery.getInstance().queryIdsByIp(
                                params.get(PARAMS_START_TIME),
                                params.get(PARAMS_END_TIME),
                                params.get(PARAMS_IP),
                                params.get(PARAMS_AUDIT_ID));
                        responseJson.add(KEY_HTTP_BODY_DATA, gson.toJsonTree(statData));
                        break;
                    case GET_IPS:
                        statData = RealTimeQuery.getInstance().queryIpsById(
                                params.get(PARAMS_START_TIME),
                                params.get(PARAMS_END_TIME),
                                params.get(PARAMS_INLONG_GROUP_ID),
                                params.get(PARAMS_INLONG_STREAM_ID),
                                params.get(PARAMS_AUDIT_ID));
                        responseJson.add(KEY_HTTP_BODY_DATA, gson.toJsonTree(statData));
                        break;
                    case GET_AUDIT_PROXY:
                        List<AuditProxy> auditProxy =
                                AuditProxyCache.getInstance().getData(params.get(PARAMS_AUDIT_COMPONENT));
                        responseJson.add(KEY_HTTP_BODY_DATA, gson.toJsonTree(auditProxy));
                        break;
                    case GET_AUDIT_ROUTE:
                        List<AuditRoute> auditRoute =
                                AuditRouteCache.getInstance().getData(params.get(PARAMS_AUDIT_HOST));
                        responseJson.add(KEY_HTTP_BODY_DATA, gson.toJsonTree(auditRoute));
                        break;
                    default:
                        LOGGER.error("Unsupported interface type! type is {}", apiType);
                        responseJson.add(KEY_HTTP_BODY_DATA, gson.toJsonTree(new LinkedList<>()));
                }
            } catch (Exception exception) {
                LOGGER.error("Handle legal params has exception ", exception);
                responseJson.add(KEY_HTTP_BODY_DATA, gson.toJsonTree(new LinkedList<>()));
            }
        }

        private List<StatData> handleMinutesApi(Map<String, String> params) {
            int cycle = Integer.parseInt(params.get(PARAMS_AUDIT_CYCLE));
            List<StatData> statData = null;
            switch (AuditCycle.fromInt(cycle)) {
                case MINUTE:
                    statData = RealTimeQuery.getInstance().queryLogTs(params.get(PARAMS_START_TIME),
                            params.get(PARAMS_END_TIME),
                            params.get(PARAMS_INLONG_GROUP_ID),
                            params.get(PARAMS_INLONG_STREAM_ID),
                            params.get(PARAMS_AUDIT_ID));
                    break;
                case MINUTE_10:
                    statData = TenMinutesCache.getInstance().getData(params.get(PARAMS_START_TIME),
                            params.get(PARAMS_END_TIME),
                            params.get(PARAMS_INLONG_GROUP_ID),
                            params.get(PARAMS_INLONG_STREAM_ID),
                            params.get(PARAMS_AUDIT_ID),
                            params.get(PARAMS_AUDIT_TAG));
                    break;
                case MINUTE_30:
                    statData = HalfHourCache.getInstance().getData(params.get(PARAMS_START_TIME),
                            params.get(PARAMS_END_TIME),
                            params.get(PARAMS_INLONG_GROUP_ID),
                            params.get(PARAMS_INLONG_STREAM_ID),
                            params.get(PARAMS_AUDIT_ID),
                            params.get(PARAMS_AUDIT_TAG));
                    break;
                default:
                    LOGGER.error("Unsupported cycle type! cycle is {}", cycle);
            }
            return statData;
        }

        @Override
        public void close() throws Exception {

        }
    }
}
