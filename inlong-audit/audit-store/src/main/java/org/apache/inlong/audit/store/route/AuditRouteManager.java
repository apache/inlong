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

package org.apache.inlong.audit.store.route;

import org.apache.inlong.audit.entity.AuditRoute;
import org.apache.inlong.audit.file.ConfigManager;
import org.apache.inlong.audit.store.entities.ServiceResponse;
import org.apache.inlong.audit.utils.HttpUtils;
import org.apache.inlong.audit.utils.NamedThreadFactory;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.audit.store.config.ConfigConstants.DEFAULT_AUDIT_SERVICE_ADDR;
import static org.apache.inlong.audit.store.config.ConfigConstants.DEFAULT_AUDIT_SERVICE_ROUTE_API;
import static org.apache.inlong.audit.store.config.ConfigConstants.DEFAULT_AUDIT_SERVICE_TIMEOUT_MS;
import static org.apache.inlong.audit.store.config.ConfigConstants.KEY_AUDIT_SERVICE_ADDR;
import static org.apache.inlong.audit.store.config.ConfigConstants.KEY_AUDIT_SERVICE_ROUTE_API;
import static org.apache.inlong.audit.store.config.ConfigConstants.KEY_AUDIT_SERVICE_TIMEOUT_MS;
import static org.apache.inlong.audit.utils.RouteUtils.extractAddress;

public class AuditRouteManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuditRouteManager.class);

    private static final int PERIOD_MS = 60 * 1000; // 1 minute

    @Getter
    private static final AuditRouteManager instance = new AuditRouteManager();

    @Getter
    private volatile List<AuditRoute> auditRoutes = new CopyOnWriteArrayList<>();

    private final ScheduledExecutorService timerExecutor =
            Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("update-audit-route"));

    private final Gson gson = new Gson();

    private static final String REQUEST_PARAM_HOST = "host";

    private AuditRouteManager() {
    }

    public void init(String jdbcUrl) {
        String serviceAddr = ConfigManager.getInstance().getValue(KEY_AUDIT_SERVICE_ADDR, DEFAULT_AUDIT_SERVICE_ADDR);
        String routeApi =
                ConfigManager.getInstance().getValue(KEY_AUDIT_SERVICE_ROUTE_API, DEFAULT_AUDIT_SERVICE_ROUTE_API);
        int timeoutMs =
                ConfigManager.getInstance().getValue(KEY_AUDIT_SERVICE_TIMEOUT_MS, DEFAULT_AUDIT_SERVICE_TIMEOUT_MS);

        String fullUrl = serviceAddr + routeApi;

        Map<String, String> requestParams = new HashMap<>();
        requestParams.put(REQUEST_PARAM_HOST, extractAddress(jdbcUrl));

        LOGGER.info("AuditRouteCache init, audit service url: {}, timeoutMs: {}, params: {}", fullUrl, timeoutMs,
                requestParams);

        timerExecutor.scheduleWithFixedDelay(() -> {
            try {
                updateAuditRouteCache(fullUrl, timeoutMs, requestParams);
            } catch (Exception e) {
                LOGGER.error("Exception occurred during audit route cache update", e);
            }
        }, 0, PERIOD_MS, TimeUnit.MILLISECONDS);
    }

    private void updateAuditRouteCache(String auditServiceUrl, int timeoutMs, Map<String, String> requestParams) {
        String jsonResponse = null;
        try {
            jsonResponse = HttpUtils.httpGet(auditServiceUrl, requestParams, timeoutMs);
        } catch (Exception e) {
            LOGGER.error("Http request error when updating audit routes. URL: {}, params: {}", auditServiceUrl,
                    requestParams, e);
            return;
        }

        if (jsonResponse == null || jsonResponse.isEmpty()) {
            LOGGER.error("Empty HTTP response when updating audit routes. URL: {}, params: {}", auditServiceUrl,
                    requestParams);
            return;
        }

        ServiceResponse response;
        try {
            response = gson.fromJson(jsonResponse, ServiceResponse.class);
        } catch (JsonSyntaxException e) {
            LOGGER.error("Parse JSON error when updating audit routes. Response: {}", jsonResponse, e);
            return;
        }

        if (response == null || !response.isSuccess()) {
            LOGGER.error("AuditRoute update failed. Response success=false. URL: {}, response: {}", auditServiceUrl,
                    jsonResponse);
            return;
        }

        if (response.getData() == null || response.getData().isEmpty()) {
            LOGGER.warn("AuditRoute update result is empty. URL: {}, response: {}", auditServiceUrl, jsonResponse);
            return;
        }

        auditRoutes = new CopyOnWriteArrayList<>(response.getData());

        LOGGER.info("AuditRoute updated successfully. Cache size={}, Response size={}", auditRoutes.size(),
                response.getData().size());
    }

    public void shutdown() {
        try {
            timerExecutor.shutdown();
            if (!timerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                timerExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}