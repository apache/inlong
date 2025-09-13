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

package org.apache.inlong.audit.tool.manager;

import org.apache.inlong.audit.tool.config.AppConfig;
import org.apache.inlong.audit.tool.dto.AuditAlertRule;
import org.apache.inlong.audit.tool.response.Response;
import org.apache.inlong.audit.tool.util.AuditAlertRulePageRequest;
import org.apache.inlong.audit.tool.util.HttpUtil;
import org.apache.inlong.audit.tool.util.PageResult;
import org.apache.inlong.audit.utils.HttpUtils;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.inlong.audit.tool.config.ConfigConstants.AUDIT_ALERT_RULE_lIST_PATH;

/**
 * Audit Alert Rule Manager implementation
 */
public class AuditAlertRuleManager {

    private AppConfig appConfig;
    private static final Logger LOGGER = LoggerFactory.getLogger(AuditAlertRuleManager.class);
    private final RestTemplate restTemplate = new RestTemplate();
    @Getter
    private List<AuditAlertRule> auditAlertRuleList = new CopyOnWriteArrayList<>();

    private static class Holder {

        private static final AuditAlertRuleManager INSTANCE = new AuditAlertRuleManager();
    }

    public static AuditAlertRuleManager getInstance() {
        return Holder.INSTANCE;
    }

    /**
     * Initialize the manager with application configuration.
     * This method is idempotent; subsequent calls are ignored with a warning.
     *
     * @param appConfig non-null configuration
     */
    public synchronized void init(AppConfig appConfig) {
        if (this.appConfig != null) {
            LOGGER.warn("AuditAlertRuleManager already initialized. Ignoring re-initialization");
            return;
        }
        if (appConfig != null) {
            this.appConfig = appConfig;
        } else {
            LOGGER.error("appConfig must not be null");
            throw new IllegalStateException("appConfig is null");
        }
    }

    private AuditAlertRuleManager() {
    }

    // Single-thread scheduler for periodic fetch
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "audit-alert-rule-scheduler");
        t.setDaemon(true);
        return t;
    });

    /**
     * Safely runs the fetch task, logging any exceptions without throwing.
     */
    private void runFetchTaskSafe() {
        try {
            LOGGER.info("Running scheduled fetch task...");
            List<AuditAlertRule> rules = fetchAlertRulesFromManager();
            if (!rules.isEmpty()) {
                auditAlertRuleList = rules;
            }
            LOGGER.info("Scheduled fetch task completed. rules={}", rules.size());
        } catch (Exception e) {
            LOGGER.error("Error during scheduled fetch task", e);
        }
    }

    /**
     * Schedules periodic fetching of rules at a fixed rate.
     * If the configured interval is <= 0, runs immediately once and returns.
     */
    public synchronized void schedule() {
        int executionIntervalTime =
                Integer.parseInt(appConfig.getProperties().getProperty("audit.data.time.interval.minute"));
        if (executionIntervalTime < 0) {
            LOGGER.warn("Execution Interval Time {} is in the past. Executing immediately", executionIntervalTime);
            executionIntervalTime = 0;
        }
        scheduler.scheduleAtFixedRate(
                this::runFetchTaskSafe,
                0L, // initial delay: fetch immediately
                executionIntervalTime,
                TimeUnit.MINUTES);
        LOGGER.info("Scheduled fetch task at {}", executionIntervalTime);
    }

    /**
     * Fetches the list of audit alert rules from the manager API.
     * Constructs the request URL, sends an HTTP GET request, and processes the response.
     *
     * @return A list of {@link AuditAlertRule} objects if successful, or null if the request fails.
     */
    public List<AuditAlertRule> fetchAlertRulesFromManager() {
        List<AuditAlertRule> auditAlertRules = new ArrayList<>();
        try {
            // Get the manager URL from app configuration
            String managerUrl = appConfig.getManagerUrl();

            // Ensure there is only one forward slash between the base URL and path
            String url = (managerUrl.endsWith("/") ? managerUrl.substring(0, managerUrl.length() - 1) : managerUrl)
                    + AUDIT_ALERT_RULE_lIST_PATH;

            Map<String, String> authHeader = HttpUtils.getAuthHeader(appConfig.getSecretId(), appConfig.getSecretKey());
            MultiValueMap<String, String> header = new LinkedMultiValueMap<>();
            authHeader.forEach(header::add);

            LOGGER.info("begin query audit alertRule list");

            // Send HTTP POST request to the manager API to retrieve audit alert rules
            Response<PageResult<AuditAlertRule>> result = HttpUtil.request(
                    restTemplate,
                    url,
                    HttpMethod.POST,
                    new AuditAlertRulePageRequest(),
                    header,
                    new ParameterizedTypeReference<Response<PageResult<AuditAlertRule>>>() {
                    });

            LOGGER.info("success to query audit info for url ={}", url);

            // Copy and return the list of audit alert rules
            if (result.isSuccess()) {
                auditAlertRules = result.getData().getList();
            } else {
                LOGGER.error("fetchAlertPolicies fail:{}", result.getErrMsg());
            }
        } catch (Exception e) {
            LOGGER.error("fetchAlertPolicies fail: ", e);
        }
        return auditAlertRules;
    }

    /**
     * Fetches audit data by retrieving audit alert rules and querying detailed audit information for each rule.
     *
     * @return List of {@link String} containing detailed audit information
     * @see #fetchAlertRulesFromManager()
     */
    public List<String> getAuditIds() {
        List<String> auditIds = new ArrayList<>();
        // Process each audit alert rule to get corresponding auditIds
        for (AuditAlertRule auditAlertRule : auditAlertRuleList) {
            // Convert comma-separated audit IDs to list and trim whitespace
            List<String> auditIdList = Arrays.stream(auditAlertRule.getAuditId().split(","))
                    .map(String::trim)
                    .collect(Collectors.toList());
            auditIds.addAll(auditIdList);
        }

        // Return empty list if no successful responses were received
        return auditIds;
    }

    /**
     * Stop the fetch task
     */
    public void stop() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}