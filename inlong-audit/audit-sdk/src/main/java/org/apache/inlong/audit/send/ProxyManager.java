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

package org.apache.inlong.audit.send;

import org.apache.inlong.audit.entity.AuditComponent;
import org.apache.inlong.audit.entity.AuditProxy;
import org.apache.inlong.audit.entity.CommonResponse;
import org.apache.inlong.audit.utils.HttpUtils;
import org.apache.inlong.audit.utils.NamedThreadFactory;
import org.apache.inlong.audit.utils.ThreadUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ProxyManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyManager.class);
    private static final ProxyManager instance = new ProxyManager();
    private final List<String> currentIpPorts = new CopyOnWriteArrayList<>();
    private final ScheduledExecutorService timer =
            Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("inlong-audit-proxy-manager"));
    private final static String GET_AUDIT_PROXY_API_PATH = "/inlong/manager/openapi/audit/getAuditProxy";
    private int timeoutMs = 10000;
    private int updateInterval = 60000;
    private String auditProxyApiUrl;
    private AuditComponent component;
    private String secretId;
    private String secretKey;
    private volatile boolean timerStarted = false;
    private static final int MAX_RETRY_TIMES = 1440;
    private static final int RETRY_INTERVAL_MS = 10000;

    private ProxyManager() {
    }

    public static ProxyManager getInstance() {
        return instance;
    }

    /**
     * update config
     */
    public synchronized void setAuditProxy(HashSet<String> ipPortList) {
        if (!ipPortList.equals(new HashSet<>(currentIpPorts))) {
            currentIpPorts.clear();
            currentIpPorts.addAll(ipPortList);
        }
    }

    public synchronized void setManagerConfig(AuditComponent component, String managerHost, String secretId,
            String secretKey) {
        if (!(managerHost.startsWith("http://") || managerHost.startsWith("https://"))) {
            managerHost = "http://" + managerHost;
        }
        auditProxyApiUrl = String.format("%s%s", managerHost, GET_AUDIT_PROXY_API_PATH);
        LOGGER.info("Audit Proxy API URL: {}", auditProxyApiUrl);

        this.component = component;
        this.secretId = secretId;
        this.secretKey = secretKey;

        retryAsync();
    }

    private void retryAsync() {
        CompletableFuture.runAsync(() -> {
            long retryIntervalMs = RETRY_INTERVAL_MS;
            for (int retryTime = 1; retryTime < MAX_RETRY_TIMES; retryTime++) {
                try {
                    if (updateAuditProxy()) {
                        LOGGER.info("Audit proxy updated successfully");
                        break;
                    }
                    LOGGER.warn("Failed to update audit proxy. Retrying in {} times...", retryTime);
                } catch (Exception exception) {
                    LOGGER.error("Failed to update audit proxy. Retrying in {} times...", retryTime, exception);
                } finally {
                    ThreadUtils.sleep(Math.min(retryIntervalMs, updateInterval));
                    retryIntervalMs *= 2;
                }
            }
        });
    }

    private boolean updateAuditProxy() {
        String response = HttpUtils.httpGet(component.getComponent(), auditProxyApiUrl, secretId, secretKey, timeoutMs);
        if (response == null) {
            LOGGER.error("Response is null: {} {} {} ", component.getComponent(), auditProxyApiUrl, secretId,
                    secretKey);
            return false;
        }
        CommonResponse<AuditProxy> commonResponse =
                CommonResponse.fromJson(response, AuditProxy.class);
        if (commonResponse == null || commonResponse.getData().isEmpty()) {
            LOGGER.error("No data in the response: {} {} {} {}", component.getComponent(), auditProxyApiUrl, secretId,
                    secretKey);
            return false;
        }
        HashSet<String> proxyList = new HashSet<>();
        for (AuditProxy auditProxy : commonResponse.getData()) {
            proxyList.add(auditProxy.toString());
        }

        setAuditProxy(proxyList);

        LOGGER.info("Get audit proxy from manager: {}", proxyList);
        return true;
    }

    private synchronized void startTimer() {
        if (timerStarted) {
            return;
        }
        timer.scheduleWithFixedDelay(this::updateAuditProxy,
                updateInterval,
                updateInterval,
                TimeUnit.MILLISECONDS);
        timerStarted = true;
    }

    public void setManagerTimeout(int timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    public void setAutoUpdateAuditProxy() {
        startTimer();
        LOGGER.info("Auto update Audit Proxy info from manager");
    }

    public void setUpdateInterval(int updateInterval) {
        this.updateInterval = updateInterval;
    }

    public InetSocketAddress getInetSocketAddress() {
        if (currentIpPorts.isEmpty()) {
            return null;
        }
        Random rand = new Random();
        String randomElement = currentIpPorts.get(rand.nextInt(currentIpPorts.size()));
        String[] ipPort = randomElement.split(":");
        if (ipPort.length != 2) {
            LOGGER.error("Invalid IP:Port format: {}", randomElement);
            return null;
        }
        return new InetSocketAddress(ipPort[0], Integer.parseInt(ipPort[1]));
    }
    public void shutdown() {
        timer.shutdown();
    }
}
