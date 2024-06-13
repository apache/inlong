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

package org.apache.inlong.audit.heartbeat;

import org.apache.inlong.audit.file.ConfigManager;
import org.apache.inlong.common.util.NetworkUtils;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.inlong.audit.entity.AuditComponent.COMMON_AUDIT;

public class Heartbeat {

    private static final Logger LOGGER = LoggerFactory.getLogger(Heartbeat.class);
    private final static String HEARTBEAT_PATH = "/audit/proxy/heartbeat";
    private static final String AUDIT_HEARTBEAT_INTERVAL_CONFIG_KEY = "audit.heartbeat.interval";
    private static final String AUDIT_SERVICE_HOST_CONFIG_KEY = "audit.service.host";
    private static final String AUDIT_SERVICE_PORT_CONFIG_KEY = "agent1.sources.tcp-source.port";
    private static final String AUDIT_COMPONENT_CONFIG_KEY = "audit.component";
    private String heartbeatHost;
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
    private final String localIp;
    private final String localPort;

    public Heartbeat() {
        localIp = NetworkUtils.getLocalIp();
        localPort = getLocalPort();
    }

    public void Start() {
        heartbeatHost = getConfiguredValue(AUDIT_SERVICE_HOST_CONFIG_KEY);
        timer.scheduleWithFixedDelay(this::heartbeat,
                1,
                getConfiguredInterval(),
                TimeUnit.MINUTES);
    }

    private void heartbeat() {
        if (heartbeatHost == null || localPort == null) {
            LOGGER.info("Heartbeat is not configure, Don`t need heartbeat");
            return;
        }
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            URIBuilder uriBuilder = new URIBuilder("http://" + heartbeatHost + HEARTBEAT_PATH);
            uriBuilder.setParameter("component", getConfiguredComponent());
            uriBuilder.setParameter("host", localIp);
            uriBuilder.setParameter("port", localPort);

            HttpGet httpGet = new HttpGet(uriBuilder.build());

            try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                String responseBody = EntityUtils.toString(response.getEntity());
                LOGGER.info("Heartbeat response: {}", responseBody);
            }
        } catch (Exception exception) {
            LOGGER.error("Heartbeat has exception", exception);
        }
    }

    private int getConfiguredInterval() {
        String intervalConfigValue = getConfiguredValue(AUDIT_HEARTBEAT_INTERVAL_CONFIG_KEY);
        return intervalConfigValue != null ? Integer.parseInt(intervalConfigValue) : 1;
    }

    private String getConfiguredComponent() {
        String intervalConfigComponent = getConfiguredValue(AUDIT_COMPONENT_CONFIG_KEY);
        return intervalConfigComponent != null ? intervalConfigComponent : COMMON_AUDIT.getComponent();
    }

    private String getConfiguredValue(String configKey) {
        return ConfigManager.getInstance().getValue(configKey);
    }

    private String getLocalPort() {
        try (Stream<Path> paths = Files.walk(Paths.get("."))) {
            Path selectedPath = paths.filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".conf"))
                    .findFirst()
                    .orElse(null);
            if (selectedPath != null) {
                String port = loadProperties(selectedPath);
                LOGGER.info("File: {} , TCP Source Port: {}", selectedPath, port);
                if (port != null) {
                    return port;
                }
            }
        } catch (IOException e) {
            LOGGER.error("Get local port has error", e);
        }
        return null;
    }

    private String loadProperties(Path path) {
        Properties prop = new Properties();
        try {
            prop.load(Files.newInputStream(path));
            return prop.getProperty(AUDIT_SERVICE_PORT_CONFIG_KEY);
        } catch (IOException e) {
            LOGGER.error("Load properties has error", e);
        }
        return null;
    }
}
