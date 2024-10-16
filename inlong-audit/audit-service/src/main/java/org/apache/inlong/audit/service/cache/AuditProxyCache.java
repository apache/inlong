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

package org.apache.inlong.audit.service.cache;

import org.apache.inlong.audit.entity.AuditProxy;
import org.apache.inlong.audit.service.config.Configuration;
import org.apache.inlong.audit.service.config.ProxyConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.inlong.audit.entity.AuditComponent.AGENT;
import static org.apache.inlong.audit.entity.AuditComponent.DATAPROXY;
import static org.apache.inlong.audit.entity.AuditComponent.PUBLIC_NETWORK;
import static org.apache.inlong.audit.entity.AuditComponent.SORT;

public class AuditProxyCache {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuditProxyCache.class);
    private static final AuditProxyCache instance = new AuditProxyCache();
    private final Map<String, List<AuditProxy>> auditProxyCache = new HashMap<>();

    private AuditProxyCache() {
    }

    public boolean init() {
        return initializeAuditProxyCache();
    }

    private boolean initializeAuditProxyCache() {
        AtomicBoolean isSuccess = new AtomicBoolean(true);
        Map<String, String> proxyConfigs = getProxyConfigs();
        proxyConfigs.forEach((component, proxyList) -> {
            List<AuditProxy> auditProxies = createAuditProxySet(proxyList);
            if (auditProxies.isEmpty()) {
                LOGGER.error("{} Audit Proxy config = {}, is invalid!", component, proxyList);
                isSuccess.set(false);
            } else {
                LOGGER.info("{} Audit Proxy config = {}", component, proxyList);
                auditProxyCache.put(component, auditProxies);
            }
        });

        return isSuccess.get();
    }

    private Map<String, String> getProxyConfigs() {
        Configuration config = Configuration.getInstance();
        Map<String, String> proxyConfigs = new HashMap<>();
        proxyConfigs.put(AGENT.getComponent(),
                config.get(ProxyConstants.KEY_AUDIT_PROXY_ADDRESS_AGENT,
                        ProxyConstants.DEFAULT_AUDIT_PROXY_ADDRESS_AGENT));
        proxyConfigs.put(DATAPROXY.getComponent(),
                config.get(ProxyConstants.KEY_AUDIT_PROXY_ADDRESS_DATAPROXY,
                        ProxyConstants.DEFAULT_AUDIT_PROXY_ADDRESS_DATAPROXY));
        proxyConfigs.put(SORT.getComponent(),
                config.get(ProxyConstants.KEY_AUDIT_PROXY_ADDRESS_SORT,
                        ProxyConstants.DEFAULT_AUDIT_PROXY_ADDRESS_SORT));
        proxyConfigs.put(PUBLIC_NETWORK.getComponent(),
                config.get(ProxyConstants.KEY_AUDIT_PROXY_ADDRESS_PUBLIC_NETWORK,
                        ProxyConstants.DEFAULT_AUDIT_PROXY_ADDRESS_PUBLIC_NETWORK));
        return proxyConfigs;
    }

    private List<AuditProxy> createAuditProxySet(String proxyList) {
        return Arrays.stream(proxyList.split(ProxyConstants.PROXY_SEPARATOR))
                .map(element -> element.split(ProxyConstants.IP_PORT_SEPARATOR))
                .filter(ipPort -> ipPort.length == 2)
                .map(ipPort -> new AuditProxy(ipPort[0], Integer.parseInt(ipPort[1])))
                .collect(Collectors.toList());
    }

    public static AuditProxyCache getInstance() {
        return instance;
    }

    public List<AuditProxy> getData(String component) {
        List<AuditProxy> result = auditProxyCache.get(component);
        if (result == null) {
            return new LinkedList<>();
        }
        return result;
    }

}
