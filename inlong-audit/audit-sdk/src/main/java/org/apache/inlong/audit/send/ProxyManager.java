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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;

public class ProxyManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyManager.class);
    private static final ProxyManager instance = new ProxyManager();
    private final List<String> currentIpPorts = new CopyOnWriteArrayList<>();

    private ProxyManager() {
    }

    public static ProxyManager getInstance() {
        return instance;
    }

    /**
     * update config
     */
    public void setAuditProxy(HashSet<String> ipPortList) {
        if (!ipPortList.equals(new HashSet<>(currentIpPorts))) {
            currentIpPorts.clear();
            currentIpPorts.addAll(ipPortList);
        }
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
}
