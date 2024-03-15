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

package org.apache.inlong.audit.loader;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * DnsIpPortListLoader
 */
public class DnsSocketAddressListLoader implements SocketAddressListLoader {

    private static final Logger LOG = LoggerFactory.getLogger(DnsSocketAddressListLoader.class);
    public static final String KEY_DNS_ADDRESS = "audit.dns.address";
    public static final String KEY_DNS_PORT = "audit.dns.port";

    private Map<String, String> commonProperties;

    @Override
    public void setCommonProperties(Map<String, String> commonProperties) {
        this.commonProperties = commonProperties;
    }

    @Override
    public List<String> loadSocketAddressList() {
        if (commonProperties == null) {
            return null;
        }
        List<String> ipPortList = new ArrayList<>();
        String dns = commonProperties.get(KEY_DNS_ADDRESS);
        String dnsPort = commonProperties.get(KEY_DNS_PORT);
        if (!StringUtils.isEmpty(dns) && !StringUtils.isEmpty(dnsPort)) {
            try {
                InetAddress[] addrs = InetAddress.getAllByName(dns);
                for (InetAddress addr : addrs) {
                    ipPortList.add(addr.getHostAddress() + ":" + dnsPort);
                }
            } catch (Throwable t) {
                LOG.error(t.getMessage(), t);
            }
        }
        Collections.sort(ipPortList);
        return ipPortList;
    }

    public static void main(String[] args) {

        try {
            InetAddress[] addrs = InetAddress.getAllByName("inlong.woa.com");
            System.out.println(addrs);
        } catch (Throwable t) {
            LOG.error(t.getMessage(), t);
        }
    }
}
