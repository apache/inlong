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

import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ProxyManagerTest {

    @Test
    public void testProxyManager() {
        HashSet<String> ipPortList = new HashSet<>();
        ipPortList.add("172.0.0.1:10081");
        ipPortList.add("172.0.0.2:10081");
        ipPortList.add("172.0.0.3:10081");
        ipPortList.add("172.0.0.4:10081");
        ipPortList.add("172.0.0.5:10081");
        ProxyManager.getInstance().setAuditProxy(ipPortList);
        InetSocketAddress inetSocketAddress = ProxyManager.getInstance().getInetSocketAddress();
        assertEquals(10081, inetSocketAddress.getPort());
        assertTrue(inetSocketAddress.getAddress().getHostAddress().startsWith("172.0.0.")
                && inetSocketAddress.getAddress().getHostAddress().length() == 9);
    }
}
