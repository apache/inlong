/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sdk.dataproxy.example;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.inlong.sdk.dataproxy.ProxyClientConfig;
import org.apache.inlong.sdk.dataproxy.network.HttpProxySender;
import org.apache.inlong.sdk.dataproxy.network.ProxysdkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpClientExample {
    private static final Logger logger = LoggerFactory.getLogger(HttpClientExample.class);

    private static String body = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+-|";

    public static void main(String[] args) {
        long sentCount = 10;
        /*
         * 1. if 'isLocalVisit' is true use local config from file in ${configBasePath}
         * directory/${dataProxyGroupId}
         * .local
         * such as :
         *  configBasePath = /data/inlong/dataproxy/conf
         *  dataProxyGroupId = test
         * so config file is :
         *  /data/inlong/dataproxy/conf/test.local
         * and config context like this:
         *  {"isInterVisit":1,"cluster_id":"1","size":1,"switch":1,"address":[{"host":"127.0.0
         * .1","port":"46802"},{"host":"127.0.0.1","port":"46802"}]}
         *
         * 2. if 'isLocalVisit' is false
         *  sdk will get config from manager auto.
         */
        boolean isLocalVisit = true;
        String dataProxyGroupId = "test";
        String groupId = "test_group_id";
        String streamId = "test_stream_id";
        String configBasePath = "/data/inlong/dataproxy/conf";
        String managerServerIp = "127.0.0.1";
        String managerServerPort = "8080";
        String localIP = "127.0.0.1";
        String netTag = "";

        ProxyClientConfig proxyConfig = null;
        try {
            proxyConfig = new ProxyClientConfig(localIP, isLocalVisit, managerServerIp,
                    Integer.valueOf(managerServerPort),
                    groupId, netTag);
            proxyConfig.setGroupId(dataProxyGroupId);
            proxyConfig.setConfStoreBasePath(configBasePath);
            proxyConfig.setReadProxyIPFromLocal(true);
            proxyConfig.setDiscardOldMessage(true);

            HttpProxySender sender = new HttpProxySender(proxyConfig);

            sendHttpMessage(sender, sentCount, groupId, streamId);
        } catch (ProxysdkException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void sendHttpMessage(HttpProxySender sender, long sentCount, String groupId,
            String streamId) throws Exception {
        int count = 0;
        String body1 = "Inlong_dataproxy";
        while (count < sentCount) {
            List<String> bodyList = new ArrayList<>();
            bodyList.add(body1);
            bodyList.add(body1);
            bodyList.add(body1);
            sender.asyncSendMessage(bodyList, groupId, streamId, System.currentTimeMillis(),
                    20,
                    TimeUnit.SECONDS, new MyMessageCallBack());
            if (count % 1000 == 0) {
                TimeUnit.SECONDS.sleep(1);
            }
            count++;
        }
    }
}
