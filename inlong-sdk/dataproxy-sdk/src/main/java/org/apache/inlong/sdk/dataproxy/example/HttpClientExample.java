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

package org.apache.inlong.sdk.dataproxy.example;

import org.apache.inlong.sdk.dataproxy.exception.ProxySdkException;
import org.apache.inlong.sdk.dataproxy.network.HttpProxySender;
import org.apache.inlong.sdk.dataproxy.sender.http.HttpMsgSenderConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class HttpClientExample {

    public static void main(String[] args) {
        String inlongGroupId = "test_group_id";
        String inlongStreamId = "test_stream_id";
        String configBasePath = "";
        String inLongManagerAddr = "127.0.0.1";
        String inLongManagerPort = "8083";
        String messageBody = "inlong message body!";

        HttpProxySender sender = getMessageSender(inLongManagerAddr,
                inLongManagerPort, inlongGroupId, true, false,
                configBasePath);
        sendHttpMessage(sender, inlongGroupId, inlongStreamId, messageBody);
        sender.close(); // close the sender
    }

    public static HttpProxySender getMessageSender(String inLongManagerAddr,
            String inLongManagerPort, String inlongGroupId,
            boolean requestByHttp, boolean isReadProxyIPFromLocal,
            String configBasePath) {
        HttpMsgSenderConfig httpConfig = null;
        HttpProxySender sender = null;
        try {
            httpConfig = new HttpMsgSenderConfig(requestByHttp, inLongManagerAddr,
                    Integer.valueOf(inLongManagerPort),
                    inlongGroupId, "admin", "inlong");// user and password of manager
            httpConfig.setMetaStoreBasePath(configBasePath);
            httpConfig.setOnlyUseLocalProxyConfig(isReadProxyIPFromLocal);
            httpConfig.setDiscardHttpCacheWhenClosing(true);
            sender = new HttpProxySender(httpConfig);
        } catch (ProxySdkException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sender;
    }

    public static void sendHttpMessage(HttpProxySender sender, String inlongGroupId,
            String inlongStreamId, String messageBody) {
        List<String> bodyList = new ArrayList<>();
        bodyList.add(messageBody);
        sender.asyncSendMessage(bodyList, inlongGroupId, inlongStreamId, System.currentTimeMillis(),
                20, TimeUnit.SECONDS, new MyMessageCallBack());
    }
}
