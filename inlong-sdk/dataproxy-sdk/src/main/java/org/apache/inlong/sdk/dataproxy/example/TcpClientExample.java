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

import org.apache.inlong.sdk.dataproxy.DefaultMessageSender;
import org.apache.inlong.sdk.dataproxy.TcpMsgSenderConfig;
import org.apache.inlong.sdk.dataproxy.common.SendResult;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;

public class TcpClientExample {

    private static final Logger logger = LoggerFactory.getLogger(TcpClientExample.class);

    /**
     * Example of client tcp.
     */
    public static void main(String[] args) throws InterruptedException {

        String inlongGroupId = "test_group_id";
        String inlongStreamId = "test_stream_id";

        String configBasePath = "";
        String inLongManagerAddr = "127.0.0.1";
        String inLongManagerPort = "8083";

        /*
         * It is recommended to use type 7. For others, please refer to the official related documents
         */
        int msgType = 7;
        String messageBody = "inglong-message-random-body!";

        TcpClientExample tcpClientExample = new TcpClientExample();
        DefaultMessageSender sender = tcpClientExample
                .getMessageSender(inLongManagerAddr, inLongManagerPort,
                        inlongGroupId, true, false, configBasePath, msgType);
        tcpClientExample.sendTcpMessage(sender, inlongGroupId, inlongStreamId,
                messageBody, System.currentTimeMillis());
        sender.close(); // close the sender
    }

    public DefaultMessageSender getMessageSender(String inLongManagerAddr, String inLongManagerPort,
            String inlongGroupId, boolean requestByHttp, boolean isReadProxyIPFromLocal,
            String configBasePath, int msgType) {
        TcpMsgSenderConfig tcpConfig = null;
        DefaultMessageSender messageSender = null;
        try {
            tcpConfig = new TcpMsgSenderConfig(requestByHttp, inLongManagerAddr,
                    Integer.valueOf(inLongManagerPort), inlongGroupId, "admin", "inlong");
            if (StringUtils.isNotEmpty(configBasePath)) {
                tcpConfig.setMetaStoreBasePath(configBasePath);
            }
            tcpConfig.setOnlyUseLocalProxyConfig(isReadProxyIPFromLocal);
            tcpConfig.setRequestTimeoutMs(20000L);
            messageSender = DefaultMessageSender.generateSenderByClusterId(tcpConfig);
            messageSender.setMsgtype(msgType);
        } catch (Exception e) {
            logger.error("getMessageSender has exception e = {}", e);
        }
        return messageSender;
    }

    public void sendTcpMessage(DefaultMessageSender sender, String inlongGroupId,
            String inlongStreamId, String messageBody, long dt) {
        SendResult result = null;
        try {
            result = sender.sendMessage(messageBody.getBytes("utf8"), inlongGroupId, inlongStreamId,
                    0, String.valueOf(dt));

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        System.out.println("messageSender" + result);
        logger.info("messageSender {}", result);
    }

}
