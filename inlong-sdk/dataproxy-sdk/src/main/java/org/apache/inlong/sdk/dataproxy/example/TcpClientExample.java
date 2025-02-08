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

import org.apache.inlong.common.msg.MsgType;
import org.apache.inlong.sdk.dataproxy.MsgSenderFactory;
import org.apache.inlong.sdk.dataproxy.MsgSenderSingleFactory;
import org.apache.inlong.sdk.dataproxy.common.ProcessResult;
import org.apache.inlong.sdk.dataproxy.sender.tcp.TcpEventInfo;
import org.apache.inlong.sdk.dataproxy.sender.tcp.TcpMsgSender;
import org.apache.inlong.sdk.dataproxy.sender.tcp.TcpMsgSenderConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

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
        int msgType = 7; // default report type
        String messageBody = "inglong-message-random-body!";

        // build sender factory
        MsgSenderSingleFactory senderFactory = new MsgSenderSingleFactory();
        // build sender object
        TcpClientExample tcpClientExample = new TcpClientExample();
        TcpMsgSender sender = tcpClientExample.getMessageSender(senderFactory, false,
                inLongManagerAddr, inLongManagerPort, inlongGroupId, msgType, false, configBasePath);
        // send message
        tcpClientExample.sendTcpMessage(sender,
                inlongGroupId, inlongStreamId, System.currentTimeMillis(), messageBody);
        // close all senders
        senderFactory.shutdownAll();
    }

    public TcpMsgSender getMessageSender(MsgSenderFactory senderFactory, boolean visitMgrByHttps,
            String managerAddr, String managerPort, String inlongGroupId, int msgType,
            boolean useLocalMetaConfig, String configBasePath) {
        TcpMsgSender messageSender = null;
        try {
            // build sender configure
            TcpMsgSenderConfig tcpConfig =
                    new TcpMsgSenderConfig(visitMgrByHttps, managerAddr,
                            Integer.parseInt(managerPort), inlongGroupId, "admin", "inlong");
            tcpConfig.setMetaStoreBasePath(configBasePath);
            tcpConfig.setOnlyUseLocalProxyConfig(useLocalMetaConfig);
            tcpConfig.setSdkMsgType(MsgType.valueOf(msgType));
            tcpConfig.setRequestTimeoutMs(20000L);
            // build sender object
            messageSender = senderFactory.genTcpSenderByClusterId(tcpConfig);
        } catch (Throwable ex) {
            System.out.println("Get MessageSender throw exception, " + ex);
        }
        return messageSender;
    }

    public void sendTcpMessage(TcpMsgSender sender,
            String inlongGroupId, String inlongStreamId, long dt, String messageBody) {
        ProcessResult procResult = new ProcessResult();
        try {
            sender.sendMessage(new TcpEventInfo(inlongGroupId, inlongStreamId,
                    dt, null, messageBody.getBytes(StandardCharsets.UTF_8)), procResult);
        } catch (Throwable ex) {
            System.out.println("Message sent throw exception, " + ex);
            return;
        }
        System.out.println("Message sent result = " + procResult);
        logger.info("Message sent result = {}", procResult);
    }
}
