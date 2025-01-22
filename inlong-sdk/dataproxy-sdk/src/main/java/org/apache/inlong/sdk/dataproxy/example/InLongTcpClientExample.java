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

import org.apache.inlong.sdk.dataproxy.common.ProcessResult;
import org.apache.inlong.sdk.dataproxy.sender.tcp.InLongTcpMsgSender;
import org.apache.inlong.sdk.dataproxy.sender.tcp.TcpMsgSenderConfig;
import org.apache.inlong.sdk.dataproxy.utils.ProxyUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InLongTcpClientExample {

    protected static final Logger logger = LoggerFactory.getLogger(InLongTcpClientExample.class);

    public static void main(String[] args) throws Exception {

        String managerIp = args[0];
        String managerPort = args[1];
        String groupId = args[2];
        String streamId = args[3];
        String secretId = args[4];
        String secretKey = args[5];
        int reqCnt = Integer.parseInt(args[6]);
        int msgSize = 1024;
        int msgCnt = 1;
        if (args.length > 7) {
            msgSize = Integer.parseInt(args[7]);
            msgCnt = Integer.parseInt(args[8]);
        }

        String managerAddr = "http://" + managerIp + ":" + managerPort;

        TcpMsgSenderConfig dataProxyConfig =
                new TcpMsgSenderConfig(managerAddr, groupId, secretId, secretKey);
        dataProxyConfig.setRequestTimeoutMs(20000L);
        InLongTcpMsgSender messageSender = new InLongTcpMsgSender(dataProxyConfig);

        logger.info("InLongTcpMsgSender start");

        ProcessResult procResult = new ProcessResult();
        if (!messageSender.start(procResult)) {
            System.out.println("Start sender failure: process result=" + procResult.toString());
        }
        ExampleUtils.sendTcpMessages(messageSender, true, false,
                groupId, streamId, reqCnt, msgSize, msgCnt, procResult);
        ExampleUtils.sendTcpMessages(messageSender, true, true,
                groupId, streamId, reqCnt, msgSize, msgCnt, procResult);
        ExampleUtils.sendTcpMessages(messageSender, false, false,
                groupId, streamId, reqCnt, msgSize, msgCnt, procResult);
        ExampleUtils.sendTcpMessages(messageSender, false, true,
                groupId, streamId, reqCnt, msgSize, msgCnt, procResult);
        ProxyUtils.sleepSomeTime(10000L);
    }
}
