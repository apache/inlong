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
import org.apache.inlong.sdk.dataproxy.sender.http.HttpMsgSenderConfig;
import org.apache.inlong.sdk.dataproxy.sender.http.InLongHttpMsgSender;
import org.apache.inlong.sdk.dataproxy.utils.ProxyUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InLongHttpClientExample {

    protected static final Logger logger = LoggerFactory.getLogger(InLongHttpClientExample.class);

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
        HttpMsgSenderConfig dataProxyConfig =
                new HttpMsgSenderConfig(managerAddr, groupId, secretId, secretKey);
        InLongHttpMsgSender messageSender = new InLongHttpMsgSender(dataProxyConfig);
        ProcessResult procResult = new ProcessResult();
        if (!messageSender.start(procResult)) {
            messageSender.close();
            System.out.println("Start http sender failure: process result=" + procResult);
            return;
        }

        System.out.println("InLongHttpMsgSender start, nodes="
                + messageSender.getProxyNodeInfos());
        ExampleUtils.sendHttpMessages(messageSender, true, false,
                groupId, streamId, reqCnt, msgSize, msgCnt, procResult);
        ExampleUtils.sendHttpMessages(messageSender, true, true,
                groupId, streamId, reqCnt, msgSize, msgCnt, procResult);
        ExampleUtils.sendHttpMessages(messageSender, false, false,
                groupId, streamId, reqCnt, msgSize, msgCnt, procResult);
        ExampleUtils.sendHttpMessages(messageSender, false, true,
                groupId, streamId, reqCnt, msgSize, msgCnt, procResult);

        ProxyUtils.sleepSomeTime(10000L);
    }
}
