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
import org.apache.inlong.sdk.dataproxy.MsgSenderMultiFactory;
import org.apache.inlong.sdk.dataproxy.MsgSenderSingleFactory;
import org.apache.inlong.sdk.dataproxy.common.ProcessResult;
import org.apache.inlong.sdk.dataproxy.sender.http.HttpMsgSenderConfig;
import org.apache.inlong.sdk.dataproxy.sender.http.InLongHttpMsgSender;
import org.apache.inlong.sdk.dataproxy.sender.tcp.InLongTcpMsgSender;
import org.apache.inlong.sdk.dataproxy.sender.tcp.TcpMsgSenderConfig;
import org.apache.inlong.sdk.dataproxy.utils.ProxyUtils;

import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;

public class InLongFactoryExample {

    protected static final Logger logger = LoggerFactory.getLogger(InLongFactoryExample.class);

    public static void main(String[] args) throws Exception {

        String managerIp = args[0];
        int managerPort = Integer.parseInt(args[1]);
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

        System.out.println("InLongFactoryExample start");

        // build singleton factory
        MsgSenderSingleFactory singleFactory = new MsgSenderSingleFactory();
        // report data by tcp
        TcpMsgSenderConfig tcpMsgSenderConfig = new TcpMsgSenderConfig(
                false, managerIp, managerPort, groupId, secretId, secretKey);
        tcpMsgSenderConfig.setRequestTimeoutMs(20000L);
        InLongTcpMsgSender tcpMsgSender =
                singleFactory.genTcpSenderByClusterId(tcpMsgSenderConfig);
        ProcessResult procResult = new ProcessResult();
        if (!tcpMsgSender.start(procResult)) {
            System.out.println("Start tcp sender failure: process result=" + procResult);
        }

        // report data
        ExampleUtils.sendTcpMessages(tcpMsgSender, false, false,
                groupId, streamId, reqCnt, msgSize, msgCnt, procResult);
        ExampleUtils.sendTcpMessages(tcpMsgSender, false, true,
                groupId, streamId, reqCnt, msgSize, msgCnt, procResult);
        ProxyUtils.sleepSomeTime(10000L);
        tcpMsgSender.close();

        // report data by http
        HttpMsgSenderConfig httpMsgSenderConfig = new HttpMsgSenderConfig(
                false, managerIp, managerPort, groupId, secretId, secretKey);
        InLongHttpMsgSender httpMsgSender =
                singleFactory.genHttpSenderByGroupId(httpMsgSenderConfig);
        if (!httpMsgSender.start(procResult)) {
            System.out.println("Start http sender failure: process result=" + procResult);
        }
        ExampleUtils.sendHttpMessages(httpMsgSender, false, false,
                groupId, streamId, reqCnt, msgSize, msgCnt, procResult);
        ExampleUtils.sendHttpMessages(httpMsgSender, false, true,
                groupId, streamId, reqCnt, msgSize, msgCnt, procResult);
        ProxyUtils.sleepSomeTime(10000L);
        httpMsgSender.close();
        System.out.println("Cur singleton factory sender count is " + singleFactory.getMsgSenderCount());

        // report data use multi-factory
        MsgSenderMultiFactory multiFactory1 = new MsgSenderMultiFactory();
        MsgSenderMultiFactory multiFactory2 = new MsgSenderMultiFactory();
        // report data by tcp
        tcpMsgSenderConfig.setSdkMsgType(MsgType.MSG_ACK_SERVICE);
        InLongTcpMsgSender tcpMsgSender1 =
                multiFactory1.genTcpSenderByGroupId(tcpMsgSenderConfig);
        if (!tcpMsgSender1.start(procResult)) {
            System.out.println("Start tcp sender1 failure: process result=" + procResult);
        }

        String managerAddr = "http://" + managerIp + ":" + managerPort;
        TcpMsgSenderConfig tcpMsgSenderConfig2 =
                new TcpMsgSenderConfig(managerAddr, groupId, secretId, secretKey);
        tcpMsgSenderConfig2.setSdkMsgType(MsgType.MSG_MULTI_BODY);
        InLongTcpMsgSender tcpMsgSender2 =
                multiFactory2.genTcpSenderByGroupId(tcpMsgSenderConfig2);
        ExampleUtils.sendTcpMessages(tcpMsgSender1, false, false,
                groupId, streamId, reqCnt, msgSize, msgCnt, procResult);
        ExampleUtils.sendTcpMessages(tcpMsgSender2, false, true,
                groupId, streamId, reqCnt, msgSize, msgCnt, procResult);
        ProxyUtils.sleepSomeTime(10000L);
        tcpMsgSender1.close();
        System.out.println("Multi-1.1 Cur multiFactory1 sender count = "
                + multiFactory1.getMsgSenderCount()
                + ", cur multiFactory2 sender count is " + multiFactory2.getMsgSenderCount());
        tcpMsgSender2.close();
        System.out.println("Multi-1.2 Cur multiFactory1 sender count = "
                + multiFactory1.getMsgSenderCount()
                + ", cur multiFactory2 sender count is " + multiFactory2.getMsgSenderCount());
        // report data by http
        InLongHttpMsgSender httpMsgSender1 =
                multiFactory1.genHttpSenderByGroupId(httpMsgSenderConfig);
        HttpMsgSenderConfig httpConfg2 = new HttpMsgSenderConfig(false,
                managerIp, managerPort, groupId, secretId, secretKey);
        InLongHttpMsgSender httpMsgSender2 =
                multiFactory2.genHttpSenderByGroupId(httpConfg2);
        ExampleUtils.sendHttpMessages(httpMsgSender1, false, false,
                groupId, streamId, reqCnt, msgSize, msgCnt, procResult);
        ExampleUtils.sendHttpMessages(httpMsgSender2, false, true,
                groupId, streamId, reqCnt, msgSize, msgCnt, procResult);
        ProxyUtils.sleepSomeTime(10000L);
        httpMsgSender1.close();
        System.out.println("Multi-2.1 Cur multiFactory1 sender count = "
                + multiFactory1.getMsgSenderCount()
                + ", cur multiFactory2 sender count is " + multiFactory2.getMsgSenderCount());
        httpMsgSender2.close();
        System.out.println("Multi-2.2 Cur multiFactory1 sender count = "
                + multiFactory1.getMsgSenderCount()
                + ", cur multiFactory2 sender count is " + multiFactory2.getMsgSenderCount());

        // test self DefineFactory
        ThreadFactory selfDefineFactory = new DefaultThreadFactory("test_self_thread_factory");
        InLongTcpMsgSender tcpMsgSelfSender =
                singleFactory.genTcpSenderByGroupId(tcpMsgSenderConfig, selfDefineFactory);
        ExampleUtils.sendTcpMessages(tcpMsgSelfSender, false, false,
                groupId, streamId, reqCnt, msgSize, msgCnt, procResult);
        ProxyUtils.sleepSomeTime(10000L);

        tcpMsgSelfSender.close();

        System.out.println("singleFactory-3 Cur singleFactory sender count = "
                + singleFactory.getMsgSenderCount());

        ProxyUtils.sleepSomeTime(3 * 60 * 1000L);

        // close all
        multiFactory1.shutdownAll();
        multiFactory2.shutdownAll();
    }
}
