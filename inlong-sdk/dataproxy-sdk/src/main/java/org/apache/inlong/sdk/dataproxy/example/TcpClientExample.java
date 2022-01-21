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
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.StringUtils;
import org.apache.inlong.sdk.dataproxy.DefaultMessageSender;
import org.apache.inlong.sdk.dataproxy.ProxyClientConfig;
import org.apache.inlong.sdk.dataproxy.SendMessageCallback;
import org.apache.inlong.sdk.dataproxy.SendResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TcpClientExample {

    private static final Logger logger = LoggerFactory.getLogger(TcpClientExample.class);

    public static String localIP = "127.0.0.1";

    private int curCount = 0;

    public static void main() {

        String dataProxyGroup = "test_test";
        String groupId = "test_test";
        String streamId = "test_test";
        String netTag = "";

        /*
         * 1. if isLocalVisit is true, will get dataproxy server info from local file in
         * ${configBasePath}/${dataProxyGroup}.local file
         *
         * for example:
         *  /data/inlong/config/test_test.local
         * and file context like this:
         * {"isInterVisit":1,"cluster_id":"1","size":1,"switch":1,"address":[{"host":"127.0.0.1",
         * "port":"46802"},{"host":"127.0.0.1","port":"46802"}]}
         * 2. if isLocalVisit is false, will get dataproxy server info from manager
         * so we must ensure that the manager server url is configured correctly!
         */
        boolean isLocalVisit = true;
        String configBasePath = "/data/inlong/config";

        String inLongManagerAddr = "127.0.0.1";
        String inLongManagerPort = "8000";

        /*
         * It is recommended to use type 7. For others, please refer to the official related documents
         */
        int msgType = 7;

        /*
         * 0 : single msg/once and sync
         * 2 : multi-messages/once and sync
         * 3 : single msg/once and async
         * 4 : multi-messages/once and async
         */
        int sendType = 3;
        int bodySize = 1024;
        String randomMessageBody = "inglong-message-random-body!";
        int sendCount = 10;

        TcpClientExample tcpClientExample = new TcpClientExample();
        DefaultMessageSender sender = tcpClientExample
                .getMessageSender(inLongManagerAddr, inLongManagerPort, groupId, netTag,
                        dataProxyGroup, isLocalVisit, configBasePath, msgType);
        tcpClientExample
                .sendMessage(sender, sendType, randomMessageBody, bodySize, groupId, streamId,
                        sendCount);
    }

    public DefaultMessageSender getMessageSender(String inLongManagerAddr, String inLongManagerPort,
            String groupId, String netTag, String dataProxyGroup, boolean isLocalVisit,
            String configBasePath, int msgtype) {
        ProxyClientConfig dataProxyConfig = null;
        DefaultMessageSender messageSender = null;
        try {
            dataProxyConfig = new ProxyClientConfig(localIP, isLocalVisit, inLongManagerAddr,
                    Integer.valueOf(inLongManagerPort), groupId, netTag);
            dataProxyConfig.setAliveConnections(3);
            if (StringUtils.isNotEmpty(configBasePath)) {
                dataProxyConfig.setConfStoreBasePath(configBasePath);
            }
            dataProxyConfig.setGroupId(dataProxyGroup);
            dataProxyConfig.setReadProxyIPFromLocal(true);
            messageSender = DefaultMessageSender.generateSenderByClusterId(dataProxyConfig);
            messageSender.setSupportLF(false);
            messageSender.setMsgtype(msgtype);
        } catch (Exception e) {
            logger.error("getMessageSender has exception e = {}", e);
        }
        return messageSender;
    }

    public void sendMessage(DefaultMessageSender messageSender, int sendType,
            String randomMessageBody, int bodysize, String groupId, String streamId,
            int sentCount) {
        SendResult result = null;
        long start = System.currentTimeMillis();
        ArrayList<byte[]> bodylist = new ArrayList<byte[]>();
        Map<String, String> extraAttrMap = new HashMap<>();
        curCount = 0;
        do {
            start = System.currentTimeMillis();
            try {
                switch (sendType) {
                    case 0: {
                        String messageBody = getRandomString(randomMessageBody, bodysize);
                        result = messageSender
                                .sendMessage(messageBody.getBytes("utf8"),
                                        groupId, streamId, 0,
                                        String.valueOf(System.currentTimeMillis()), 20,
                                        TimeUnit.SECONDS);
                        if (result == SendResult.OK) {
                            logger.info("messageSender {} costTime{}", curCount,
                                    System.currentTimeMillis() - start);
                        }
                    }
                    break;

                    case 1: {
                        String messageBody = getRandomString(randomMessageBody, bodysize);
                        for (int i = 0; i < 5; i++) {
                            bodylist.add(messageBody.getBytes("utf8"));
                            messageBody = getRandomString(randomMessageBody, bodysize);
                        }
                        try {
                            result = messageSender.sendMessage(bodylist, groupId, streamId, 0,
                                    String.valueOf(System.currentTimeMillis()), 20,
                                    TimeUnit.SECONDS, extraAttrMap);
                        } catch (Exception ex) {
                            logger.info("messageSender " + curCount + " " +
                                    (System.currentTimeMillis() - start) + " " + result);
                        }
                        result = messageSender.sendMessage(bodylist, groupId, streamId, 0,
                                String.valueOf(System.currentTimeMillis()), 20,
                                TimeUnit.SECONDS);
                        logger.info("messageSender " + curCount + " " +
                                (System.currentTimeMillis() - start) + " " + result);
                        bodylist.clear();
                    }
                    break;

                    case 2: {
                        SendMessageCallback mySendMessageCallBack = new MyMessageCallBack();
                        String messageBody = getRandomString(randomMessageBody, bodysize);
                        for (int i = 0; i < 5; i++) {
                            bodylist.add(messageBody.getBytes("utf8"));
                            messageBody = getRandomString(randomMessageBody, bodysize);
                        }
                        messageSender.asyncSendMessage(mySendMessageCallBack, bodylist, groupId,
                                streamId, 0, String.valueOf(System.currentTimeMillis()),
                                20, TimeUnit.SECONDS);
                        bodylist.clear();
                    }
                    break;

                    case 3: {
                        String messageBody = getRandomString(randomMessageBody, bodysize);
                        SendMessageCallback mySendMessageCallBack = new SendMessageCallback() {
                            @Override
                            public void onMessageAck(SendResult sendResult) {
                                logger.info("messageSender count=" + String.valueOf(curCount)
                                        + ",cost time=" + String.valueOf(System.currentTimeMillis())
                                        + ",result=" + sendResult);
                            }

                            @Override
                            public void onException(Throwable throwable) {
                                logger.info("messageSender count=" + String.valueOf(curCount)
                                        + ",cost time=" + String.valueOf(System.currentTimeMillis())
                                        + ",result=" + throwable);
                            }
                        };
                        messageSender.asyncSendMessage(mySendMessageCallBack,
                                messageBody.getBytes("utf8"), groupId, streamId, 0,
                                String.valueOf(System.currentTimeMillis() / 1000), 20,
                                TimeUnit.SECONDS);

                    }
                    break;

                    case 4: {
                        String messageBody = getRandomString(randomMessageBody, bodysize);
                        String result2 = messageSender
                                .sendMessageFile(messageBody.getBytes("utf8"),
                                        groupId, "test",
                                        System.currentTimeMillis(),
                                        (int) (System.currentTimeMillis() / 1000),
                                        String.valueOf(System.currentTimeMillis()), 20,
                                        TimeUnit.SECONDS);
                        logger.info("messageSender count= {} ,cost time= {}, result= " + result2,
                                String.valueOf(curCount),
                                String.valueOf(System.currentTimeMillis() - start));
                    }
                    break;

                    case 5: {
                        MyFileCallBack mySendMessageCallBack = new MyFileCallBack();
                        String messageBody = getRandomString(randomMessageBody, bodysize);
                        messageSender.asyncsendMessageProxy(mySendMessageCallBack,
                                messageBody.getBytes("utf8"), groupId, streamId, 0,
                                (int) System.currentTimeMillis() / 1000, localIP,
                                String.valueOf(System.currentTimeMillis()), 20,
                                TimeUnit.SECONDS);
                        logger.info(
                                "messageSender count=" + String.valueOf(curCount) + ",cost time="
                                        + String.valueOf(System.currentTimeMillis() - start));
                    }
                    break;

                    default: {
                        String messageBody = getRandomString(randomMessageBody, bodysize);
                        for (int i = 0; i < 5; i++) {
                            bodylist.add(messageBody.getBytes("utf8"));
                            messageBody = getRandomString(randomMessageBody, bodysize);
                        }
                        SendMessageCallback mySendMessageCallBack = new MyMessageCallBack();
                        messageSender.asyncSendMessage(mySendMessageCallBack, bodylist, groupId,
                                streamId, 0, String.valueOf(System.currentTimeMillis()),
                                20, TimeUnit.SECONDS);
                        bodylist.clear();
                    }
                    break;
                }
            } catch (Throwable e1) {
                logger.error("Sent message failure, type={}, msg {}", sendType, e1);
                e1.printStackTrace();
            }
            curCount++;
        } while (curCount <= sentCount);
        logger.error("Sent message finished, sentCount={}", curCount);
        try {
            Thread.sleep(100000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static String getRandomString(String messageRandomBody, int length) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int number = new Random().nextInt(messageRandomBody.length());
            sb.append(messageRandomBody.charAt(number));
        }
        return sb.toString();
    }

}
