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

import org.apache.inlong.sdk.dataproxy.MsgSenderFactory;
import org.apache.inlong.sdk.dataproxy.MsgSenderSingleFactory;
import org.apache.inlong.sdk.dataproxy.common.ProcessResult;
import org.apache.inlong.sdk.dataproxy.sender.MsgSendCallback;
import org.apache.inlong.sdk.dataproxy.sender.http.HttpEventInfo;
import org.apache.inlong.sdk.dataproxy.sender.http.HttpMsgSender;
import org.apache.inlong.sdk.dataproxy.sender.http.HttpMsgSenderConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpClientExample {

    private static final Logger logger = LoggerFactory.getLogger(HttpClientExample.class);

    public static void main(String[] args) {
        String inlongGroupId = "test_group_id";
        String inlongStreamId = "test_stream_id";
        String configBasePath = "";
        String inLongManagerAddr = "127.0.0.1";
        String inLongManagerPort = "8083";
        String messageBody = "inlong message body!";

        // build sender factory
        MsgSenderSingleFactory senderFactory = new MsgSenderSingleFactory();
        // build sender object
        HttpMsgSender sender = getMessageSender(senderFactory, false,
                inLongManagerAddr, inLongManagerPort, inlongGroupId, false, configBasePath);
        // send message
        sendHttpMessage(sender, inlongGroupId, inlongStreamId, messageBody);
        // close all senders
        sender.close();
    }

    public static HttpMsgSender getMessageSender(MsgSenderFactory senderFactory, boolean visitMsgByHttps,
            String managerAddr, String managerPort, String inlongGroupId, boolean useLocalMetaConfig,
            String configBasePath) {
        HttpMsgSender sender = null;
        try {
            HttpMsgSenderConfig httpConfig = new HttpMsgSenderConfig(visitMsgByHttps, managerAddr,
                    Integer.parseInt(managerPort), inlongGroupId, "admin", "inlong");
            httpConfig.setDiscardHttpCacheWhenClosing(true);
            httpConfig.setMetaStoreBasePath(configBasePath);
            httpConfig.setOnlyUseLocalProxyConfig(useLocalMetaConfig);
            httpConfig.setHttpConTimeoutMs(20000);
            sender = senderFactory.genHttpSenderByGroupId(httpConfig);
        } catch (Throwable ex) {
            System.out.println("Get MessageSender throw exception, " + ex);
        }
        return sender;
    }

    public static void sendHttpMessage(HttpMsgSender sender,
            String inlongGroupId, String inlongStreamId, String messageBody) {
        try {
            ProcessResult procResult = new ProcessResult();
            if (!sender.asyncSendMessage(new HttpEventInfo(inlongGroupId,
                    inlongStreamId, System.currentTimeMillis(), messageBody), new MyMessageCallBack(), procResult)) {
                System.out.println("Send message failure, result = " + procResult);
                return;
            }
            System.out.println("Send message success!");
        } catch (Throwable ex) {
            System.out.println("Send message exception" + ex);
        }
    }

    // async callback class
    public static class MyMessageCallBack implements MsgSendCallback {

        private HttpMsgSender messageSender = null;
        private HttpEventInfo event = null;

        public MyMessageCallBack() {

        }

        public MyMessageCallBack(HttpMsgSender messageSender, HttpEventInfo event) {
            this.messageSender = messageSender;
            this.event = event;
        }

        @Override
        public void onMessageAck(ProcessResult result) {
            if (result.isSuccess()) {
                logger.info("onMessageAck return Ok");
            } else {
                logger.info("onMessageAck return failure = {}", result);
            }
        }

        @Override
        public void onException(Throwable ex) {
            logger.error("Send message throw exception", ex);
        }
    }
}
