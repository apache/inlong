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
import org.apache.inlong.sdk.dataproxy.sender.MsgSendCallback;
import org.apache.inlong.sdk.dataproxy.sender.http.HttpEventInfo;
import org.apache.inlong.sdk.dataproxy.sender.http.HttpMsgSender;
import org.apache.inlong.sdk.dataproxy.sender.tcp.TcpEventInfo;
import org.apache.inlong.sdk.dataproxy.sender.tcp.TcpMsgSender;
import org.apache.inlong.sdk.dataproxy.utils.ProxyUtils;

import org.apache.commons.codec.binary.StringUtils;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExampleUtils {

    private static final SecureRandom cntRandom = new SecureRandom(
            Long.toString(System.nanoTime()).getBytes());

    public static void sendTcpMessages(TcpMsgSender msgSender, boolean isSync, boolean isMultiItem,
            String groupId, String streamId, int reqCnt, int baseBodyLen, int msgItemCnt, ProcessResult procResult) {
        int sucCnt = 0;
        int curCount = 0;
        TcpEventInfo eventInfo;
        byte[] itemBody = buildBoydData(baseBodyLen);
        List<byte[]> multiBodys = new ArrayList<>();
        for (int i = 0; i < msgItemCnt; i++) {
            multiBodys.add(itemBody);
        }
        Map<String, String> localAttrs = new HashMap<>();
        if (isSync) {
            if (isMultiItem) {
                // send single message
                while (curCount++ < reqCnt) {
                    Map<String, String> filteredAttrs;
                    try {
                        if (curCount > 1) {
                            localAttrs.clear();
                            localAttrs.put("index", String.valueOf(curCount));
                        }
                        filteredAttrs = ProxyUtils.getValidAttrs(localAttrs);
                        eventInfo = new TcpEventInfo(groupId, streamId,
                                System.currentTimeMillis(), filteredAttrs, multiBodys);
                    } catch (Throwable ex) {
                        System.out.println("Build tcp event failure, ex=" + ex);
                        continue;
                    }
                    if (!msgSender.sendMessage(eventInfo, procResult)) {
                        System.out.println("Sync request index=" + curCount + ", process result=" + procResult);
                        continue;
                    }
                    curCount++;
                }
            } else {
                // send single message
                while (curCount++ < reqCnt) {
                    try {
                        if (curCount > 1) {
                            localAttrs.clear();
                            localAttrs.put("index", String.valueOf(curCount));
                            localAttrs.put("multi", String.valueOf(false));
                        }
                        eventInfo = new TcpEventInfo(groupId, streamId,
                                System.currentTimeMillis(), localAttrs, itemBody);
                    } catch (Throwable ex) {
                        System.out.println("Build tcp event failure, ex=" + ex);
                        continue;
                    }
                    if (!msgSender.sendMessage(eventInfo, procResult)) {
                        System.out.println("Sync request index=" + curCount + ", process result=" + procResult);
                        continue;
                    }
                    curCount++;
                }
            }
        } else {
            if (isMultiItem) {
                // send multiple message
                while (curCount++ < reqCnt) {
                    try {
                        if (curCount > 1) {
                            localAttrs.clear();
                            localAttrs.put("index", String.valueOf(curCount));
                            localAttrs.put("multi", String.valueOf(true));
                        }
                        eventInfo = new TcpEventInfo(groupId, streamId,
                                System.currentTimeMillis(), localAttrs, multiBodys);
                    } catch (Throwable ex) {
                        System.out.println("Build multiple tcp event failure, ex=" + ex);
                        continue;
                    }
                    if (!msgSender.asyncSendMessage(eventInfo, new MyMsgSendBack(curCount), procResult)) {
                        System.out.println("Async request index=" + curCount + ", post result=" + procResult);
                        continue;
                    }
                    curCount++;
                }
            } else {
                // send single message
                while (curCount++ < reqCnt) {
                    try {
                        eventInfo = new TcpEventInfo(groupId, streamId,
                                System.currentTimeMillis(), null, itemBody);
                    } catch (Throwable ex) {
                        System.out.println("Build tcp event failure, ex=" + ex);
                        continue;
                    }
                    if (!msgSender.asyncSendMessage(eventInfo, new MyMsgSendBack(curCount), procResult)) {
                        System.out.println("Async request index=" + curCount + ", post result=" + procResult);
                        continue;
                    }
                    curCount++;
                }
            }
        }
    }

    public static void sendHttpMessages(HttpMsgSender msgSender, boolean isSync, boolean isMultiItem,
            String groupId, String streamId, int reqCnt, int baseBodyLen, int msgItemCnt, ProcessResult procResult) {
        int sucCnt = 0;
        int curCount = 0;
        HttpEventInfo eventInfo;
        String itemBody = getRandomString(baseBodyLen);
        List<String> multiBodys = new ArrayList<>();
        for (int i = 0; i < msgItemCnt; i++) {
            multiBodys.add(itemBody);
        }
        if (isSync) {
            if (isMultiItem) {
                // send multiple message
                while (curCount++ < reqCnt) {
                    try {
                        eventInfo = new HttpEventInfo(groupId, streamId,
                                System.currentTimeMillis(), multiBodys);
                    } catch (Throwable ex) {
                        System.out.println("Build multiple http event failure, ex=" + ex);
                        continue;
                    }
                    if (!msgSender.syncSendMessage(eventInfo, procResult)) {
                        System.out.println("Sync request index=" + curCount + ", process result=" + procResult);
                        continue;
                    }
                    curCount++;
                }
            } else {
                // send single message
                while (curCount++ < reqCnt) {
                    try {
                        eventInfo = new HttpEventInfo(groupId, streamId,
                                System.currentTimeMillis(), itemBody);
                    } catch (Throwable ex) {
                        System.out.println("Build single http event failure, ex=" + ex);
                        continue;
                    }
                    if (!msgSender.syncSendMessage(eventInfo, procResult)) {
                        System.out.println("Sync request index=" + curCount + ", process result=" + procResult);
                        continue;
                    }
                    curCount++;
                }
            }
        } else {
            if (isMultiItem) {
                // send multiple message
                while (curCount++ < reqCnt) {
                    try {
                        eventInfo = new HttpEventInfo(groupId, streamId,
                                System.currentTimeMillis(), multiBodys);
                    } catch (Throwable ex) {
                        System.out.println("Build multiple http event failure, ex=" + ex);
                        continue;
                    }
                    if (!msgSender.asyncSendMessage(eventInfo, new MyMsgSendBack(curCount), procResult)) {
                        System.out.println("Async request index=" + curCount + ", post result=" + procResult);
                        continue;
                    }
                    curCount++;
                }
            } else {
                // send single message
                while (curCount++ < reqCnt) {
                    try {
                        eventInfo = new HttpEventInfo(groupId, streamId, System.currentTimeMillis(), itemBody);
                    } catch (Throwable ex) {
                        System.out.println("Build single http event failure, ex=" + ex);
                        continue;
                    }
                    if (!msgSender.asyncSendMessage(eventInfo, new MyMsgSendBack(curCount), procResult)) {
                        System.out.println("Async request: index=" + curCount + ", post result=" + procResult);
                        continue;
                    }
                    curCount++;
                }
            }
        }
    }

    private static String getRandomString(int length) {
        String strBase = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            int number = cntRandom.nextInt(strBase.length());
            sb.append(strBase.charAt(number));
        }
        return sb.toString();
    }

    private static byte[] buildBoydData(int bodySize) {
        final byte[] itemBaseData =
                StringUtils.getBytesUtf8("inglong tcp test data!");
        final ByteBuffer dataBuffer = ByteBuffer.allocate(bodySize);
        while (dataBuffer.hasRemaining()) {
            int offset = dataBuffer.arrayOffset();
            dataBuffer.put(itemBaseData, offset,
                    Math.min(dataBuffer.remaining(), itemBaseData.length));
        }
        dataBuffer.flip();
        return dataBuffer.array();
    }

    private static class MyMsgSendBack implements MsgSendCallback {

        private final int msgId;

        public MyMsgSendBack(int msgId) {
            this.msgId = msgId;
        }

        @Override
        public void onMessageAck(ProcessResult result) {
            // System.out.println("msgId=" + msgId + ", send result = " + result);
        }

        @Override
        public void onException(Throwable ex) {
            System.out.println("msgId=" + msgId + ", send exception=" + ex);

        }
    }
}
