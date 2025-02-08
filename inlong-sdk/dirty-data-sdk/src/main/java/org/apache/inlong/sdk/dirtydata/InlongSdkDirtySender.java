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

package org.apache.inlong.sdk.dirtydata;

import org.apache.inlong.sdk.dataproxy.MsgSenderSingleFactory;
import org.apache.inlong.sdk.dataproxy.common.ProcessResult;
import org.apache.inlong.sdk.dataproxy.sender.MsgSendCallback;
import org.apache.inlong.sdk.dataproxy.sender.tcp.TcpEventInfo;
import org.apache.inlong.sdk.dataproxy.sender.tcp.TcpMsgSender;
import org.apache.inlong.sdk.dataproxy.sender.tcp.TcpMsgSenderConfig;

import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
@Builder
public class InlongSdkDirtySender {

    private String inlongGroupId;
    private String inlongStreamId;
    private String inlongManagerAddr;
    private int inlongManagerPort;
    private String authId;
    private String authKey;
    private boolean ignoreErrors;
    private int maxRetryTimes;
    private int maxCallbackSize;
    @Builder.Default
    private boolean closed = false;

    private LinkedBlockingQueue<DirtyMessageWrapper> dirtyDataQueue;
    private TcpMsgSender sender;
    private MsgSenderSingleFactory messageSenderFactory;
    private Executor executor;

    public void init() throws Exception {
        Preconditions.checkNotNull(inlongGroupId, "inlongGroupId cannot be null");
        Preconditions.checkNotNull(inlongStreamId, "inlongStreamId cannot be null");
        Preconditions.checkNotNull(inlongManagerAddr, "inlongManagerAddr cannot be null");
        Preconditions.checkNotNull(authId, "authId cannot be null");
        Preconditions.checkNotNull(authKey, "authKey cannot be null");
        // build sender configure
        TcpMsgSenderConfig proxyClientConfig =
                new TcpMsgSenderConfig(false,
                        inlongManagerAddr, inlongManagerPort, inlongGroupId, authId, authKey);
        // build sender factory
        this.messageSenderFactory = new MsgSenderSingleFactory();
        this.sender = this.messageSenderFactory.genTcpSenderByClusterId(proxyClientConfig);
        this.dirtyDataQueue = new LinkedBlockingQueue<>(maxCallbackSize);
        this.executor = Executors.newSingleThreadExecutor();
        executor.execute(this::doSendDirtyMessage);
        log.info("init InlongSdkDirtySink successfully, target group={}, stream={}", inlongGroupId, inlongStreamId);
    }

    public void sendDirtyMessage(DirtyMessageWrapper messageWrapper) throws InterruptedException {
        dirtyDataQueue.put(messageWrapper);
    }

    public boolean sendDirtyMessageAsync(DirtyMessageWrapper messageWrapper) {
        return dirtyDataQueue.offer(messageWrapper);
    }

    private void doSendDirtyMessage() {
        ProcessResult procResult = new ProcessResult();
        while (!closed) {
            try {
                DirtyMessageWrapper messageWrapper = dirtyDataQueue.poll();
                if (messageWrapper == null) {
                    Thread.sleep(100L);
                    continue;
                }
                messageWrapper.increaseRetry();
                if (messageWrapper.getRetryTimes() > maxRetryTimes) {
                    log.error("failed to send dirty message after {} times, dirty data ={}", maxRetryTimes,
                            messageWrapper);
                    continue;
                }
                if (!sender.asyncSendMessage(new TcpEventInfo(inlongGroupId, inlongStreamId,
                        System.currentTimeMillis(), null, messageWrapper.format().getBytes()),
                        new LogCallBack(messageWrapper), procResult)) {
                    dirtyDataQueue.offer(messageWrapper);
                }
            } catch (Throwable t) {
                log.error("failed to send inlong dirty message", t);
                if (!ignoreErrors) {
                    throw new RuntimeException("writing dirty message to inlong sdk failed", t);
                }
            }
        }
    }

    public void close() {
        closed = true;
        dirtyDataQueue.clear();
        if (messageSenderFactory != null) {
            messageSenderFactory.shutdownAll();
        }
    }

    @Getter
    class LogCallBack implements MsgSendCallback {

        private final DirtyMessageWrapper wrapper;

        public LogCallBack(DirtyMessageWrapper wrapper) {
            this.wrapper = wrapper;
        }

        @Override
        public void onMessageAck(ProcessResult result) {
            if (!result.isSuccess()) {
                dirtyDataQueue.offer(wrapper);
            }
        }

        @Override
        public void onException(Throwable e) {
            log.error("failed to send inlong dirty message", e);
            if (!ignoreErrors) {
                throw new RuntimeException("writing dirty message to inlong sdk failed", e);
            }
        }
    }
}
