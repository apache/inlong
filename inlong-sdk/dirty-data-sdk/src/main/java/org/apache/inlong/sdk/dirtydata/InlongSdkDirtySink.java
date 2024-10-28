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

import org.apache.inlong.sdk.dataproxy.DefaultMessageSender;
import org.apache.inlong.sdk.dataproxy.MessageSender;
import org.apache.inlong.sdk.dataproxy.ProxyClientConfig;
import org.apache.inlong.sdk.dataproxy.common.SendMessageCallback;
import org.apache.inlong.sdk.dataproxy.common.SendResult;
import org.apache.inlong.sdk.dataproxy.network.ProxysdkException;

import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Builder
public class InlongSdkDirtySink {

    private String inlongGroupId;
    private String inlongStreamId;
    private String inlongManagerAddr;
    private String authId;
    private String authKey;
    private boolean ignoreErrors;

    private SendMessageCallback callback;
    private MessageSender sender;

    public void init() throws Exception {
        Preconditions.checkNotNull(inlongGroupId, "inlongGroupId cannot be null");
        Preconditions.checkNotNull(inlongStreamId, "inlongStreamId cannot be null");
        Preconditions.checkNotNull(inlongManagerAddr, "inlongManagerAddr cannot be null");
        Preconditions.checkNotNull(authId, "authId cannot be null");
        Preconditions.checkNotNull(authKey, "authKey cannot be null");

        this.callback = new LogCallBack();
        ProxyClientConfig proxyClientConfig =
                new ProxyClientConfig(inlongManagerAddr, inlongGroupId, authId, authKey);
        this.sender = DefaultMessageSender.generateSenderByClusterId(proxyClientConfig);
        log.info("init InlongSdkDirtySink successfully, target group={}, stream={}", inlongGroupId, inlongStreamId);
    }

    public void sendDirtyMessage(DirtyMessageWrapper messageWrapper)
            throws ProxysdkException {
        sender.asyncSendMessage(inlongGroupId, inlongStreamId, messageWrapper.format().getBytes(), callback);
    }

    class LogCallBack implements SendMessageCallback {

        @Override
        public void onMessageAck(SendResult result) {
            if (result == SendResult.OK) {
                return;
            }
            log.error("failed to send inlong dirty message, response={}", result);

            if (!ignoreErrors) {
                throw new RuntimeException("writing dirty message to inlong sdk failed, response=" + result);
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
