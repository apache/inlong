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

package org.apache.inlong.sdk.dirtydata.sink;

import org.apache.inlong.sdk.dataproxy.DefaultMessageSender;
import org.apache.inlong.sdk.dataproxy.MessageSender;
import org.apache.inlong.sdk.dataproxy.ProxyClientConfig;
import org.apache.inlong.sdk.dataproxy.common.SendMessageCallback;
import org.apache.inlong.sdk.dataproxy.common.SendResult;
import org.apache.inlong.sdk.dirtydata.DirtyData;
import org.apache.inlong.sdk.dirtydata.DirtySink;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.StringJoiner;

import static org.apache.inlong.sdk.dirtydata.Constants.DIRTY_SIDE_OUTPUT_IGNORE_ERRORS;

@Slf4j
public class InlongSdkDirtySink implements DirtySink {

    // The inlong manager addr to init inlong sdk
    private static final String DIRTY_SIDE_OUTPUT_INLONG_MANAGER = "dirty.side-output.inlong-sdk.inlong-manager-addr";
    // The inlong manager auth id to init inlong sdk
    private static final String DIRTY_SIDE_OUTPUT_INLONG_AUTH_ID = "dirty.side-output.inlong-sdk.inlong-auth-id";
    // The inlong manager auth id to init inlong sdk
    private static final String DIRTY_SIDE_OUTPUT_INLONG_AUTH_KEY = "dirty.side-output.inlong-sdk.inlong-auth-key";
    // The inlong group id of dirty sink
    private static final String DIRTY_SIDE_OUTPUT_INLONG_GROUP = "dirty.side-output.inlong-sdk.inlong-group-id";
    // The inlong stream id of dirty sink
    private static final String DIRTY_SIDE_OUTPUT_INLONG_STREAM = "dirty.side-output.inlong-sdk.inlong-stream-id";

    private InlongSdkOptions options;
    private String inlongGroupId;
    private String inlongStreamId;
    private final SendMessageCallback callback;

    private transient DateTimeFormatter dateTimeFormatter;
    private transient MessageSender sender;

    public InlongSdkDirtySink() {
        this.callback = new LogCallBack();
    }

    @Override
    public void open(Configure configuration) throws Exception {
        options = getOptions(configuration);
        this.inlongGroupId = options.getInlongGroupId();
        this.inlongStreamId = options.getInlongStreamId();
        dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        // init sender
        ProxyClientConfig proxyClientConfig =
                new ProxyClientConfig(options.getInlongManagerAddr(), options.getInlongGroupId(),
                        options.getInlongManagerAuthId(), options.getInlongManagerAuthKey());
        sender = DefaultMessageSender.generateSenderByClusterId(proxyClientConfig);
    }

    @Override
    public void invoke(DirtyData dirtyData) {
        try {
            Map<String, String> labelMap = LabelUtils.parseLabels(dirtyData.getLabels());
            String groupId = Preconditions.checkNotNull(labelMap.get("groupId"));
            String streamId = Preconditions.checkNotNull(labelMap.get("streamId"));

            String message = join(groupId, streamId,
                    dirtyData.getDirtyType(), dirtyData.getLabels(),
                    new String(dirtyData.getData(), StandardCharsets.UTF_8));
            sender.asyncSendMessage(inlongGroupId, inlongStreamId, message.getBytes(), callback);
        } catch (Throwable t) {
            log.error("failed to send dirty message to inlong sdk", t);
        }
    }

    private InlongSdkOptions getOptions(Configure config) {
        return InlongSdkOptions.builder()
                .inlongManagerAddr(config.get(DIRTY_SIDE_OUTPUT_INLONG_MANAGER))
                .inlongGroupId(config.get(DIRTY_SIDE_OUTPUT_INLONG_GROUP))
                .inlongStreamId(config.get(DIRTY_SIDE_OUTPUT_INLONG_STREAM))
                .inlongManagerAuthKey(config.get(DIRTY_SIDE_OUTPUT_INLONG_AUTH_KEY))
                .inlongManagerAuthId(config.get(DIRTY_SIDE_OUTPUT_INLONG_AUTH_ID))
                .ignoreSideOutputErrors(config.getBoolean(DIRTY_SIDE_OUTPUT_IGNORE_ERRORS, true))
                .enableDirtyLog(true)
                .build();
    }

    @Override
    public void close() throws Exception {
        if (sender != null) {
            sender.close();
        }
    }

    private String join(
            String inlongGroup,
            String inlongStream,
            String type,
            String label,
            String formattedData) {

        String now = LocalDateTime.now().format(dateTimeFormatter);

        StringJoiner joiner = new StringJoiner(options.getCsvFieldDelimiter());
        return joiner.add(inlongGroup + "." + inlongStream)
                .add(now)
                .add(type)
                .add(label)
                .add(formattedData).toString();
    }

    class LogCallBack implements SendMessageCallback {

        @Override
        public void onMessageAck(SendResult result) {
            if (result == SendResult.OK) {
                return;
            }
            log.error("failed to send inlong dirty message, response={}", result);

            if (!options.isIgnoreSideOutputErrors()) {
                throw new RuntimeException("writing dirty message to inlong sdk failed, response=" + result);
            }
        }

        @Override
        public void onException(Throwable e) {
            log.error("failed to send inlong dirty message", e);

            if (!options.isIgnoreSideOutputErrors()) {
                throw new RuntimeException("writing dirty message to inlong sdk failed", e);
            }
        }
    }
}
