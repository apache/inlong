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

package org.apache.inlong.manager.service.message;

import org.apache.inlong.common.enums.MessageWrapType;
import org.apache.inlong.common.msg.AttributeConstants;
import org.apache.inlong.manager.pojo.consume.DisplayMessage;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.sdk.commons.protocol.ProxySdk.MapFieldEntry;
import org.apache.inlong.sdk.commons.protocol.ProxySdk.MessageObj;
import org.apache.inlong.sdk.commons.protocol.ProxySdk.MessageObjs;
import org.apache.inlong.sdk.sort.util.Utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class PbMsgDeserializeOperator implements DeserializeOperator {

    public static final String NODE_IP = "NodeIP";
    private static final String COMPRESS_TYPE_KEY = "compressType";
    private static final int COMPRESS_TYPE_NONE = 0;
    private static final int COMPRESS_TYPE_GZIP = 1;
    private static final int COMPRESS_TYPE_SNAPPY = 2;

    @Override
    public boolean accept(MessageWrapType type) {
        return MessageWrapType.PB.equals(type);
    }

    @Override
    public List<DisplayMessage> decodeMsg(InlongStreamInfo streamInfo,
            byte[] msgBytes, Map<String, String> headers, int index) throws Exception {
        List<DisplayMessage> messageList = new ArrayList<>();
        int compressType = Integer.parseInt(headers.getOrDefault(COMPRESS_TYPE_KEY, "0"));
        byte[] values = msgBytes;
        switch (compressType) {
            case COMPRESS_TYPE_NONE:
                break;
            case COMPRESS_TYPE_SNAPPY:
                values = Utils.snappyDecompress(msgBytes, 0, msgBytes.length);
                break;
            case COMPRESS_TYPE_GZIP:
                values = Utils.gzipDecompress(msgBytes, 0, msgBytes.length);
                break;
            default:
                throw new IllegalArgumentException("Unknown compress type:" + compressType);
        }
        messageList = transformMessageObjs(MessageObjs.parseFrom(values), streamInfo, index);
        return messageList;
    }

    private List<DisplayMessage> transformMessageObjs(MessageObjs messageObjs, InlongStreamInfo streamInfo, int index) {
        if (null == messageObjs) {
            return null;
        }
        List<DisplayMessage> displayMessages = new ArrayList<>();
        for (MessageObj messageObj : messageObjs.getMsgsList()) {
            List<MapFieldEntry> mapFieldEntries = messageObj.getParamsList();
            Map<String, String> headers = new HashMap<>();
            for (MapFieldEntry mapFieldEntry : mapFieldEntries) {
                headers.put(mapFieldEntry.getKey(), mapFieldEntry.getValue());
            }
            DisplayMessage displayMessage = new DisplayMessage(index, headers.get(AttributeConstants.GROUP_ID),
                    headers.get(AttributeConstants.STREAM_ID), messageObj.getMsgTime(),
                    headers.get(NODE_IP),
                    new String(messageObj.getBody().toByteArray(), Charset.forName(streamInfo.getDataEncoding())));
            displayMessages.add(displayMessage);
        }
        return displayMessages;
    }
}
