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

import org.apache.inlong.common.enums.DataTypeEnum;
import org.apache.inlong.common.enums.MessageWrapType;
import org.apache.inlong.common.msg.AttributeConstants;
import org.apache.inlong.common.pojo.sort.dataflow.deserialization.DeserializationConfig;
import org.apache.inlong.common.pojo.sort.dataflow.deserialization.InlongMsgPbDeserialiationConfig;
import org.apache.inlong.common.util.Utils;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.pojo.consume.BriefMQMessage;
import org.apache.inlong.manager.pojo.consume.BriefMQMessage.FieldInfo;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.pojo.stream.QueryMessageRequest;
import org.apache.inlong.manager.service.datatype.DataTypeOperator;
import org.apache.inlong.manager.service.datatype.DataTypeOperatorFactory;
import org.apache.inlong.sdk.commons.protocol.ProxySdk.INLONG_COMPRESSED_TYPE;
import org.apache.inlong.sdk.commons.protocol.ProxySdk.MapFieldEntry;
import org.apache.inlong.sdk.commons.protocol.ProxySdk.MessageObj;
import org.apache.inlong.sdk.commons.protocol.ProxySdk.MessageObjs;
import org.apache.inlong.sort.configuration.Constants;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class PbMsgDeserializeOperator implements DeserializeOperator {

    @Autowired
    public DataTypeOperatorFactory dataTypeOperatorFactory;

    @Override
    public boolean accept(MessageWrapType type) {
        return MessageWrapType.INLONG_MSG_V1.equals(type);
    }

    @Override
    public List<BriefMQMessage> decodeMsg(InlongStreamInfo streamInfo, List<BriefMQMessage> briefMQMessages,
            byte[] msgBytes, Map<String, String> headers, int index, QueryMessageRequest request) throws Exception {
        int compressType = Integer.parseInt(headers.getOrDefault(COMPRESS_TYPE_KEY, "0"));
        byte[] values = msgBytes;
        switch (compressType) {
            case INLONG_COMPRESSED_TYPE.INLONG_NO_COMPRESS_VALUE:
                break;
            case INLONG_COMPRESSED_TYPE.INLONG_GZ_VALUE:
                values = Utils.gzipDecompress(msgBytes, 0, msgBytes.length);
                break;
            case INLONG_COMPRESSED_TYPE.INLONG_SNAPPY_VALUE:
                values = Utils.snappyDecompress(msgBytes, 0, msgBytes.length);
                break;
            default:
                throw new IllegalArgumentException("Unknown compress type:" + compressType);
        }
        briefMQMessages.addAll(transformMessageObjs(MessageObjs.parseFrom(values), streamInfo, index, request));
        return briefMQMessages;
    }

    private List<BriefMQMessage> transformMessageObjs(MessageObjs messageObjs, InlongStreamInfo streamInfo, int index,
            QueryMessageRequest request) {
        if (null == messageObjs) {
            return null;
        }

        List<BriefMQMessage> messageList = new ArrayList<>();
        for (MessageObj messageObj : messageObjs.getMsgsList()) {
            List<MapFieldEntry> mapFieldEntries = messageObj.getParamsList();
            Map<String, String> headers = new HashMap<>();
            for (MapFieldEntry mapFieldEntry : mapFieldEntries) {
                headers.put(mapFieldEntry.getKey(), mapFieldEntry.getValue());
            }

            try {
                String body = new String(messageObj.getBody().toByteArray(),
                        Charset.forName(streamInfo.getDataEncoding()));
                DataTypeOperator dataTypeOperator =
                        dataTypeOperatorFactory.getInstance(DataTypeEnum.forType(streamInfo.getDataType()));
                List<FieldInfo> streamFieldList = dataTypeOperator.parseFields(body, streamInfo);
                if (checkIfFilter(request, streamFieldList)) {
                    continue;
                }
                BriefMQMessage message = BriefMQMessage.builder()
                        .id(index)
                        .inlongGroupId(headers.get(AttributeConstants.GROUP_ID))
                        .inlongStreamId(headers.get(AttributeConstants.STREAM_ID))
                        .dt(messageObj.getMsgTime())
                        .clientIp(headers.get(CLIENT_IP))
                        .body(body)
                        .headers(headers)
                        .fieldList(streamFieldList)
                        .build();
                messageList.add(message);
            } catch (Exception e) {
                String errMsg = String.format("decode msg failed for groupId=%s, streamId=%s",
                        streamInfo.getInlongGroupId(), streamInfo.getInlongStreamId());
                log.error(errMsg, e);
                throw new BusinessException(errMsg);
            }
        }

        return messageList;
    }

    @Override
    public DeserializationConfig getDeserializationConfig(InlongStreamInfo streamInfo) {
        InlongMsgPbDeserialiationConfig inlongMsgPbDeserialiationConfig = new InlongMsgPbDeserialiationConfig();
        inlongMsgPbDeserialiationConfig.setCompressionType(Constants.CompressionType.GZIP.name());
        return inlongMsgPbDeserialiationConfig;
    }
}
