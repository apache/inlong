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
import org.apache.inlong.common.pojo.sort.dataflow.deserialization.RawDeserializationConfig;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.pojo.consume.BriefMQMessage;
import org.apache.inlong.manager.pojo.consume.BriefMQMessage.FieldInfo;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.pojo.stream.QueryMessageRequest;
import org.apache.inlong.manager.service.datatype.DataTypeOperator;
import org.apache.inlong.manager.service.datatype.DataTypeOperatorFactory;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class RawMsgDeserializeOperator implements DeserializeOperator {

    @Autowired
    public DataTypeOperatorFactory dataTypeOperatorFactory;

    @Override
    public boolean accept(MessageWrapType type) {
        return MessageWrapType.RAW.equals(type);
    }

    @Override
    public List<BriefMQMessage> decodeMsg(InlongStreamInfo streamInfo, List<BriefMQMessage> briefMQMessages,
            byte[] msgBytes, Map<String, String> headers, int index, QueryMessageRequest request) {
        String groupId = headers.get(AttributeConstants.GROUP_ID);
        String streamId = headers.get(AttributeConstants.STREAM_ID);
        long msgTime = Long.parseLong(headers.getOrDefault(MSG_TIME_KEY, "0"));
        String body = new String(msgBytes, Charset.forName(streamInfo.getDataEncoding()));
        try {
            DataTypeOperator dataTypeOperator =
                    dataTypeOperatorFactory.getInstance(DataTypeEnum.forType(streamInfo.getDataType()));
            List<FieldInfo> fieldList = dataTypeOperator.parseFields(body, streamInfo);
            if (checkIfFilter(request, fieldList)) {
                return briefMQMessages;
            }
            BriefMQMessage briefMQMessage = BriefMQMessage.builder()
                    .id(index)
                    .inlongGroupId(groupId)
                    .inlongStreamId(streamId)
                    .dt(msgTime)
                    .clientIp(headers.get(CLIENT_IP))
                    .headers(headers)
                    .body(body)
                    .fieldList(fieldList)
                    .build();
            briefMQMessages.addAll(Collections.singletonList(briefMQMessage));
            return briefMQMessages;
        } catch (Exception e) {
            String errMsg = String.format("decode msg failed for groupId=%s, streamId=%s", groupId, streamId);
            log.error(errMsg, e);
            throw new BusinessException(errMsg);
        }
    }

    @Override
    public DeserializationConfig getDeserializationConfig(InlongStreamInfo streamInfo) {
        RawDeserializationConfig rawDeserializationConfig = new RawDeserializationConfig();
        rawDeserializationConfig.setStreamId(streamInfo.getInlongStreamId());
        return rawDeserializationConfig;
    }

}
