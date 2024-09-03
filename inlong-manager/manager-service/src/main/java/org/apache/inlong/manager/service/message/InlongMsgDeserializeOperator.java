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
import org.apache.inlong.common.msg.InLongMsg;
import org.apache.inlong.common.pojo.sort.dataflow.deserialization.DeserializationConfig;
import org.apache.inlong.common.pojo.sort.dataflow.deserialization.InlongMsgDeserializationConfig;
import org.apache.inlong.common.util.StringUtil;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
@Service
public class InlongMsgDeserializeOperator implements DeserializeOperator {

    @Autowired
    public DataTypeOperatorFactory dataTypeOperatorFactory;

    @Override
    public boolean accept(MessageWrapType type) {
        return MessageWrapType.INLONG_MSG_V0.equals(type);
    }

    @Override
    public List<BriefMQMessage> decodeMsg(InlongStreamInfo streamInfo, List<BriefMQMessage> messageList,
            byte[] msgBytes, Map<String, String> headers, int index, QueryMessageRequest request) {
        String groupId = headers.get(AttributeConstants.GROUP_ID);
        String streamId = headers.get(AttributeConstants.STREAM_ID);
        InLongMsg inLongMsg = InLongMsg.parseFrom(msgBytes);
        for (String attr : inLongMsg.getAttrs()) {
            Map<String, String> attrMap = StringUtil.splitKv(attr, INLONGMSG_ATTR_ENTRY_DELIMITER,
                    INLONGMSG_ATTR_KV_DELIMITER, null, null);
            // Extracts time from the attributes
            long msgTime;
            if (attrMap.containsKey(INLONGMSG_ATTR_TIME_T)) {
                String date = attrMap.get(INLONGMSG_ATTR_TIME_T).trim();
                msgTime = StringUtil.parseDateTime(date);
            } else if (attrMap.containsKey(INLONGMSG_ATTR_TIME_DT)) {
                String epoch = attrMap.get(INLONGMSG_ATTR_TIME_DT).trim();
                msgTime = Long.parseLong(epoch);
            } else {
                throw new IllegalArgumentException(String.format("PARSE_ATTR_ERROR_STRING%s",
                        INLONGMSG_ATTR_TIME_T + " or " + INLONGMSG_ATTR_TIME_DT));
            }
            Iterator<byte[]> iterator = inLongMsg.getIterator(attr);
            while (iterator.hasNext()) {
                byte[] bodyBytes = iterator.next();
                if (Objects.isNull(bodyBytes)) {
                    continue;
                }
                try {
                    String body = new String(bodyBytes, Charset.forName(streamInfo.getDataEncoding()));
                    DataTypeOperator dataTypeOperator =
                            dataTypeOperatorFactory.getInstance(DataTypeEnum.forType(streamInfo.getDataType()));
                    List<FieldInfo> streamFieldList = dataTypeOperator.parseFields(body, streamInfo);
                    if (checkIfFilter(request, streamFieldList)) {
                        continue;
                    }
                    BriefMQMessage message = BriefMQMessage.builder()
                            .id(index)
                            .inlongGroupId(groupId)
                            .inlongStreamId(streamId)
                            .dt(msgTime)
                            .clientIp(attrMap.get(CLIENT_IP))
                            .headers(headers)
                            .attribute(attr)
                            .body(body)
                            .fieldList(streamFieldList)
                            .build();
                    messageList.add(message);
                } catch (Exception e) {
                    String errMsg = String.format("decode msg failed for groupId=%s, streamId=%s", groupId, streamId);
                    log.error(errMsg, e);
                    throw new BusinessException(errMsg);
                }
            }
        }
        return messageList;
    }

    @Override
    public DeserializationConfig getDeserializationConfig(InlongStreamInfo streamInfo) {
        InlongMsgDeserializationConfig inlongMsgDeserializationConfig = new InlongMsgDeserializationConfig();
        inlongMsgDeserializationConfig.setStreamId(streamInfo.getInlongStreamId());
        return inlongMsgDeserializationConfig;
    }
}
