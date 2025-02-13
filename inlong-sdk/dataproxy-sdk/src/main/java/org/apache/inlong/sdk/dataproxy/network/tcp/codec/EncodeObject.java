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

package org.apache.inlong.sdk.dataproxy.network.tcp.codec;

import org.apache.inlong.common.msg.AttributeConstants;
import org.apache.inlong.common.msg.MsgType;

import com.google.common.base.Joiner;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Encode Object class
 *
 * Used to encapsulate the reported event information to be sent
 */
public class EncodeObject {

    private static final Joiner.MapJoiner mapJoiner = Joiner.on(AttributeConstants.SEPARATOR)
            .withKeyValueSeparator(AttributeConstants.KEY_VALUE_SEPARATOR);

    private final MsgType msgType;
    private int intMsgType;
    private final String groupId;
    private final String streamId;
    private final long dtMs;
    private final long rtms;
    private int messageId;
    private int msgCnt = 0;
    private int eventSize;
    private int extField = 0;
    private int attrDataLength = 0;
    private byte[] attrData = null;
    private int bodyDataLength = 0;
    private byte[] bodyData = null;
    private int groupIdNum = 0;
    private int streamIdNum = 0;
    //
    private final Map<String, String> attrMap = new HashMap<>();
    private boolean compress;
    private byte[] aesKey;

    public EncodeObject(String groupId, String streamId, MsgType msgType, long dtMs, int eventSize) {
        this.groupId = groupId;
        this.streamId = streamId;
        this.msgType = msgType;
        this.eventSize = eventSize;
        this.intMsgType = this.msgType.getValue();
        this.rtms = System.currentTimeMillis();
        if (this.msgType == MsgType.MSG_BIN_MULTI_BODY) {
            this.dtMs = dtMs / 1000;
        } else {
            this.dtMs = dtMs;
        }
    }

    public void setGroupAndStreamId2Num(int groupIdNum, int streamIdNum) {
        this.groupIdNum = groupIdNum;
        this.streamIdNum = streamIdNum;
    }

    public void setExtField(int extField) {
        this.extField = extField;
    }

    public void setAttrInfo(int intMsgType, boolean isCompress, byte[] aesKey, Map<String, String> tgtAttrs) {
        this.intMsgType = intMsgType;
        this.compress = isCompress;
        this.aesKey = aesKey;
        if (tgtAttrs != null && !tgtAttrs.isEmpty()) {
            for (Map.Entry<String, String> entry : tgtAttrs.entrySet()) {
                if (entry == null || entry.getKey() == null) {
                    continue;
                }
                this.attrMap.put(entry.getKey(), entry.getValue());
            }
            String preAttrStr = mapJoiner.join(this.attrMap);
            this.attrData = preAttrStr.getBytes(StandardCharsets.UTF_8);
            this.attrDataLength = this.attrData.length;
        }
    }

    public void setMessageIdInfo(int messageId) {
        this.messageId = messageId;
        if (msgType == MsgType.MSG_ACK_SERVICE
                || msgType == MsgType.MSG_MULTI_BODY) {
            this.attrMap.put(AttributeConstants.MESSAGE_ID, String.valueOf(this.messageId));
            String preAttrStr = mapJoiner.join(this.attrMap);
            this.attrData = preAttrStr.getBytes(StandardCharsets.UTF_8);
            this.attrDataLength = this.attrData.length;
        }
    }

    public void setBodyData(int msgCnt, byte[] bodyBytes) {
        this.msgCnt = msgCnt;
        this.bodyData = bodyBytes;
        if (this.bodyData != null) {
            this.bodyDataLength = this.bodyData.length;
        }
    }

    public MsgType getMsgType() {
        return msgType;
    }

    public int getIntMsgType() {
        return intMsgType;
    }

    public int getMessageId() {
        return messageId;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getStreamId() {
        return streamId;
    }

    public long getRtms() {
        return rtms;
    }

    public int getStreamIdNum() {
        return streamIdNum;
    }

    public int getGroupIdNum() {
        return groupIdNum;
    }

    public boolean isCompress() {
        return compress;
    }

    public byte[] getAesKey() {
        return aesKey;
    }

    public int getBodyDataLength() {
        return bodyDataLength;
    }

    public byte[] getBodyData() {
        return bodyData;
    }

    public int getAttrDataLength() {
        return attrDataLength;
    }

    public byte[] getAttrData() {
        return attrData;
    }

    public int getExtField() {
        return extField;
    }

    public int getMsgSize() {
        return attrDataLength + bodyDataLength;
    }

    public int getMsgCnt() {
        return msgCnt;
    }

    public long getDtMs() {
        return dtMs;
    }

    public int getEventSize() {
        return eventSize;
    }
}
