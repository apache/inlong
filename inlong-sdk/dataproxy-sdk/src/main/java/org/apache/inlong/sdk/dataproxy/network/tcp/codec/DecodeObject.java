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

import org.apache.inlong.common.enums.DataProxyErrCode;
import org.apache.inlong.common.msg.AttributeConstants;
import org.apache.inlong.common.msg.MsgType;
import org.apache.inlong.sdk.dataproxy.common.ErrorCode;
import org.apache.inlong.sdk.dataproxy.common.ProcessResult;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Decode Object class
 *
 * Used to carry the decoded information of the response
 */
public class DecodeObject {

    private final MsgType msgType;
    private int messageId;
    private String dpIp;
    private ProcessResult procResult;
    private String addErrMsg;
    private Map<String, String> retAttr;

    public DecodeObject(MsgType msgType, String attributes) {
        this.msgType = msgType;
        handleAttr(attributes);
    }

    public DecodeObject(MsgType msgType, int messageId, String attributes) {
        this.msgType = msgType;
        this.messageId = messageId;
        handleAttr(attributes);
    }

    public MsgType getMsgType() {
        return msgType;
    }

    public int getMessageId() {
        return messageId;
    }

    public String getDpIp() {
        return dpIp;
    }

    public ProcessResult getSendResult() {
        return procResult;
    }

    public String getAddErrMsg() {
        return addErrMsg;
    }

    public Map<String, String> getRetAttr() {
        return retAttr;
    }

    private void handleAttr(String attributes) {
        if (StringUtils.isBlank(attributes)) {
            this.procResult = new ProcessResult(ErrorCode.OK);
            return;
        }
        // decode attribute string
        this.retAttr = new HashMap<>();
        String[] keyValSet = attributes.split(AttributeConstants.SEPARATOR);
        for (String keyVal : keyValSet) {
            if (StringUtils.isBlank(keyVal)) {
                continue;
            }
            String[] keyValSplit = keyVal.split(AttributeConstants.KEY_VALUE_SEPARATOR);
            if (keyValSplit.length == 1) {
                if (StringUtils.isBlank(keyValSplit[0])) {
                    continue;
                }
                retAttr.put(keyValSplit[0].trim(), "");
            } else {
                if (StringUtils.isBlank(keyValSplit[0]) || keyValSplit[1] == null) {
                    continue;
                }
                retAttr.put(keyValSplit[0].trim(), keyValSplit[1].trim());
            }
        }
        if (retAttr.containsKey(AttributeConstants.MESSAGE_ID)) {
            this.messageId = Integer.parseInt(retAttr.get(AttributeConstants.MESSAGE_ID));
        }
        dpIp = retAttr.get(AttributeConstants.MESSAGE_DP_IP);
        String errCode = retAttr.get(AttributeConstants.MESSAGE_PROCESS_ERRCODE);
        // errCode is empty or equals 0 -> success
        if (StringUtils.isBlank(errCode) || Integer.parseInt(errCode) == 0) {
            this.procResult = new ProcessResult(ErrorCode.OK);
        } else {
            // get errMsg
            this.addErrMsg = retAttr.get(AttributeConstants.MESSAGE_PROCESS_ERRMSG);
            if (StringUtils.isBlank(addErrMsg)) {
                this.addErrMsg = DataProxyErrCode.valueOf(Integer.parseInt(errCode)).getErrMsg();
            }
            // sendResult
            this.procResult = convertToSendResult(Integer.parseInt(errCode));
        }
    }

    private ProcessResult convertToSendResult(int errCode) {
        DataProxyErrCode dpErrCode = DataProxyErrCode.valueOf(errCode);
        switch (dpErrCode) {
            case SINK_SERVICE_UNREADY:
                return new ProcessResult(ErrorCode.DP_SINK_SERVICE_UNREADY);

            case MISS_REQUIRED_GROUPID_ARGUMENT:
            case MISS_REQUIRED_STREAMID_ARGUMENT:
            case MISS_REQUIRED_DT_ARGUMENT:
            case UNSUPPORTED_EXTEND_FIELD_VALUE:
                return new ProcessResult(ErrorCode.DP_INVALID_ATTRS, String.valueOf(dpErrCode));

            case MISS_REQUIRED_BODY_ARGUMENT:
            case EMPTY_MSG:
                return new ProcessResult(ErrorCode.DP_EMPTY_BODY, String.valueOf(dpErrCode));

            case BODY_EXCEED_MAX_LEN:
                return new ProcessResult(ErrorCode.DP_BODY_EXCEED_MAX_LEN);

            case UNCONFIGURED_GROUPID_OR_STREAMID:
                return new ProcessResult(ErrorCode.DP_UNCONFIGURED_GROUPID_OR_STREAMID);

            case PUT_EVENT_TO_CHANNEL_FAILURE:
            case NO_AVAILABLE_PRODUCER:
            case PRODUCER_IS_NULL:
            case SEND_REQUEST_TO_MQ_FAILURE:
            case MQ_RETURN_ERROR:
            case DUPLICATED_MESSAGE:
                return new ProcessResult(ErrorCode.DP_RECEIVE_FAILURE, String.valueOf(dpErrCode));

            default:
                return new ProcessResult(ErrorCode.UNKNOWN_ERROR, String.valueOf(dpErrCode));
        }
    }
}
