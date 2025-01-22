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

package org.apache.inlong.sdk.dataproxy.sender.http;

import org.apache.inlong.sdk.dataproxy.MsgSenderFactory;
import org.apache.inlong.sdk.dataproxy.common.ErrorCode;
import org.apache.inlong.sdk.dataproxy.common.ProcessResult;
import org.apache.inlong.sdk.dataproxy.common.SdkConsts;
import org.apache.inlong.sdk.dataproxy.network.http.HttpAsyncObj;
import org.apache.inlong.sdk.dataproxy.network.http.HttpClientMgr;
import org.apache.inlong.sdk.dataproxy.sender.BaseSender;
import org.apache.inlong.sdk.dataproxy.sender.MsgSendCallback;
import org.apache.inlong.sdk.dataproxy.utils.LogCounter;

/**
 * HTTP(s) Message Sender class
 *
 * Used to define the HTTP(s) sender common methods
 */
public class InLongHttpMsgSender extends BaseSender implements HttpMsgSender {

    protected static final LogCounter httpExceptCnt = new LogCounter(10, 100000, 60 * 1000L);
    private final HttpClientMgr httpClientMgr;
    private final HttpMsgSenderConfig httpConfig;

    public InLongHttpMsgSender(HttpMsgSenderConfig configure) {
        this(configure, null, null);
    }

    public InLongHttpMsgSender(HttpMsgSenderConfig configure, MsgSenderFactory senderFactory, String clusterIdKey) {
        super(configure, senderFactory, clusterIdKey);
        this.httpConfig = (HttpMsgSenderConfig) baseConfig;
        this.clientMgr = new HttpClientMgr(this, this.httpConfig);
        this.httpClientMgr = (HttpClientMgr) clientMgr;
    }

    @Override
    public boolean syncSendMessage(HttpEventInfo eventInfo, ProcessResult procResult) {
        validParamsNotNull(eventInfo, procResult);
        if (!this.isStarted()) {
            return procResult.setFailResult(ErrorCode.SDK_CLOSED);
        }
        if (this.isMetaInfoUnReady()) {
            return procResult.setFailResult(ErrorCode.NO_NODE_META_INFOS);
        }
        // check package length
        if (!isValidPkgLength(eventInfo, this.getAllowedPkgLength(), procResult)) {
            return false;
        }
        return httpClientMgr.sendMessage(eventInfo, procResult);
    }

    @Override
    public boolean asyncSendMessage(HttpEventInfo eventInfo, MsgSendCallback callback, ProcessResult procResult) {
        validParamsNotNull(eventInfo, callback, procResult);
        if (!this.isStarted()) {
            return procResult.setFailResult(ErrorCode.SDK_CLOSED);
        }
        if (this.isMetaInfoUnReady()) {
            return procResult.setFailResult(ErrorCode.NO_NODE_META_INFOS);
        }
        // check package length
        if (!isValidPkgLength(eventInfo, this.getAllowedPkgLength(), procResult)) {
            return false;
        }
        return httpClientMgr.asyncSendMessage(new HttpAsyncObj(eventInfo, callback), procResult);
    }

    @Override
    public int getActiveNodeCnt() {
        return httpClientMgr.getActiveNodeCnt();
    }

    @Override
    public int getInflightMsgCnt() {
        return httpClientMgr.getInflightMsgCnt();
    }

    private boolean isValidPkgLength(HttpEventInfo eventInfo, int allowedLen, ProcessResult procResult) {
        // Not valid if the maximum limit is less than or equal to 0
        if (allowedLen < 0) {
            return true;
        }
        int eventLen = eventInfo.getBodySize()
                + eventInfo.getGroupId().length()
                + eventInfo.getStreamId().length()
                + String.valueOf(eventInfo.getDtMs()).length();
        // Reserve space for attribute
        if (eventLen > allowedLen - SdkConsts.RESERVED_ATTRIBUTE_LENGTH) {
            String errMsg = String.format("OverMaxLen: content length(%d) > allowedLen(%d) - fixedLen(%d)",
                    eventLen, allowedLen, SdkConsts.RESERVED_ATTRIBUTE_LENGTH);
            if (httpExceptCnt.shouldPrint()) {
                logger.warn(errMsg);
            }
            return procResult.setFailResult(ErrorCode.REPORT_INFO_EXCEED_MAX_LEN, errMsg);
        }
        return true;
    }

    private void validParamsNotNull(HttpEventInfo eventInfo, ProcessResult procResult) {
        if (eventInfo == null) {
            throw new NullPointerException("eventInfo is null");
        }
        if (procResult == null) {
            throw new NullPointerException("procResult is null");
        }
    }

    private void validParamsNotNull(HttpEventInfo eventInfo, MsgSendCallback callback, ProcessResult procResult) {
        if (eventInfo == null) {
            throw new NullPointerException("eventInfo is null");
        }
        if (callback == null) {
            throw new NullPointerException("callback is null");
        }
        if (procResult == null) {
            throw new NullPointerException("procResult is null");
        }
    }

}
