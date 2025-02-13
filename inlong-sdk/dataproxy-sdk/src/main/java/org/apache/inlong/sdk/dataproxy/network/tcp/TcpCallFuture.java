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

package org.apache.inlong.sdk.dataproxy.network.tcp;

import org.apache.inlong.sdk.dataproxy.common.ErrorCode;
import org.apache.inlong.sdk.dataproxy.common.ProcessResult;
import org.apache.inlong.sdk.dataproxy.network.tcp.codec.EncodeObject;
import org.apache.inlong.sdk.dataproxy.sender.MsgSendCallback;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * TCP Call Future class
 *
 * a future implementation for tcp RPCs.
 */
public class TcpCallFuture implements MsgSendCallback {

    private final int messageId;
    private final String groupId;
    private final String streamId;
    private final int msgCnt;
    private final int eventSize;
    private final long rtTime;
    private final String clientAddr;
    private final long chanTerm;
    private final String chanStr;
    private final MsgSendCallback callback;
    private final CountDownLatch latch = new CountDownLatch(1);
    private final boolean isAsyncCall;
    private ProcessResult result = null;
    private Throwable error = null;

    public TcpCallFuture(EncodeObject encObject,
            String clientAddr, long chanTerm, String chanStr, MsgSendCallback callback) {
        this.messageId = encObject.getMessageId();
        this.groupId = encObject.getGroupId();
        this.streamId = encObject.getStreamId();
        this.rtTime = encObject.getRtms();
        this.msgCnt = encObject.getMsgCnt();
        this.eventSize = encObject.getEventSize();
        this.clientAddr = clientAddr;
        this.chanTerm = chanTerm;
        this.chanStr = chanStr;
        this.callback = callback;
        this.isAsyncCall = (callback != null);
    }

    @Override
    public void onMessageAck(ProcessResult result) {
        this.result = result;
        latch.countDown();
        if (isAsyncCall) {
            callback.onMessageAck(result);
        }
    }

    @Override
    public void onException(Throwable ex) {
        this.error = ex;
        latch.countDown();
        if (isAsyncCall) {
            callback.onException(error);
        }
    }

    public boolean get(ProcessResult processResult, long timeout, TimeUnit unit) {
        try {
            if (latch.await(timeout, unit)) {
                if (error != null) {
                    return processResult.setFailResult(ErrorCode.SEND_ON_EXCEPTION, error.getMessage());
                }
                return processResult.setFailResult(result);
            } else {
                return processResult.setFailResult(ErrorCode.SEND_WAIT_TIMEOUT);
            }
        } catch (Throwable ex) {
            if (ex instanceof InterruptedException) {
                return processResult.setFailResult(ErrorCode.SEND_WAIT_INTERRUPT);
            } else {
                return processResult.setFailResult(ErrorCode.UNKNOWN_ERROR, ex.getMessage());
            }
        }
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

    public int getMsgCnt() {
        return msgCnt;
    }

    public long getRtTime() {
        return rtTime;
    }

    public String getClientAddr() {
        return clientAddr;
    }

    public String getChanStr() {
        return chanStr;
    }

    public long getChanTerm() {
        return chanTerm;
    }

    public boolean isAsyncCall() {
        return isAsyncCall;
    }

    public int getEventSize() {
        return eventSize;
    }
}
