package org.apache.tubemq.server.common.utils;

import org.apache.tubemq.corebase.TBaseConstants;

// append result return
public class AppendResult {
    private boolean isSuccess = false;
    private long appendTime = TBaseConstants.META_VALUE_UNDEFINED;
    private long msgId;
    private long appendIndexOffset = TBaseConstants.META_VALUE_UNDEFINED;
    private long appendDataOffset = TBaseConstants.META_VALUE_UNDEFINED;

    public AppendResult() {

    }

    public void putReceivedInfo(long msgId, long appendTime) {
        this.msgId = msgId;
        this.appendTime = appendTime;
    }

    public void putAppendResult(long appendIndexOffset, long appendDataOffset) {
        this.isSuccess = true;
        this.appendIndexOffset = appendIndexOffset;
        this.appendDataOffset = appendDataOffset;
    }

    public boolean isSuccess() {
        return isSuccess;
    }

    public long getMsgId() {
        return msgId;
    }

    public long getAppendTime() {
        return appendTime;
    }

    public long getAppendIndexOffset() {
        return appendIndexOffset;
    }

    public long getAppendDataOffset() {
        return appendDataOffset;
    }
}
