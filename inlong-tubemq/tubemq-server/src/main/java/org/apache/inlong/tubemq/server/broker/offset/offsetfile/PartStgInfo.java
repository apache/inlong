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

package org.apache.inlong.tubemq.server.broker.offset.offsetfile;

import org.apache.inlong.tubemq.corebase.TBaseConstants;

public class PartStgInfo {

    private final String topic;
    private final int partId;
    private long lstRstTerm = 0;
    private long lstOffset = TBaseConstants.META_VALUE_UNDEFINED;
    private long msgId = 0;
    private long lstUpdTime = 0;
    private long createOffset = TBaseConstants.META_VALUE_UNDEFINED;
    private long createTime = 0;

    public PartStgInfo(String topicName, int partitionId) {
        this.topic = topicName;
        this.partId = partitionId;
    }

    public void updateOffset(long lstRstTerm, long msgId, boolean isFstCreate,
            long fstOffset, long fstUpdTime, long lstOffset, long lstUpdTime) {
        this.lstRstTerm = lstRstTerm;
        this.msgId = msgId;
        if (isFstCreate) {
            this.createOffset = fstOffset;
            this.createTime = fstUpdTime;
        }
        this.lstOffset = lstOffset;
        this.lstUpdTime = lstUpdTime;
    }

    public void resetOffset(long rstTerm, boolean isFstCreate, long offset, long resetTime) {
        this.lstRstTerm = rstTerm;
        if (isFstCreate) {
            this.createOffset = offset;
            this.createTime = resetTime;
        }
        this.lstOffset = offset;
        this.lstUpdTime = resetTime;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartId() {
        return partId;
    }

    public long getLstOffset() {
        return lstOffset;
    }

    public long getLstRstTerm() {
        return lstRstTerm;
    }

    public long getMsgId() {
        return msgId;
    }

    public long getLstUpdTime() {
        return lstUpdTime;
    }

    public long getCreateOffset() {
        return createOffset;
    }

    public long getCreateTime() {
        return createTime;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("PartStgInfo{");
        sb.append("topic='").append(topic).append('\'');
        sb.append(", partId=").append(partId);
        sb.append(", lstRstTerm=").append(lstRstTerm);
        sb.append(", lstOffset=").append(lstOffset);
        sb.append(", msgId=").append(msgId);
        sb.append(", lstUpdTime=").append(lstUpdTime);
        sb.append(", createOffset=").append(createOffset);
        sb.append(", createTime=").append(createTime);
        sb.append('}');
        return sb.toString();
    }
}
