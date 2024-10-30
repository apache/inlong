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

package org.apache.inlong.tubemq.server.broker.offset;

import org.apache.inlong.tubemq.corebase.TBaseConstants;
import org.apache.inlong.tubemq.corebase.TokenConstants;
import org.apache.inlong.tubemq.server.broker.utils.DataStoreUtils;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

public class OffsetStorageInfo implements Serializable {

    private static final long serialVersionUID = -4232003748320500757L;
    private final String topic;
    private final int brokerId;
    private final int partitionId;
    private final AtomicLong offset = new AtomicLong(0);
    private long messageId;
    private long lstRstTerm;
    private boolean firstCreate = false;
    private long firstOffset = TBaseConstants.META_VALUE_UNDEFINED;
    private long createTime = TBaseConstants.META_VALUE_UNDEFINED;
    private boolean modified = false;
    private final AtomicLong lstUpdateTime = new AtomicLong(0);

    /**
     * Initial offset storage information
     *
     * @param topic          the topic name
     * @param brokerId       the broker id
     * @param partitionId    the partition id
     * @param lstRstTerm     the last reset term
     * @param offset         the offset
     * @param messageId      the message id
     */
    public OffsetStorageInfo(String topic, int brokerId, int partitionId,
            long lstRstTerm, long offset, long messageId) {
        this(topic, brokerId, partitionId, lstRstTerm, offset, messageId, true, System.currentTimeMillis());
    }

    /**
     * Initial offset storage information
     *
     * @param topic          the topic name
     * @param brokerId       the broker id
     * @param partitionId    the partition id
     * @param lstRstTerm     the last reset term
     * @param offset         the offset
     * @param messageId      the message id
     * @param firstCreate    whether is the first record creation
     * @param lstUpdateTime  the last update time
     */
    public OffsetStorageInfo(String topic, int brokerId, int partitionId,
            long lstRstTerm, long offset, long messageId,
            boolean firstCreate, long lstUpdateTime) {
        this.topic = topic;
        this.brokerId = brokerId;
        this.partitionId = partitionId;
        this.lstRstTerm = lstRstTerm;
        this.offset.set(offset - offset % DataStoreUtils.STORE_INDEX_HEAD_LEN);
        this.messageId = messageId;
        this.firstCreate = firstCreate;
        if (firstCreate) {
            this.modified = true;
            this.firstOffset = this.offset.get();
            this.createTime = lstUpdateTime;
        }
        this.lstUpdateTime.set(lstUpdateTime);
    }

    public boolean isFirstCreate() {
        return firstCreate;
    }

    public String getTopic() {
        return topic;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public long getLstRstTerm() {
        return lstRstTerm;
    }

    public long getOffset() {
        return offset.get();
    }

    public long getMessageId() {
        return messageId;
    }

    public long getLstUpdateTime() {
        return lstUpdateTime.get();
    }

    public boolean isModified() {
        return modified;
    }

    public long getFirstOffset() {
        return firstOffset;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setModified(boolean modified) {
        this.modified = modified;
    }

    public long addAndGetOffset(long tmpOffset) {
        this.firstCreate = false;
        this.modified = true;
        this.lstUpdateTime.set(System.currentTimeMillis());
        return this.offset.addAndGet(tmpOffset - tmpOffset % DataStoreUtils.STORE_INDEX_HEAD_LEN);
    }

    public long getAndSetOffset(long absOffset) {
        this.firstCreate = false;
        this.modified = true;
        this.lstUpdateTime.set(System.currentTimeMillis());
        return this.offset.getAndSet(absOffset - absOffset % DataStoreUtils.STORE_INDEX_HEAD_LEN);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OffsetStorageInfo)) {
            return false;
        }
        OffsetStorageInfo that = (OffsetStorageInfo) o;
        if (brokerId != that.brokerId) {
            return false;
        }
        if (partitionId != that.partitionId) {
            return false;
        }
        if (messageId != that.messageId) {
            return false;
        }
        if (firstCreate != that.firstCreate) {
            return false;
        }
        if (modified != that.modified) {
            return false;
        }
        if (lstRstTerm != that.lstRstTerm) {
            return false;
        }
        if (lstUpdateTime.get() != that.lstUpdateTime.get()) {
            return false;
        }
        if (!topic.equals(that.topic)) {
            return false;
        }
        return (offset.get() == that.offset.get());

    }

    public int hashCode() {
        int result = topic.hashCode();
        result = 31 * result + brokerId;
        result = 31 * result + partitionId;
        result = 31 * result + offset.hashCode();
        result = 31 * result + Long.hashCode(messageId);
        result = 31 * result + Long.hashCode(lstUpdateTime.get());
        result = 31 * result + Long.hashCode(lstRstTerm);
        result = 31 * result + (firstCreate ? 1 : 0);
        result = 31 * result + (modified ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return new StringBuilder(512).append("OffsetStorageInfo [OffsetStoreKey=")
                .append(topic).append(TokenConstants.HYPHEN).append(brokerId)
                .append(TokenConstants.HYPHEN).append(partitionId)
                .append(", offset=").append(offset.get())
                .append(", messageId=").append(messageId)
                .append(", modified=").append(modified)
                .append(", firstCreate=").append(firstCreate)
                .append(", firstOffset=").append(firstOffset)
                .append(", createTime=").append(createTime)
                .append(", lstUpdateTime=").append(lstUpdateTime.get())
                .append(", lstRstTerm=").append(lstRstTerm)
                .append("]").toString();
    }
}
