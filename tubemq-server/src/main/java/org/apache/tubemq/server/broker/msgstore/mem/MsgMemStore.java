/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.server.broker.msgstore.mem;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.corebase.TErrCodeConstants;
import org.apache.tubemq.server.broker.BrokerConfig;
import org.apache.tubemq.server.broker.metadata.ClusterConfigHolder;
import org.apache.tubemq.server.broker.msgstore.disk.MsgFileStore;
import org.apache.tubemq.server.broker.utils.DataStoreUtils;
import org.apache.tubemq.server.common.utils.AppendResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

/***
 * Message's memory storage. It use direct memory store messages that received but not have been flushed to disk.
 */
public class MsgMemStore implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(MsgMemStore.class);
    //　statistics of memory store
    private final AtomicInteger cacheDataOffset = new AtomicInteger(0);
    private final AtomicInteger cacheIndexOffset = new AtomicInteger(0);
    private final AtomicInteger curMessageCount = new AtomicInteger(0);
    private final ReentrantLock writeLock = new ReentrantLock();
    //　partitionId to index position, accelerate query
    private final ConcurrentHashMap<Integer, Integer> queuesMap =
            new ConcurrentHashMap<>(20);
    //　key to index position, used for filter consume
    private final ConcurrentHashMap<Integer, Integer> keysMap =
            new ConcurrentHashMap<>(100);
    //　where messages in memory will sink to disk
    private int maxDataCacheSize;
    private long writeDataStartPos = -1;
    private ByteBuffer cacheDataSegment;
    private int maxIndexCacheSize;
    private long writeIndexStartPos = -1;
    private ByteBuffer cachedIndexSegment;
    private int maxAllowedMsgCount;


    public MsgMemStore(int maxCacheSize, int maxMsgCount, final BrokerConfig tubeConfig) {
        this.maxDataCacheSize = maxCacheSize;
        this.maxAllowedMsgCount = maxMsgCount;
        this.maxIndexCacheSize = this.maxAllowedMsgCount * DataStoreUtils.STORE_INDEX_HEAD_LEN;
        this.cacheDataSegment = ByteBuffer.allocateDirect(this.maxDataCacheSize);
        this.cachedIndexSegment = ByteBuffer.allocateDirect(this.maxIndexCacheSize);
    }

    public void resetStartPos(long writeDataStartPos, long writeIndexStartPos) {
        this.clear();
        this.writeDataStartPos = writeDataStartPos;
        this.writeIndexStartPos = writeIndexStartPos;
    }

    public boolean appendMsg(final MsgMemStatisInfo msgMemStatisInfo,
                             final int partitionId, final int keyCode,
                             final long timeRecv, final int entryLength,
                             final ByteBuffer entry, final AppendResult appendResult) {
        boolean fullDataSize = false;
        boolean fullIndexSize = false;
        boolean fullCount = false;
        long indexOffset = TBaseConstants.META_VALUE_UNDEFINED;
        long dataOffset = TBaseConstants.META_VALUE_UNDEFINED;
        this.writeLock.lock();
        try {
            //　judge whether can write to memory or not.
            if ((fullDataSize = (this.cacheDataOffset.get() + entryLength > this.maxDataCacheSize))
                || (fullIndexSize =
                (this.cacheIndexOffset.get() + DataStoreUtils.STORE_INDEX_HEAD_LEN > this.maxIndexCacheSize))
                || (fullCount = (this.curMessageCount.get() + 1 > maxAllowedMsgCount))) {
                msgMemStatisInfo.addFullTypeCount(timeRecv, fullDataSize, fullIndexSize, fullCount);
                return false;
            }
            // conduct message with filling process
            indexOffset = this.writeIndexStartPos + this.cacheIndexOffset.get();
            dataOffset = this.writeDataStartPos + this.cacheDataOffset.get();
            entry.putLong(DataStoreUtils.STORE_HEADER_POS_QUEUE_LOGICOFF, indexOffset);
            this.cacheDataSegment.position(this.cacheDataOffset.get());
            this.cacheDataSegment.put(entry.array());
            this.cachedIndexSegment.position(this.cacheIndexOffset.get());
            this.cachedIndexSegment.putInt(partitionId);
            this.cachedIndexSegment.putLong(dataOffset);
            this.cachedIndexSegment.putInt(entryLength);
            this.cachedIndexSegment.putInt(keyCode);
            this.cachedIndexSegment.putLong(timeRecv);
            this.cacheDataOffset.getAndAdd(entryLength);
            Integer indexSizePos = this.cacheIndexOffset.getAndAdd(DataStoreUtils.STORE_INDEX_HEAD_LEN);
            this.queuesMap.put(partitionId, indexSizePos);
            this.keysMap.put(keyCode, indexSizePos);
            this.curMessageCount.getAndAdd(1);
            msgMemStatisInfo.addMsgSizeStatis(timeRecv, entryLength);
        } finally {
            this.writeLock.unlock();
        }
        appendResult.putAppendResult(indexOffset, dataOffset);
        return true;
    }

    /***
     * Read from memory, read index, then data.
     *
     * @param lstRdDataOffset
     * @param lstRdIndexOffset
     * @param maxReadSize
     * @param maxReadCount
     * @param partitionId
     * @param isSecond
     * @param isFilterConsume
     * @param filterKeySet
     * @return
     */
    public GetCacheMsgResult getMessages(final long lstRdDataOffset, final long lstRdIndexOffset,
                                         final int maxReadSize, final int maxReadCount,
                                         final int partitionId, final boolean isSecond,
                                         final boolean isFilterConsume,
                                         final Set<Integer> filterKeySet) {
        // #lizard forgives
        Integer lastWritePos = 0;
        boolean hasMsg = false;
        //　judge memory contains the given offset or not.
        List<ByteBuffer> cacheMsgList = new ArrayList<>();
        if (lstRdIndexOffset < this.writeIndexStartPos) {
            return new GetCacheMsgResult(false, TErrCodeConstants.MOVED,
                    lstRdIndexOffset, "Request offset lower than cache minOffset");
        }
        if (lstRdIndexOffset >= this.writeIndexStartPos + this.cacheIndexOffset.get()) {
            return new GetCacheMsgResult(false, TErrCodeConstants.NOT_FOUND,
                    lstRdIndexOffset, "Request offset reached cache maxOffset");
        }
        int totalReadSize = 0;
        int currIndexOffset;
        int currDataOffset;
        long lastDataRdOff = lstRdDataOffset;
        int startReadOff = (int) (lstRdIndexOffset - this.writeIndexStartPos);
        this.writeLock.lock();
        try {
            if (isFilterConsume) {
                //　filter conduct. accelerate by keysMap.
                for (Integer keyCode : filterKeySet) {
                    if (keyCode != null) {
                        lastWritePos = this.keysMap.get(keyCode);
                        if ((lastWritePos != null) && (lastWritePos >= startReadOff)) {
                            hasMsg = true;
                            break;
                        }
                    }
                }
            } else {
                // orderly consume by partition id.
                lastWritePos = this.queuesMap.get(partitionId);
                if ((lastWritePos != null) && (lastWritePos >= startReadOff)) {
                    hasMsg = true;
                }
            }
            currDataOffset = this.cacheDataOffset.get();
            currIndexOffset = this.cacheIndexOffset.get();
            lastDataRdOff = this.writeDataStartPos + currDataOffset;
        } finally {
            this.writeLock.unlock();
        }
        int limitReadSize = currIndexOffset - startReadOff;
        // cannot find message, return not found
        if (!hasMsg) {
            if (isSecond && !isFilterConsume) {
                return new GetCacheMsgResult(true, 0, "Ok2",
                        lstRdIndexOffset, limitReadSize, lastDataRdOff, totalReadSize, cacheMsgList);
            } else {
                return new GetCacheMsgResult(false, TErrCodeConstants.NOT_FOUND,
                        "Can't found Message by index!", lstRdIndexOffset,
                        limitReadSize, lastDataRdOff, totalReadSize, cacheMsgList);
            }
        }
        //　fetch data by index.
        int readedSize = 0;
        int cPartitionId = 0;
        long cDataPos = 0L;
        int cDataSize = 0;
        int cKeyCode = 0;
        long cTimeRecv = 0L;
        int cDataOffset = 0;
        ByteBuffer tmpIndexRdBuf = this.cachedIndexSegment.asReadOnlyBuffer();
        ByteBuffer tmpDataRdBuf = this.cacheDataSegment.asReadOnlyBuffer();
        //　loop read by index
        for (int count = 0; count < maxReadCount;
             count++, startReadOff += DataStoreUtils.STORE_INDEX_HEAD_LEN) {
            //　cannot find matched message, return
            if ((startReadOff >= currIndexOffset)
                || (startReadOff + DataStoreUtils.STORE_INDEX_HEAD_LEN > currIndexOffset)) {
                break;
            }
            // read index content.
            tmpIndexRdBuf.position(startReadOff);
            cPartitionId = tmpIndexRdBuf.getInt();
            cDataPos = tmpIndexRdBuf.getLong();
            cDataSize = tmpIndexRdBuf.getInt();
            cKeyCode = tmpIndexRdBuf.getInt();
            cTimeRecv = tmpIndexRdBuf.getLong();
            cDataOffset = (int) (cDataPos - this.writeDataStartPos);
            //　skip when mismatch condition
            if ((cDataOffset < 0)
                    || (cDataSize <= 0)
                    || (cDataOffset >= currDataOffset)
                    || (cDataSize > ClusterConfigHolder.getMaxMsgSize())
                    || (cDataOffset + cDataSize > currDataOffset)) {
                readedSize += DataStoreUtils.STORE_INDEX_HEAD_LEN;
                continue;
            }
            if ((cPartitionId != partitionId)
                    || (isFilterConsume && (!filterKeySet.contains(cKeyCode)))) {
                readedSize += DataStoreUtils.STORE_INDEX_HEAD_LEN;
                continue;
            }
            //　read data file.
            byte[] tmpArray = new byte[cDataSize];
            final ByteBuffer buffer = ByteBuffer.wrap(tmpArray);
            tmpDataRdBuf.position(cDataOffset);
            tmpDataRdBuf.get(tmpArray);
            buffer.rewind();
            cacheMsgList.add(buffer);
            lastDataRdOff = cDataPos + cDataSize;
            readedSize += DataStoreUtils.STORE_INDEX_HEAD_LEN;
            totalReadSize += cDataSize;
            // break when exceed the max transfer size.
            if (totalReadSize >= maxReadSize) {
                break;
            }
        }
        // return result
        return new GetCacheMsgResult(true, 0, "Ok1",
                lstRdIndexOffset, readedSize, lastDataRdOff, totalReadSize, cacheMsgList);
    }

    /***
     * Batch flush memory data to disk.
     *
     * @param msgFileStore
     * @param strBuffer
     * @return
     * @throws IOException
     */
    public boolean batchFlush(MsgFileStore msgFileStore,
                              final StringBuilder strBuffer) throws Throwable {
        if (this.curMessageCount.get() == 0) {
            return true;
        }
        ByteBuffer tmpIndexBuffer = this.cachedIndexSegment.asReadOnlyBuffer();
        final ByteBuffer tmpDataReadBuf = this.cacheDataSegment.asReadOnlyBuffer();
        tmpIndexBuffer.flip();
        tmpDataReadBuf.flip();
        msgFileStore.batchAppendMsg(strBuffer, curMessageCount.get(),
            cacheIndexOffset.get(), tmpIndexBuffer, cacheDataOffset.get(), tmpDataReadBuf);
        return true;
    }

    public int getCurMsgCount() {
        return this.curMessageCount.get();
    }

    public int getCurDataCacheSize() {
        return this.cacheDataOffset.get();
    }

    public int getIndexCacheSize() {
        return this.cacheIndexOffset.get();
    }

    public int getMaxDataCacheSize() {
        return this.maxDataCacheSize;
    }

    public int getMaxAllowedMsgCount() {
        return this.maxAllowedMsgCount;
    }

    public int isOffsetInHold(long requestOffset) {
        if (requestOffset < this.writeIndexStartPos) {
            return -1;
        } else if (requestOffset >= this.writeIndexStartPos + this.cacheIndexOffset.get()) {
            return 1;
        }
        return 0;
    }

    public long getDataLastWritePos() {
        return this.writeDataStartPos + this.cacheDataOffset.get();
    }

    public long getIndexLastWritePos() {
        return this.writeIndexStartPos + this.cacheIndexOffset.get();
    }

    public void clear() {
        this.writeDataStartPos = -1;
        this.writeIndexStartPos = -1;
        this.cacheDataOffset.set(0);
        this.cacheIndexOffset.set(0);
        this.curMessageCount.set(0);
        this.queuesMap.clear();
        this.keysMap.clear();
    }

    @Override
    public void close() {
        ((DirectBuffer) this.cacheDataSegment).cleaner().clean();
        ((DirectBuffer) this.cachedIndexSegment).cleaner().clean();
    }

}
