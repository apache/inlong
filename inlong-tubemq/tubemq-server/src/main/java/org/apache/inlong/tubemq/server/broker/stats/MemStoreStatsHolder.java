/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.tubemq.server.broker.stats;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.inlong.tubemq.corebase.metric.impl.ESTHistogram;
import org.apache.inlong.tubemq.corebase.metric.impl.LongStatsCounter;
import org.apache.inlong.tubemq.corebase.metric.impl.SimpleHistogram;
import org.apache.inlong.tubemq.corebase.metric.impl.SinceTime;
import org.apache.inlong.tubemq.corebase.utils.DateTimeConvertUtils;
import org.apache.inlong.tubemq.server.common.TServerConstants;

/**
 * MemStoreStatsHolder, The statistics set related to the memory cache.
 *
 * Through this class, we complete the collection of metric data that occurs in
 * the memory cache operation, including the number of messages produced, size,
 * number of times of flushing, distribution of flushing time,
 * and triggering conditions for flushing, etc.
 *
 * This part supports index comparison output before and after data collection.
 */
public class MemStoreStatsHolder {
    // Switchable statistic items
    private final MemStatsItemSet[] memStatsSets = new MemStatsItemSet[2];
    // Current writable index
    private final AtomicInteger writableIndex = new AtomicInteger(0);
    // Last query time
    private final AtomicLong lstQueryTime = new AtomicLong(0);
    // Last snapshot time
    private final AtomicLong lstSnapshotTime = new AtomicLong(0);
    // whether the statistic is closed
    private volatile boolean isClosed;

    public MemStoreStatsHolder() {
        this.isClosed = true;
        this.memStatsSets[0] = new MemStatsItemSet();
        this.memStatsSets[1] = new MemStatsItemSet();
        this.lstQueryTime.set(System.currentTimeMillis());
        this.lstSnapshotTime.set(System.currentTimeMillis());
    }

    /**
     * Add appended message size statistic.
     *
     * @param msgSize   the message size
     */
    public void addAppendedMsgSize(int msgSize) {
        if (isClosed) {
            return;
        }
        memStatsSets[getIndex()].msgRcvStats.update(msgSize);
    }

    /**
     * Add write message failure count statistics.
     */
    public void addMsgWriteFail() {
        if (isClosed) {
            return;
        }
        memStatsSets[getIndex()].writeFailCnt.incValue();
    }

    /**
     * Add cache pending count statistics.
     */
    public void addCachePending() {
        if (isClosed) {
            return;
        }
        memStatsSets[getIndex()].flushPendingCnt.incValue();
    }

    /**
     * Add cache re-alloc count statistics.
     */
    public void addCacheReAlloc() {
        if (isClosed) {
            return;
        }
        memStatsSets[getIndex()].cacheReAllocCnt.incValue();
    }

    /**
     * Add flush trigger type statistics.
     *
     * @param isDataSizeFull     whether the cached data is full
     * @param isIndexSizeFull    whether the cached index is full
     * @param isMsgCntFull       whether the cached message count is full
     */
    public void addCacheFullType(boolean isDataSizeFull,
                                 boolean isIndexSizeFull,
                                 boolean isMsgCntFull) {
        if (isClosed) {
            return;
        }
        if (isDataSizeFull) {
            memStatsSets[getIndex()].dataSizeFullCnt.incValue();
        }
        if (isIndexSizeFull) {
            memStatsSets[getIndex()].indexSizeFullCnt.incValue();
        }
        if (isMsgCntFull) {
            memStatsSets[getIndex()].msgCountFullCnt.incValue();
        }
    }

    /**
     * Add flush time statistic.
     *
     * @param flushTime          the flush time
     * @param isTimeoutFlush     whether is timeout flush
     */
    public void addFlushTime(long flushTime, boolean isTimeoutFlush) {
        if (isClosed) {
            return;
        }
        memStatsSets[getIndex()].cacheSyncStats.update(flushTime);
        if (isTimeoutFlush) {
            memStatsSets[getIndex()].cachedTimeFullCnt.incValue();
        }
    }

    /**
     * Check whether has exceeded the maximum self-statistics period.
     *
     * @param checkTime   the check time
     */
    public synchronized void chkStatsExpired(long checkTime) {
        if ((checkTime - this.lstQueryTime.get())
                >= TServerConstants.CFG_STORE_STATS_MAX_REFRESH_DURATION) {
            this.isClosed = true;
        }
    }

    /**
     * Get current writing statistics information.
     *
     * @param statsMap  the return map information
     */
    public void getValue(Map<String, Long> statsMap) {
        enableStats();
        getStatsValue(true, memStatsSets[getIndex()], statsMap);
    }

    /**
     * Get current writing statistics information.
     *
     * @param strBuff  the return information in json format
     */
    public void getValue(StringBuilder strBuff) {
        enableStats();
        getStatsValue(true, memStatsSets[getIndex()], strBuff);
    }

    /**
     * Snapshot and get current writing statistics information.
     *
     * @param statsMap  the return map information
     */
    public void snapShort(Map<String, Long> statsMap) {
        enableStats();
        if (switchStatsSets()) {
            getStatsValue(false, memStatsSets[getIndex(writableIndex.get() - 1)], statsMap);
        } else {
            getStatsValue(true, memStatsSets[getIndex()], statsMap);
        }
    }

    /**
     * Snapshot and get current writing statistics information.
     *
     * @param strBuff  the return information in json format
     */
    public void snapShort(StringBuilder strBuff) {
        this.enableStats();
        if (switchStatsSets()) {
            getStatsValue(false, memStatsSets[getIndex(writableIndex.get() - 1)], strBuff);
        } else {
            getStatsValue(true, memStatsSets[getIndex()], strBuff);
        }
    }

    /**
     * Get current memory store statistics information. Contains the data results of
     * the current statistics and the previous snapshot
     *
     * @param isSwitch  whether to switch the writing statistics block
     * @param strBuff   the return information
     */
    public synchronized void getAllMemStatsInfo(boolean isSwitch, StringBuilder strBuff) {
        this.enableStats();
        strBuff.append("[");
        getStatsValue(false, memStatsSets[getIndex(writableIndex.get() - 1)], strBuff);
        strBuff.append(",");
        getStatsValue(true, memStatsSets[getIndex()], strBuff);
        strBuff.append("]");
        if (isSwitch) {
            switchStatsSets();
        }
    }

    /**
     * Set statistic status
     *
     */
    private synchronized void enableStats() {
        this.lstQueryTime.set(System.currentTimeMillis());
        if (this.isClosed) {
            this.isClosed = false;
        }
    }

    /**
     * Check and switch statistic sets
     *
     * @return  whether the statistic sets has been switched
     */
    private boolean switchStatsSets() {
        long curSwitchTime = lstSnapshotTime.get();
        // Avoid frequent snapshots
        if ((System.currentTimeMillis() - curSwitchTime)
                >= TServerConstants.MIN_SNAPSHOT_PERIOD_MS) {
            if (lstSnapshotTime.compareAndSet(curSwitchTime, System.currentTimeMillis())) {
                memStatsSets[getIndex(writableIndex.get() - 1)].clear();
                int befIndex = writableIndex.getAndIncrement();
                memStatsSets[getIndex(befIndex)].setSnapshotTime(lstSnapshotTime.get());
                return true;
            }
        }
        return false;
    }

    /**
     * Get current writable block index.
     *
     * @return the writable block index
     */
    private int getIndex() {
        return getIndex(writableIndex.get());
    }

    /**
     * Gets the metric block index based on the specified value.
     *
     * @param origIndex    the specified value
     * @return the metric block index
     */
    private int getIndex(int origIndex) {
        return Math.abs(origIndex % 2);
    }

    /**
     * Read metric block data into map.
     *
     * @param isWriting    the metric block is writing
     * @param statsSet     the metric block need to read
     * @param statsMap     the read result
     */
    private void getStatsValue(boolean isWriting,
                               MemStatsItemSet statsSet,
                               Map<String, Long> statsMap) {
        statsMap.put(statsSet.resetTime.getFullName(),
                statsSet.resetTime.getSinceTime());
        statsSet.msgRcvStats.getValue(statsMap, false);
        statsMap.put(statsSet.writeFailCnt.getFullName(),
                statsSet.writeFailCnt.getValue());
        statsMap.put(statsSet.dataSizeFullCnt.getFullName(),
                statsSet.dataSizeFullCnt.getValue());
        statsMap.put(statsSet.indexSizeFullCnt.getFullName(),
                statsSet.indexSizeFullCnt.getValue());
        statsMap.put(statsSet.msgCountFullCnt.getFullName(),
                statsSet.msgCountFullCnt.getValue());
        statsMap.put(statsSet.cachedTimeFullCnt.getFullName(),
                statsSet.cachedTimeFullCnt.getValue());
        statsMap.put(statsSet.flushPendingCnt.getFullName(),
                statsSet.flushPendingCnt.getValue());
        statsMap.put(statsSet.cacheReAllocCnt.getFullName(),
                statsSet.cacheReAllocCnt.getValue());
        statsSet.cacheSyncStats.getValue(statsMap, false);
        if (isWriting) {
            statsMap.put(statsSet.snapShotTime.getFullName(),
                    System.currentTimeMillis());
        } else {
            statsMap.put(statsSet.snapShotTime.getFullName(),
                    statsSet.snapShotTime.getSinceTime());
        }
    }

    /**
     * Read metric block data into string format.
     *
     * @param isWriting    the metric block is writing
     * @param statsSet     the metric block need to read
     * @param strBuff     the return buffer
     */
    private static void getStatsValue(boolean isWriting,
                                      MemStatsItemSet statsSet,
                                      StringBuilder strBuff) {
        strBuff.append("{\"").append(statsSet.resetTime.getFullName())
                .append("\":\"").append(statsSet.resetTime.getStrSinceTime())
                .append("\",");
        statsSet.msgRcvStats.getValue(strBuff, false);
        strBuff.append(",\"").append(statsSet.writeFailCnt.getFullName())
                .append("\":").append(statsSet.writeFailCnt.getValue())
                .append(",\"").append(statsSet.dataSizeFullCnt.getFullName())
                .append("\":").append(statsSet.dataSizeFullCnt.getValue())
                .append(",\"").append(statsSet.msgCountFullCnt.getFullName())
                .append("\":").append(statsSet.msgCountFullCnt.getValue())
                .append(",\"").append(statsSet.cachedTimeFullCnt.getFullName())
                .append("\":").append(statsSet.cachedTimeFullCnt.getValue())
                .append(",\"").append(statsSet.flushPendingCnt.getFullName())
                .append("\":").append(statsSet.flushPendingCnt.getValue())
                .append(",\"").append(statsSet.cacheReAllocCnt.getFullName())
                .append("\":").append(statsSet.cacheReAllocCnt.getValue())
                .append(",\"").append(statsSet.dataSizeFullCnt.getFullName())
                .append("\":").append(statsSet.dataSizeFullCnt.getValue())
                .append(",");
        statsSet.cacheSyncStats.getValue(strBuff, false);
        strBuff.append(",\"").append(statsSet.snapShotTime.getFullName()).append("\":\"");
        if (isWriting) {
            strBuff.append(DateTimeConvertUtils.ms2yyyyMMddHHmmss(System.currentTimeMillis()));
        } else {
            strBuff.append(statsSet.snapShotTime.getStrSinceTime());
        }
        strBuff.append("\"}");
    }

    /**
     * MemStatsItemSet, Memory cache related statistics set
     *
     */
    private static class MemStatsItemSet {
        // The reset time of memory statistics set
        protected final SinceTime resetTime =
                new SinceTime("reset_time", null);
        // The status of messages received
        protected final SimpleHistogram msgRcvStats =
                new SimpleHistogram("msg_in", null);
        // The count of message append failures
        protected final LongStatsCounter writeFailCnt =
                new LongStatsCounter("msg_append_fail", null);
        // The cached message data full statistics
        protected final LongStatsCounter dataSizeFullCnt =
                new LongStatsCounter("data_size_full", null);
        // The cached message index full statistics
        protected final LongStatsCounter indexSizeFullCnt =
                new LongStatsCounter("index_size_full", null);
        // The cached message count full statistics
        protected final LongStatsCounter msgCountFullCnt =
                new LongStatsCounter("msg_count_full", null);
        // The cache timeout refresh amount statistics
        protected final LongStatsCounter cachedTimeFullCnt =
                new LongStatsCounter("cache_time_full", null);
        // The pending count for cache flush operations
        protected final LongStatsCounter flushPendingCnt =
                new LongStatsCounter("flush_pending", null);
        // The cache re-alloc count
        protected final LongStatsCounter cacheReAllocCnt =
                new LongStatsCounter("cache_realloc", null);
        // The cache persistence duration statistics
        protected final ESTHistogram cacheSyncStats =
                new ESTHistogram("cache_flush_dlt", null);
        // The snapshot time of memory statistics set
        protected final SinceTime snapShotTime =
                new SinceTime("read_time", null);

        public MemStatsItemSet() {
            clear();
        }

        public void setSnapshotTime(long snapshotTime) {
            this.snapShotTime.reset(snapshotTime);
        }

        public void clear() {
            this.snapShotTime.reset();
            this.msgRcvStats.clear();
            this.writeFailCnt.clear();
            this.dataSizeFullCnt.clear();
            this.indexSizeFullCnt.clear();
            this.msgCountFullCnt.clear();
            this.flushPendingCnt.clear();
            this.cacheReAllocCnt.clear();
            this.cachedTimeFullCnt.clear();
            this.cacheSyncStats.clear();
            this.resetTime.reset();
        }
    }
}
