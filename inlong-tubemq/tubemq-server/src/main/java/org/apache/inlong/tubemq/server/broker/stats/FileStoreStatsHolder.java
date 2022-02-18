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
import org.apache.inlong.tubemq.corebase.metric.impl.LongStatsCounter;
import org.apache.inlong.tubemq.corebase.metric.impl.SimpleHistogram;
import org.apache.inlong.tubemq.corebase.metric.impl.SinceTime;
import org.apache.inlong.tubemq.corebase.utils.DateTimeConvertUtils;
import org.apache.inlong.tubemq.server.common.TServerConstants;

/**
 * FileStoreStatsHolder, The statistics set related to the file store.
 *
 * Through this class, we complete the collection of metric data that occurs in
 * the memory cache operation, including the number of messages produced, size,
 * number of times of flushing, distribution of flushing time,
 * and triggering conditions for flushing, etc.
 *
 * This part supports index comparison output before and after data collection.
 */
public class FileStoreStatsHolder {
    // Switchable statistic items
    private final FileStatsItemSet[] fileStatsSets = new FileStatsItemSet[2];
    // Current writable index
    private final AtomicInteger writableIndex = new AtomicInteger(0);
    // Last query time
    private final AtomicLong lstQueryTime = new AtomicLong(0);
    // Last snapshot time
    private final AtomicLong lstSnapshotTime = new AtomicLong(0);
    // whether the statistic is closed
    private volatile boolean isClosed;

    public FileStoreStatsHolder() {
        this.isClosed = true;
        this.fileStatsSets[0] = new FileStatsItemSet();
        this.fileStatsSets[1] = new FileStatsItemSet();
        this.lstQueryTime.set(System.currentTimeMillis());
        this.lstSnapshotTime.set(System.currentTimeMillis());
    }

    /**
     * Add message store statistics information.
     *
     * @param msgCnt             the message count written
     * @param msgIndexSize       the message index size written
     * @param msgDataSize        the message data size written
     * @param flushedMsgCnt      the flushed message count
     * @param flushedDataSize    the flushed message size
     * @param isDataSegFlush     whether the data segment flushed
     * @param isIndexSegFlush    whether the index segment flushed
     * @param isDataSizeFull     whether the cached data is full
     * @param isMsgCntFull       whether the cached message count is full
     * @param isCacheTimeFull    whether the cached time is full
     * @param isForceMetadata    whether force push metadata
     */
    public void addFileFlushStatsInfo(int msgCnt, int msgIndexSize, int msgDataSize,
                                      long flushedMsgCnt, long flushedDataSize,
                                      boolean isDataSegFlush, boolean isIndexSegFlush,
                                      boolean isDataSizeFull, boolean isMsgCntFull,
                                      boolean isCacheTimeFull, boolean isForceMetadata) {
        if (isClosed) {
            return;
        }
        FileStatsItemSet tmStatsSet = fileStatsSets[getIndex()];
        tmStatsSet.accumMsgCnt.addValue(msgCnt);
        tmStatsSet.accumMsgIndexSize.addValue(msgIndexSize);
        tmStatsSet.accumMsgDataSize.addValue(msgDataSize);
        if (flushedDataSize > 0) {
            tmStatsSet.flushedDataSizeStats.update(flushedDataSize);
        }
        if (flushedMsgCnt > 0) {
            tmStatsSet.flushedMsgCntStats.update(flushedMsgCnt);
        }
        if (isDataSegFlush) {
            tmStatsSet.dataSegAddCnt.incValue();
        }
        if (isIndexSegFlush) {
            tmStatsSet.indexSegAddCnt.incValue();
        }
        if (isDataSizeFull) {
            tmStatsSet.dataSizeFullCnt.incValue();
        }
        if (isMsgCntFull) {
            tmStatsSet.msgCountFullCnt.incValue();
        }
        if (isCacheTimeFull) {
            tmStatsSet.cachedTimeFullCnt.incValue();
        }
        if (isForceMetadata) {
            tmStatsSet.metaFlushCnt.incValue();
        }
    }

    /**
     * Add flush time timeout statistic.
     *
     * @param flushedMsgCnt      the flushed message count
     * @param flushedDataSize    the flushed message size
     * @param isForceMetadata    whether force push metadata
     */
    public void addTimeoutFlush(long flushedMsgCnt,
                                long flushedDataSize,
                                boolean isForceMetadata) {
        if (isClosed) {
            return;
        }
        FileStatsItemSet tmStatsSet = fileStatsSets[getIndex()];
        tmStatsSet.cachedTimeFullCnt.incValue();
        if (flushedDataSize > 0) {
            tmStatsSet.flushedDataSizeStats.update(flushedDataSize);
        }
        if (flushedMsgCnt > 0) {
            tmStatsSet.flushedMsgCntStats.update(flushedMsgCnt);
        }
        if (isForceMetadata) {
            tmStatsSet.metaFlushCnt.incValue();
        }
    }

    /**
     * Check whether has exceeded the maximum self-statistics period.
     *
     * @param checkTime   the check time
     */
    public synchronized void chkStatsExpired(long checkTime) {
        if (!this.isClosed) {
            if ((checkTime - this.lstQueryTime.get())
                    >= TServerConstants.CFG_STORE_STATS_MAX_REFRESH_DURATION) {
                this.isClosed = true;
            }
        }
    }

    /**
     * Get current writing statistics information.
     *
     * @param statsMap  the return map information
     */
    public void getValue(Map<String, Long> statsMap) {
        enableStats();
        getStatsValue(true, fileStatsSets[getIndex()], statsMap);
    }

    /**
     * Get current writing statistics information.
     *
     * @param strBuff  the return information in json format
     */
    public void getValue(StringBuilder strBuff) {
        enableStats();
        getStatsValue(true, fileStatsSets[getIndex()], strBuff);
    }

    /**
     * Snapshot and get current writing statistics information.
     *
     * @param statsMap  the return map information
     */
    public void snapShort(Map<String, Long> statsMap) {
        enableStats();
        if (switchStatsSets()) {
            getStatsValue(false, fileStatsSets[getIndex(writableIndex.get() - 1)], statsMap);
        } else {
            getStatsValue(true, fileStatsSets[getIndex()], statsMap);
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
            getStatsValue(false, fileStatsSets[getIndex(writableIndex.get() - 1)], strBuff);
        } else {
            getStatsValue(true, fileStatsSets[getIndex()], strBuff);
        }
    }

    /**
     * Get current file store statistics information.
     * Contains the data results of the current statistics and the previous snapshot
     *
     * @param isSwitch  whether to switch the writing statistics block
     * @param strBuff   the return information
     */
    public synchronized void getAllFileStatsInfo(boolean isSwitch, StringBuilder strBuff) {
        this.enableStats();
        strBuff.append("[");
        getStatsValue(false, fileStatsSets[getIndex(writableIndex.get() - 1)], strBuff);
        strBuff.append(",");
        getStatsValue(true, fileStatsSets[getIndex()], strBuff);
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
                fileStatsSets[getIndex(writableIndex.get() - 1)].clear();
                int befIndex = writableIndex.getAndIncrement();
                fileStatsSets[getIndex(befIndex)].setSnapshotTime(lstSnapshotTime.get());
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
                               FileStatsItemSet statsSet,
                               Map<String, Long> statsMap) {
        statsMap.put(statsSet.resetTime.getFullName(),
                statsSet.resetTime.getSinceTime());
        statsMap.put(statsSet.accumMsgCnt.getFullName(),
                statsSet.accumMsgCnt.getValue());
        statsMap.put(statsSet.accumMsgDataSize.getFullName(),
                statsSet.accumMsgDataSize.getValue());
        statsMap.put(statsSet.accumMsgIndexSize.getFullName(),
                statsSet.accumMsgIndexSize.getValue());
        statsSet.flushedDataSizeStats.getValue(statsMap, false);
        statsSet.flushedMsgCntStats.getValue(statsMap, false);
        statsMap.put(statsSet.dataSegAddCnt.getFullName(),
                statsSet.dataSegAddCnt.getValue());
        statsMap.put(statsSet.indexSegAddCnt.getFullName(),
                statsSet.indexSegAddCnt.getValue());
        statsMap.put(statsSet.metaFlushCnt.getFullName(),
                statsSet.metaFlushCnt.getValue());
        statsMap.put(statsSet.dataSizeFullCnt.getFullName(),
                statsSet.dataSizeFullCnt.getValue());
        statsMap.put(statsSet.msgCountFullCnt.getFullName(),
                statsSet.msgCountFullCnt.getValue());
        statsMap.put(statsSet.cachedTimeFullCnt.getFullName(),
                statsSet.cachedTimeFullCnt.getValue());
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
                                      FileStatsItemSet statsSet,
                                      StringBuilder strBuff) {
        strBuff.append("{\"").append(statsSet.resetTime.getFullName())
                .append("\":\"").append(statsSet.resetTime.getStrSinceTime())
                .append("\",\"").append(statsSet.accumMsgCnt.getFullName())
                .append("\":").append(statsSet.accumMsgCnt.getValue())
                .append(",\"").append(statsSet.accumMsgDataSize.getFullName())
                .append("\":").append(statsSet.accumMsgDataSize.getValue())
                .append(",\"").append(statsSet.accumMsgIndexSize.getFullName())
                .append("\":").append(statsSet.accumMsgIndexSize.getValue())
                .append(",");
        statsSet.flushedDataSizeStats.getValue(strBuff, false);
        strBuff.append(",");
        statsSet.flushedMsgCntStats.getValue(strBuff, false);
        strBuff.append(",\"").append(statsSet.dataSegAddCnt.getFullName())
                .append("\":").append(statsSet.dataSegAddCnt.getValue())
                .append(",\"").append(statsSet.indexSegAddCnt.getFullName())
                .append("\":").append(statsSet.indexSegAddCnt.getValue())
                .append(",\"").append(statsSet.metaFlushCnt.getFullName())
                .append("\":").append(statsSet.metaFlushCnt.getValue())
                .append(",\"").append(statsSet.dataSizeFullCnt.getFullName())
                .append("\":").append(statsSet.dataSizeFullCnt.getValue())
                .append(",\"").append(statsSet.msgCountFullCnt.getFullName())
                .append("\":").append(statsSet.msgCountFullCnt.getValue())
                .append(",\"").append(statsSet.cachedTimeFullCnt.getFullName())
                .append("\":").append(statsSet.cachedTimeFullCnt.getValue())
                .append(",\"").append(statsSet.snapShotTime.getFullName())
                .append("\":\"");
        if (isWriting) {
            strBuff.append(DateTimeConvertUtils.ms2yyyyMMddHHmmss(System.currentTimeMillis()));
        } else {
            strBuff.append(statsSet.snapShotTime.getStrSinceTime());
        }
        strBuff.append("\"}");
    }

    /**
     * FileStatsItemSet, a collection of statistical metrics related to file storage
     *
     */
    private static class FileStatsItemSet {
        // The reset time of file statistics set
        protected final SinceTime resetTime =
                new SinceTime("reset_time", null);
        // The accumulate message count statistics
        protected final LongStatsCounter accumMsgCnt =
                new LongStatsCounter("total_msg_cnt", null);
        // The accumulate message data size statistics
        protected final LongStatsCounter accumMsgDataSize =
                new LongStatsCounter("total_data_size", null);
        // The accumulate message index statistics
        protected final LongStatsCounter accumMsgIndexSize =
                new LongStatsCounter("total_index_size", null);
        // The data flushed statistics
        protected final SimpleHistogram flushedDataSizeStats =
                new SimpleHistogram("flushed_data_size", null);
        // The message count flushed statistics
        protected final SimpleHistogram flushedMsgCntStats =
                new SimpleHistogram("flushed_msg_cnt", null);
        // The new data segment statistics
        protected final LongStatsCounter dataSegAddCnt =
                new LongStatsCounter("data_seg_cnt", null);
        // The new index segment full statistics
        protected final LongStatsCounter indexSegAddCnt =
                new LongStatsCounter("index_seg_cnt", null);
        // The flush count statistics of file meta-data
        protected final LongStatsCounter metaFlushCnt =
                new LongStatsCounter("meta_flush_cnt", null);
        // The cached message data full statistics
        protected final LongStatsCounter dataSizeFullCnt =
                new LongStatsCounter("data_size_full", null);
        // The cached message count full statistics
        protected final LongStatsCounter msgCountFullCnt =
                new LongStatsCounter("msg_count_full", null);
        // The cache timeout refresh amount statistics
        protected final LongStatsCounter cachedTimeFullCnt =
                new LongStatsCounter("cache_time_full", null);
        // The snapshot time of file statistics set
        protected final SinceTime snapShotTime =
                new SinceTime("end_time", null);

        public FileStatsItemSet() {
            clear();
        }

        public void setSnapshotTime(long snapshotTime) {
            this.snapShotTime.reset(snapshotTime);
        }

        public void clear() {
            this.snapShotTime.reset();
            this.accumMsgCnt.clear();
            this.accumMsgDataSize.clear();
            this.flushedDataSizeStats.clear();
            this.accumMsgIndexSize.clear();
            this.flushedMsgCntStats.clear();
            this.dataSegAddCnt.clear();
            this.indexSegAddCnt.clear();
            this.dataSizeFullCnt.clear();
            this.metaFlushCnt.clear();
            this.msgCountFullCnt.clear();
            this.cachedTimeFullCnt.clear();
            this.resetTime.reset();
        }
    }
}
