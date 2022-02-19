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

package org.apache.inlong.tubemq.client.common;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.inlong.tubemq.corebase.TBaseConstants;
import org.apache.inlong.tubemq.corebase.TErrCodeConstants;
import org.apache.inlong.tubemq.corebase.metric.TrafficStatsUnit;
import org.apache.inlong.tubemq.corebase.metric.impl.ESTHistogram;
import org.apache.inlong.tubemq.corebase.metric.impl.LongStatsCounter;
import org.apache.inlong.tubemq.corebase.metric.impl.SinceTime;
import org.apache.inlong.tubemq.corebase.utils.DateTimeConvertUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientStatsInfo {
    private static final Logger logger =
            LoggerFactory.getLogger(ClientStatsInfo.class);
    private final boolean isProducer;
    private final String logPrefix;
    // switchable statistic items
    private final ClientStatsItemSet[] switchableSets = new ClientStatsItemSet[2];
    // current writable index
    private final AtomicInteger writableIndex = new AtomicInteger(0);
    // statistics self-print period, ms
    private final long statsPrintPeriodMs;
    // statistics force reset period, ms
    private final long statsForcedResetPeriodMs;
    // whether the statistic is closed
    private volatile boolean isClosed = false;
    // whether to self-print statistics
    private volatile boolean isSelfPrint = true;
    // last self-print time
    private final AtomicLong lstSelfPrintTime = new AtomicLong(0);
    // last snapshot time
    private final AtomicLong lstSnapshotTime = new AtomicLong(0);

    public ClientStatsInfo(boolean isProducer, String clientId,
                           boolean isSelfPrint, long statsPrintPeriodMs,
                           long statsForcedResetPeriodMs) {
        this.isProducer = isProducer;
        this.isSelfPrint = isSelfPrint;
        this.statsPrintPeriodMs = statsPrintPeriodMs;
        this.statsForcedResetPeriodMs = statsForcedResetPeriodMs;
        StringBuilder strBuff = new StringBuilder(512);
        if (isProducer) {
            strBuff.append("[Producer");
        } else {
            strBuff.append("[Consumer");
        }
        this.logPrefix = strBuff.append(" Stats]: ")
                .append("Client=").append(clientId).toString();
        this.switchableSets[0] = new ClientStatsItemSet();
        this.switchableSets[1] = new ClientStatsItemSet();
    }

    public synchronized void setStatsStatus(boolean enableStats) {
        this.isClosed = !enableStats;
    }

    public boolean isStatsClosed() {
        return this.isClosed;
    }

    public boolean isStatsSelfPrint() {
        return this.isSelfPrint;
    }

    public void bookReg2Master(boolean isFailure) {
        if (isClosed) {
            return;
        }
        switchableSets[getIndex()].regMasterCnt.incValue();
        if (isFailure) {
            switchableSets[getIndex()].regMasterFailCnt.incValue();
        }
    }

    public void bookHB2MasterTimeout() {
        if (isClosed) {
            return;
        }
        switchableSets[getIndex()].regMasterTimoutCnt.incValue();
    }

    public void bookHB2MasterException() {
        if (isClosed) {
            return;
        }
        switchableSets[getIndex()].hbMasterExcCnt.incValue();
    }

    public void bookReg2Broker(boolean isFailure) {
        if (isClosed) {
            return;
        }
        switchableSets[getIndex()].regBrokerCnt.incValue();
        if (isFailure) {
            switchableSets[getIndex()].regBrokerFailCnt.incValue();
        }
    }

    public void bookHB2BrokerTimeout() {
        if (isClosed) {
            return;
        }
        switchableSets[getIndex()].regBrokerTimoutCnt.incValue();
    }

    public void bookHB2BrokerException() {
        if (isClosed) {
            return;
        }
        switchableSets[getIndex()].hbBrokerExcCnt.incValue();
    }

    public void bookConfirmDuration(long dltTime) {
        if (isClosed) {
            return;
        }
        switchableSets[getIndex()].csmLatencyStats.update(dltTime);
    }

    public void bookSuccSendMsg(long dltTime, String topicName, int msgSize) {
        if (isClosed) {
            return;
        }
        sendOrRecvMsg(topicName, dltTime, 1, msgSize);
        bookFailRpcCall(TErrCodeConstants.SUCCESS);
    }

    public void bookSuccGetMsg(long dltTime, String topicName, int msgCnt, int msgSize) {
        if (isClosed) {
            return;
        }
        sendOrRecvMsg(topicName, dltTime, msgCnt, msgSize);
        bookFailRpcCall(TErrCodeConstants.SUCCESS);
    }

    public void bookFailRpcCall(int errCode) {
        if (isClosed) {
            return;
        }
        // accumulate msg count by errcode
        ClientStatsItemSet curItemSet = switchableSets[getIndex()];
        LongStatsCounter curItemCounter = curItemSet.errRspStatsMap.get(errCode);
        if (curItemCounter == null) {
            LongStatsCounter tmpCounter = new LongStatsCounter(
                    "err_" + errCode, "");
            curItemCounter = curItemSet.errRspStatsMap.putIfAbsent(errCode, tmpCounter);
            if (curItemCounter == null) {
                curItemCounter = tmpCounter;
            }
        }
        curItemCounter.incValue();
    }

    /**
     * Self print statistics information to log file
     *
     * @param forcePrint    whether force print statistics information
     * @param strBuff       string buffer
     */
    public void selfPrintStatsInfo(boolean forcePrint, StringBuilder strBuff) {
        if (this.isClosed || !this.isSelfPrint) {
            return;
        }
        long lstPrintTime = lstSelfPrintTime.get();
        long curChkTime = Clock.systemDefaultZone().millis();
        if (forcePrint || (curChkTime - lstPrintTime > this.statsPrintPeriodMs)) {
            if (lstSelfPrintTime.compareAndSet(lstPrintTime, curChkTime)) {
                if (switchWritingStatsUnit(false)) {
                    strBuff.append(this.logPrefix).append(", reset value=");
                    getStatsInfo(switchableSets[getIndex(writableIndex.get() - 1)],
                            strBuff, StatsOutputLevel.FULL, false);
                } else {
                    strBuff.append(this.logPrefix).append(", value=");
                    getStatsInfo(switchableSets[getIndex()],
                            strBuff, StatsOutputLevel.FULL, false);
                }
                logger.info(strBuff.toString());
                strBuff.delete(0, strBuff.length());
            }
        }
    }

    // private functions
    private boolean switchWritingStatsUnit(boolean needReset) {
        long lstResetTime = lstSnapshotTime.get();
        long checkDltTime = System.currentTimeMillis() - lstResetTime;
        if (((needReset && (checkDltTime > TBaseConstants.CFG_STATS_MIN_SNAPSHOT_PERIOD_MS))
                || (checkDltTime > this.statsForcedResetPeriodMs))
                && lstSnapshotTime.compareAndSet(lstResetTime, System.currentTimeMillis())) {
            switchableSets[getIndex(writableIndex.incrementAndGet())].resetSinceTime();
            return true;
        }
        return false;
    }

    /**
     * Get current data encapsulated by Json format
     *
     * @param strBuff      string buffer
     * @param outputLevel  the output level of statistics
     * @param resetValue   whether to reset the current data
     */
    private void getStatsInfo(ClientStatsItemSet statsSet, StringBuilder strBuff,
                              StatsOutputLevel outputLevel, boolean resetValue) {
        int totalCnt = 0;
        strBuff.append("{\"").append(statsSet.resetTime.getFullName())
                .append("\":\"").append(statsSet.resetTime.getStrSinceTime())
                .append("\",\"probe_time\":\"")
                .append(DateTimeConvertUtils.ms2yyyyMMddHHmmss(System.currentTimeMillis()))
                .append("\"");
        if (resetValue) {
            strBuff.append(",\"").append(statsSet.totalTrafficStats.msgCnt.getFullName())
                    .append("\":").append(statsSet.totalTrafficStats.msgCnt.getAndResetValue())
                    .append(",\"").append(statsSet.totalTrafficStats.msgSize.getFullName())
                    .append("\":").append(statsSet.totalTrafficStats.msgSize.getAndResetValue())
                    .append(",\"traffic_details\":{");
            for (Map.Entry<String, TrafficStatsUnit> entry
                    : statsSet.topicTrafficMap.entrySet()) {
                if (entry == null) {
                    continue;
                }
                if (totalCnt++ > 0) {
                    strBuff.append(",");
                }
                strBuff.append("\"").append(entry.getKey()).append("\":{\"")
                        .append(entry.getValue().msgCnt.getShortName()).append("\":")
                         .append(entry.getValue().msgCnt.getAndResetValue()).append(",\"")
                        .append(entry.getValue().msgSize.getShortName()).append("\":")
                        .append(entry.getValue().msgCnt.getAndResetValue()).append("}");
            }
            strBuff.append("}");
            if (outputLevel != StatsOutputLevel.SIMPLEST) {
                strBuff.append(",");
                statsSet.msgCallDltStats.snapShort(strBuff, false);
                if (!isProducer) {
                    strBuff.append(",");
                    statsSet.csmLatencyStats.snapShort(strBuff, false);
                }
                strBuff.append(",\"rsp_details\":{");
                totalCnt = 0;
                for (LongStatsCounter statsCounter : statsSet.errRspStatsMap.values()) {
                    if (statsCounter == null) {
                        continue;
                    }
                    if (totalCnt++ > 0) {
                        strBuff.append(",");
                    }
                    strBuff.append("\"").append(statsCounter.getFullName()).append("\":")
                            .append(statsCounter.getAndResetValue());
                }
                strBuff.append("}");
            }
            if (outputLevel == StatsOutputLevel.FULL) {
                strBuff.append(",\"").append(statsSet.regMasterCnt.getFullName())
                        .append("\":").append(statsSet.regMasterCnt.getAndResetValue())
                        .append(",\"").append(statsSet.regMasterFailCnt.getFullName())
                        .append("\":").append(statsSet.regMasterFailCnt.getAndResetValue())
                        .append(",\"").append(statsSet.regMasterTimoutCnt.getFullName())
                        .append("\":").append(statsSet.regMasterTimoutCnt.getAndResetValue())
                        .append(",\"").append(statsSet.hbMasterExcCnt.getFullName())
                        .append("\":").append(statsSet.hbMasterExcCnt.getAndResetValue())
                        .append(",\"").append(statsSet.regBrokerCnt.getFullName())
                        .append("\":").append(statsSet.regBrokerCnt.getAndResetValue())
                        .append(",\"").append(statsSet.regBrokerFailCnt.getFullName())
                        .append("\":").append(statsSet.regBrokerFailCnt.getAndResetValue())
                        .append(",\"").append(statsSet.regBrokerTimoutCnt.getFullName())
                        .append("\":").append(statsSet.regBrokerTimoutCnt.getAndResetValue())
                        .append(",\"").append(statsSet.hbBrokerExcCnt.getFullName())
                        .append("\":").append(statsSet.hbBrokerExcCnt.getAndResetValue());
            }
            strBuff.append("}");
        } else {
            strBuff.append(",\"").append(statsSet.totalTrafficStats.msgCnt.getFullName())
                    .append("\":").append(statsSet.totalTrafficStats.msgCnt.getValue())
                    .append(",\"").append(statsSet.totalTrafficStats.msgSize.getFullName())
                    .append("\":").append(statsSet.totalTrafficStats.msgSize.getValue())
                    .append(",\"traffic_details\":{");
            for (Map.Entry<String, TrafficStatsUnit> entry
                    : statsSet.topicTrafficMap.entrySet()) {
                if (entry == null) {
                    continue;
                }
                if (totalCnt++ > 0) {
                    strBuff.append(",");
                }
                strBuff.append("\"").append(entry.getKey()).append("\":{\"")
                        .append(entry.getValue().msgCnt.getShortName()).append("\":")
                        .append(entry.getValue().msgCnt.getValue()).append(",\"")
                        .append(entry.getValue().msgSize.getShortName()).append("\":")
                        .append(entry.getValue().msgCnt.getValue()).append("}");
            }
            strBuff.append("}");
            if (outputLevel != StatsOutputLevel.SIMPLEST) {
                strBuff.append(",");
                statsSet.msgCallDltStats.getValue(strBuff, false);
                if (!isProducer) {
                    strBuff.append(",");
                    statsSet.csmLatencyStats.getValue(strBuff, false);
                }
                strBuff.append(",\"rsp_details\":{");
                totalCnt = 0;
                for (LongStatsCounter statsCounter : statsSet.errRspStatsMap.values()) {
                    if (statsCounter == null) {
                        continue;
                    }
                    if (totalCnt++ > 0) {
                        strBuff.append(",");
                    }
                    strBuff.append("\"").append(statsCounter.getFullName()).append("\":")
                            .append(statsCounter.getValue());
                }
                strBuff.append("}");
            }
            if (outputLevel == StatsOutputLevel.FULL) {
                strBuff.append(",\"").append(statsSet.regMasterCnt.getFullName())
                        .append("\":").append(statsSet.regMasterCnt.getValue())
                        .append(",\"").append(statsSet.regMasterFailCnt.getFullName())
                        .append("\":").append(statsSet.regMasterFailCnt.getValue())
                        .append(",\"").append(statsSet.regMasterTimoutCnt.getFullName())
                        .append("\":").append(statsSet.regMasterTimoutCnt.getValue())
                        .append(",\"").append(statsSet.hbMasterExcCnt.getFullName())
                        .append("\":").append(statsSet.hbMasterExcCnt.getValue())
                        .append(",\"").append(statsSet.regBrokerCnt.getFullName())
                        .append("\":").append(statsSet.regBrokerCnt.getValue())
                        .append(",\"").append(statsSet.regBrokerFailCnt.getFullName())
                        .append("\":").append(statsSet.regBrokerFailCnt.getValue())
                        .append(",\"").append(statsSet.regBrokerTimoutCnt.getFullName())
                        .append("\":").append(statsSet.regBrokerTimoutCnt.getValue())
                        .append(",\"").append(statsSet.hbBrokerExcCnt.getFullName())
                        .append("\":").append(statsSet.hbBrokerExcCnt.getValue());
            }
            strBuff.append("}");
        }
    }

    /**
     * Accumulate sent or received message information
     *
     * @param topic     the topic name
     * @param dltTime   the latency
     * @param msgCnt    the message count
     * @param msgSize   the message size
     */
    private void sendOrRecvMsg(String topic, long dltTime,
                               int msgCnt, int msgSize) {
        ClientStatsItemSet curItemSet = switchableSets[getIndex()];
        curItemSet.msgCallDltStats.update(dltTime);
        curItemSet.totalTrafficStats.addMsgCntAndSize(msgCnt, msgSize);
        // accumulate traffic information by topic
        TrafficStatsUnit curStatsUnit = curItemSet.topicTrafficMap.get(topic);
        if (curStatsUnit == null) {
            TrafficStatsUnit tmpUnit =
                    new TrafficStatsUnit("msg_cnt", "msg_size", topic);
            curStatsUnit = curItemSet.topicTrafficMap.putIfAbsent(topic, tmpUnit);
            if (curStatsUnit == null) {
                curStatsUnit = tmpUnit;
            }
        }
        curStatsUnit.addMsgCntAndSize(msgCnt, msgSize);
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
     * ClientStatsItemSet, Client related statistics set
     *
     */
    private static class ClientStatsItemSet {
        // The reset time of statistics set
        protected final SinceTime resetTime =
                new SinceTime("reset_time", null);
        // received or sent message traffic statistic
        protected final TrafficStatsUnit totalTrafficStats =
                new TrafficStatsUnit("msg_cnt", "msg_size", "total");
        // topic-based traffic statistics
        protected final ConcurrentHashMap<String, TrafficStatsUnit> topicTrafficMap =
                new ConcurrentHashMap<>();
        // time consumption statistics for sending or receiving messages
        protected final ESTHistogram msgCallDltStats =
                new ESTHistogram("msg_call_dlt", "");
        // statistics on consumption transaction time
        protected final ESTHistogram csmLatencyStats =
                new ESTHistogram("csm_latency_dlt", "");
        // error response distribution statistics
        protected final ConcurrentHashMap<Integer, LongStatsCounter> errRspStatsMap =
                new ConcurrentHashMap<>();
        // client 2 Master status statistics
        protected final LongStatsCounter regMasterCnt =
                new LongStatsCounter("reg_master_cnt", null);
        protected final LongStatsCounter regMasterFailCnt =
                new LongStatsCounter("reg_master_fail", null);
        protected final LongStatsCounter regMasterTimoutCnt =
                new LongStatsCounter("reg_master_timeout", null);
        protected final LongStatsCounter hbMasterExcCnt =
                new LongStatsCounter("hb_master_exception", null);
        // client 2 Broker status statistics
        protected final LongStatsCounter regBrokerCnt =
                new LongStatsCounter("reg_broker_cnt", null);
        protected final LongStatsCounter regBrokerFailCnt =
                new LongStatsCounter("reg_broker_fail", null);
        protected final LongStatsCounter regBrokerTimoutCnt =
                new LongStatsCounter("reg_broker_timeout", null);
        protected final LongStatsCounter hbBrokerExcCnt =
                new LongStatsCounter("hb_broker_exception", null);

        public ClientStatsItemSet() {
            resetSinceTime();
        }

        public void resetSinceTime() {
            this.resetTime.reset();
        }
    }
}

