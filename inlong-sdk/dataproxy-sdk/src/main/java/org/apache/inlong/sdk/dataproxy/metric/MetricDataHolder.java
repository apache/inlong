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

package org.apache.inlong.sdk.dataproxy.metric;

import org.apache.inlong.sdk.dataproxy.common.ProcessResult;
import org.apache.inlong.sdk.dataproxy.sender.BaseSender;
import org.apache.inlong.sdk.dataproxy.sender.http.HttpMsgSenderConfig;
import org.apache.inlong.sdk.dataproxy.sender.tcp.TcpMsgSender;
import org.apache.inlong.sdk.dataproxy.sender.tcp.TcpMsgSenderConfig;
import org.apache.inlong.sdk.dataproxy.utils.LogCounter;
import org.apache.inlong.sdk.dataproxy.utils.ProxyUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class MetricDataHolder implements Runnable {

    private static final String DEFAULT_KEY_SPLITTER = "#";
    private static final Logger logger = LoggerFactory.getLogger(MetricDataHolder.class);
    private static final LogCounter exceptCnt = new LogCounter(5, 100000, 60 * 1000L);

    private final MetricConfig metricConfig;
    private final BaseSender sender;
    private volatile boolean started = true;
    // Current writable index
    private volatile int itemIndex;
    // metric data items
    private final MetricInfoUnit[] metricUnits = new MetricInfoUnit[2];
    // Last snapshot time
    private volatile long lstReportTime;
    private final ScheduledExecutorService outputExecutor =
            Executors.newScheduledThreadPool(1);

    public MetricDataHolder(BaseSender sender) {
        this.sender = sender;
        this.metricConfig = sender.getConfigure().getMetricConfig();
        this.itemIndex = 0;
        this.metricUnits[0] = new MetricInfoUnit();
        this.metricUnits[1] = new MetricInfoUnit();
    }

    public boolean start(ProcessResult procResult) {
        this.outputExecutor.scheduleWithFixedDelay(this,
                this.metricConfig.getMetricOutIntvlMs(),
                this.metricConfig.getMetricOutIntvlMs(), TimeUnit.MILLISECONDS);
        logger.info("Metric DataHolder({}) started!", this.sender.getSenderId());
        return procResult.setSuccess();
    }

    @Override
    public void run() {
        long startTime = System.currentTimeMillis();
        outputMetricData(false, startTime, getAndIncIndex());
        long dltTime = System.currentTimeMillis() - startTime;
        if (dltTime > this.metricConfig.getMetricOutWarnIntMs()) {
            logger.warn("Metric DataHolder({}) snapshot finished, cost = {} ms!",
                    this.sender.getSenderId(), dltTime);
        }
        this.lstReportTime = startTime;
    }

    public void close() {
        logger.info("Metric DataHolder({}) closing ......", this.sender.getSenderId());
        // process rest data
        long startTime = System.currentTimeMillis();
        this.started = false;
        this.outputExecutor.shutdown();
        outputMetricData(true, startTime, getOldIndex());
        outputMetricData(true, startTime, getCurIndex());
        logger.info("Metric DataHolder({}) closed, cost = {} ms!",
                this.sender.getSenderId(), System.currentTimeMillis() - startTime);
    }

    public void addMetaSyncMetric(int errCode, long syncCostMs) {
        if (!this.metricConfig.isEnableMetric()) {
            return;
        }
        MetricInfoUnit selectedUnit = metricUnits[itemIndex];
        selectedUnit.refCnt.incrementAndGet();
        try {
            selectedUnit.metaSyncInfo.addSucMsgInfo(errCode, syncCostMs);
        } finally {
            selectedUnit.refCnt.decrementAndGet();
        }
    }

    public void addSyncSucMetric(String groupId, String streamId, int msgCnt, long costMs) {
        if (!this.metricConfig.isEnableMetric()) {
            return;
        }
        MetricInfoUnit selectedUnit = metricUnits[itemIndex];
        selectedUnit.refCnt.incrementAndGet();
        try {
            selectedUnit.addSyncSendSucInfo(groupId,
                    (this.metricConfig.isMaskStreamId() ? "" : streamId), msgCnt, costMs);
        } finally {
            selectedUnit.refCnt.decrementAndGet();
        }
    }

    public void addSyncFailMetric(int errCode, String groupId, String streamId, int msgCnt) {
        if (!this.metricConfig.isEnableMetric()) {
            return;
        }
        MetricInfoUnit selectedUnit = metricUnits[itemIndex];
        selectedUnit.refCnt.incrementAndGet();
        try {
            selectedUnit.addSyncSendFailInfo(groupId,
                    (this.metricConfig.isMaskStreamId() ? "" : streamId), msgCnt, errCode);
        } finally {
            selectedUnit.refCnt.decrementAndGet();
        }
    }

    public void addAsyncSucReqMetric(String groupId, String streamId, int msgCnt) {
        if (!this.metricConfig.isEnableMetric()) {
            return;
        }
        MetricInfoUnit selectedUnit = metricUnits[itemIndex];
        selectedUnit.refCnt.incrementAndGet();
        try {
            selectedUnit.addAsyncSendSucInfo(groupId,
                    (this.metricConfig.isMaskStreamId() ? "" : streamId), msgCnt);
        } finally {
            selectedUnit.refCnt.decrementAndGet();
        }
    }

    public void addAsyncFailReqMetric(int errCode, String groupId, String streamId, int msgCnt) {
        if (!this.metricConfig.isEnableMetric()) {
            return;
        }
        MetricInfoUnit selectedUnit = metricUnits[itemIndex];
        selectedUnit.refCnt.incrementAndGet();
        try {
            selectedUnit.addAsyncSendFailInfo(groupId,
                    (this.metricConfig.isMaskStreamId() ? "" : streamId), msgCnt, errCode);
        } finally {
            selectedUnit.refCnt.decrementAndGet();
        }
    }

    public void addCallbackSucMetric(String groupId, String streamId, int msgCnt, long costMs, long callDurMs) {
        if (!this.metricConfig.isEnableMetric()) {
            return;
        }
        MetricInfoUnit selectedUnit = metricUnits[itemIndex];
        selectedUnit.refCnt.incrementAndGet();
        try {
            selectedUnit.addAsyncRspSucInfo(groupId,
                    (this.metricConfig.isMaskStreamId() ? "" : streamId), msgCnt, costMs, callDurMs);
        } finally {
            selectedUnit.refCnt.decrementAndGet();
        }
    }

    public void addCallbackFailMetric(int errCode, String groupId, String streamId, int msgCnt, long costMs) {
        if (!this.metricConfig.isEnableMetric()) {
            return;
        }
        MetricInfoUnit selectedUnit = metricUnits[itemIndex];
        selectedUnit.refCnt.incrementAndGet();
        try {
            selectedUnit.addAsyncRspFailInfo(groupId,
                    (this.metricConfig.isMaskStreamId() ? "" : streamId), msgCnt, errCode, costMs);
        } finally {
            selectedUnit.refCnt.decrementAndGet();
        }
    }

    public void addAsyncHttpSucPutMetric(String groupId, String streamId, int msgCnt) {
        if (!this.metricConfig.isEnableMetric()) {
            return;
        }
        MetricInfoUnit selectedUnit = metricUnits[itemIndex];
        selectedUnit.refCnt.incrementAndGet();
        try {
            selectedUnit.addAsyncHttpPutSucInfo(groupId,
                    (this.metricConfig.isMaskStreamId() ? "" : streamId), msgCnt);
        } finally {
            selectedUnit.refCnt.decrementAndGet();
        }
    }

    public void addAsyncHttpFailPutMetric(int errCode, String groupId, String streamId, int msgCnt) {
        if (!this.metricConfig.isEnableMetric()) {
            return;
        }
        MetricInfoUnit selectedUnit = metricUnits[itemIndex];
        selectedUnit.refCnt.incrementAndGet();
        try {
            selectedUnit.addAsyncHttpPutFailInfo(groupId,
                    (this.metricConfig.isMaskStreamId() ? "" : streamId), msgCnt, errCode);
        } finally {
            selectedUnit.refCnt.decrementAndGet();
        }
    }

    public void addAsyncHttpSucGetMetric(String groupId, String streamId, int msgCnt) {
        if (!this.metricConfig.isEnableMetric()) {
            return;
        }
        MetricInfoUnit selectedUnit = metricUnits[itemIndex];
        selectedUnit.refCnt.incrementAndGet();
        try {
            selectedUnit.addAsyncHttpGetSucInfo(groupId,
                    (this.metricConfig.isMaskStreamId() ? "" : streamId), msgCnt);
        } finally {
            selectedUnit.refCnt.decrementAndGet();
        }
    }

    private void outputMetricData(boolean forceOutput, long reportTime, int readIndex) {
        if (!this.metricConfig.isEnableMetric()) {
            return;
        }
        if (!forceOutput && !this.started) {
            return;
        }
        MetricInfoUnit selectedUnit = metricUnits[readIndex];
        if (selectedUnit == null) {
            return;
        }
        long startTime = System.currentTimeMillis();
        do {
            if ((!forceOutput && !this.started)
                    || (System.currentTimeMillis() - startTime >= 5000L)) {
                break;
            }
            ProxyUtils.sleepSomeTime(80);
        } while (selectedUnit.refCnt.get() > 0);
        if (!forceOutput && !this.started) {
            logger.info("Metric DataHolder({}) closed, stop output metric info",
                    sender.getSenderId());
            return;
        }
        StringBuilder strBuff = new StringBuilder(512);
        String rptContent = buildMetricReportInfo(strBuff, reportTime, selectedUnit);
        logger.info("Metric DataHolder({}) output metricInfo={}",
                sender.getSenderId(), rptContent);
    }

    private int getCurIndex() {
        return itemIndex;
    }

    private int getOldIndex() {
        return itemIndex ^ 0x01;
    }

    private int getAndIncIndex() {
        int curIndex = itemIndex;
        this.itemIndex = curIndex ^ 0x01;
        return curIndex;
    }

    private static class MetricInfoUnit {

        protected final AtomicLong refCnt = new AtomicLong();
        protected final MetaSyncInfo metaSyncInfo = new MetaSyncInfo();
        protected final ConcurrentHashMap<String, TrafficInfo> trafficMap = new ConcurrentHashMap<>();
        protected final ConcurrentHashMap<Integer, LongAdder> errCodeMap = new ConcurrentHashMap<>();

        public void addSyncSendSucInfo(String groupId, String streamId, int msgCnt, long costMs) {
            String recordKey = getKeyStringByConfig(groupId, streamId);
            TrafficInfo trafficInfo = this.trafficMap.get(recordKey);
            if (trafficInfo == null) {
                TrafficInfo tmpInfo = new TrafficInfo(groupId, streamId);
                trafficInfo = this.trafficMap.putIfAbsent(recordKey, tmpInfo);
                if (trafficInfo == null) {
                    trafficInfo = tmpInfo;
                }
            }
            trafficInfo.addSyncSucMsgInfo(msgCnt, costMs);
        }

        public void addSyncSendFailInfo(String groupId, String streamId, int msgCnt, int errCode) {
            String recordKey = getKeyStringByConfig(groupId, streamId);
            TrafficInfo trafficInfo = this.trafficMap.get(recordKey);
            if (trafficInfo == null) {
                TrafficInfo tmpInfo = new TrafficInfo(groupId, streamId);
                trafficInfo = this.trafficMap.putIfAbsent(recordKey, tmpInfo);
                if (trafficInfo == null) {
                    trafficInfo = tmpInfo;
                }
            }
            trafficInfo.addSyncFailMsgInfo(msgCnt);
            addSendErrCodeInfo(errCode);
        }

        public void addAsyncSendSucInfo(String groupId, String streamId, int msgCnt) {
            String recordKey = getKeyStringByConfig(groupId, streamId);
            TrafficInfo trafficInfo = this.trafficMap.get(recordKey);
            if (trafficInfo == null) {
                TrafficInfo tmpInfo = new TrafficInfo(groupId, streamId);
                trafficInfo = this.trafficMap.putIfAbsent(recordKey, tmpInfo);
                if (trafficInfo == null) {
                    trafficInfo = tmpInfo;
                }
            }
            trafficInfo.addAsyncSucSendInfo(msgCnt);
        }

        public void addAsyncSendFailInfo(String groupId, String streamId, int msgCnt, int errCode) {
            String recordKey = getKeyStringByConfig(groupId, streamId);
            TrafficInfo trafficInfo = this.trafficMap.get(recordKey);
            if (trafficInfo == null) {
                TrafficInfo tmpInfo = new TrafficInfo(groupId, streamId);
                trafficInfo = this.trafficMap.putIfAbsent(recordKey, tmpInfo);
                if (trafficInfo == null) {
                    trafficInfo = tmpInfo;
                }
            }
            trafficInfo.addAsyncFailSendInfo(msgCnt);
            addSendErrCodeInfo(errCode);
        }

        public void addAsyncHttpPutSucInfo(String groupId, String streamId, int msgCnt) {
            String recordKey = getKeyStringByConfig(groupId, streamId);
            TrafficInfo trafficInfo = this.trafficMap.get(recordKey);
            if (trafficInfo == null) {
                TrafficInfo tmpInfo = new TrafficInfo(groupId, streamId);
                trafficInfo = this.trafficMap.putIfAbsent(recordKey, tmpInfo);
                if (trafficInfo == null) {
                    trafficInfo = tmpInfo;
                }
            }
            trafficInfo.addAsyncHttpSucPutInfo(msgCnt);
        }

        public void addAsyncHttpPutFailInfo(String groupId, String streamId, int msgCnt, int errCode) {
            String recordKey = getKeyStringByConfig(groupId, streamId);
            TrafficInfo trafficInfo = this.trafficMap.get(recordKey);
            if (trafficInfo == null) {
                TrafficInfo tmpInfo = new TrafficInfo(groupId, streamId);
                trafficInfo = this.trafficMap.putIfAbsent(recordKey, tmpInfo);
                if (trafficInfo == null) {
                    trafficInfo = tmpInfo;
                }
            }
            trafficInfo.addAsyncHttpFailPutInfo(msgCnt);
            addSendErrCodeInfo(errCode);
        }

        public void addAsyncHttpGetSucInfo(String groupId, String streamId, int msgCnt) {
            String recordKey = getKeyStringByConfig(groupId, streamId);
            TrafficInfo trafficInfo = this.trafficMap.get(recordKey);
            if (trafficInfo == null) {
                TrafficInfo tmpInfo = new TrafficInfo(groupId, streamId);
                trafficInfo = this.trafficMap.putIfAbsent(recordKey, tmpInfo);
                if (trafficInfo == null) {
                    trafficInfo = tmpInfo;
                }
            }
            trafficInfo.addAsyncHttpSucGetInfo(msgCnt);
        }

        public void addAsyncRspSucInfo(String groupId, String streamId, int msgCnt, long sdCostMs, long cbCostMs) {
            String recordKey = getKeyStringByConfig(groupId, streamId);
            TrafficInfo trafficInfo = this.trafficMap.get(recordKey);
            if (trafficInfo == null) {
                TrafficInfo tmpInfo = new TrafficInfo(groupId, streamId);
                trafficInfo = this.trafficMap.putIfAbsent(recordKey, tmpInfo);
                if (trafficInfo == null) {
                    trafficInfo = tmpInfo;
                }
            }
            trafficInfo.addAsyncSucRspInfo(msgCnt, sdCostMs, cbCostMs);
        }

        public void addAsyncRspFailInfo(String groupId, String streamId, int msgCnt, int errCode, long cbCostMs) {
            String recordKey = getKeyStringByConfig(groupId, streamId);
            TrafficInfo trafficInfo = this.trafficMap.get(recordKey);
            if (trafficInfo == null) {
                TrafficInfo tmpInfo = new TrafficInfo(groupId, streamId);
                trafficInfo = this.trafficMap.putIfAbsent(recordKey, tmpInfo);
                if (trafficInfo == null) {
                    trafficInfo = tmpInfo;
                }
            }
            trafficInfo.addAsyncFailRspInfo(msgCnt, cbCostMs);
            addSendErrCodeInfo(errCode);
        }

        private void addSendErrCodeInfo(int errCode) {
            LongAdder longCount = this.errCodeMap.get(errCode);
            if (longCount == null) {
                LongAdder tmpCount = new LongAdder();
                longCount = this.errCodeMap.putIfAbsent(errCode, tmpCount);
                if (longCount == null) {
                    longCount = tmpCount;
                }
            }
            longCount.increment();
        }

        public void getAndResetValue(StringBuilder strBuff) {
            int count = 0;
            metaSyncInfo.getAndResetValue(strBuff);
            strBuff.append(",\"tr\":[");
            for (Map.Entry<String, TrafficInfo> entry : trafficMap.entrySet()) {
                if (count++ > 0) {
                    strBuff.append(",");
                }
                entry.getValue().getAndResetValue(strBuff);
            }
            strBuff.append("],\"errs\":{");
            count = 0;
            for (Map.Entry<Integer, LongAdder> entry : errCodeMap.entrySet()) {
                if (count++ > 0) {
                    strBuff.append(",");
                }
                strBuff.append("\"e").append(entry.getKey())
                        .append("\":").append(entry.getValue().sumThenReset());
            }
            strBuff.append("},");
            metaSyncInfo.getAndResetValue(strBuff);
            trafficMap.clear();
            errCodeMap.clear();
        }

        private String getKeyStringByConfig(String groupId, String streamId) {
            return groupId + DEFAULT_KEY_SPLITTER + streamId + DEFAULT_KEY_SPLITTER;
        }
    }

    private String buildMetricReportInfo(StringBuilder strBuff, long curTimeMs, MetricInfoUnit metricUnit) {
        strBuff.append("{\"type\":\"JAVA\",\"pVer\":1.0,\"ver\":\"")
                .append(ProxyUtils.getJarVersion())
                .append("\",\"ip\":\"").append(ProxyUtils.getLocalIp())
                .append("\",\"pid\":").append(ProxyUtils.getProcessPid())
                .append(",\"sid\":\"").append(sender.getSenderId())
                .append("\",\"rT\":").append(curTimeMs)
                .append(",\"lrT\":").append(lstReportTime)
                .append(",");
        metricUnit.getAndResetValue(strBuff);
        strBuff.append(",\"s\":{\"tNs\":").append(sender.getProxyNodeCnt())
                .append(",\"aNs\":").append(sender.getActiveNodeCnt())
                .append(",\"ifRs\":").append(sender.getInflightMsgCnt())
                .append("},\"c\":{\"aC\":").append(sender.getConfigure().getAliveConnections())
                .append(",\"rP\":\"").append(sender.getConfigure().getDataRptProtocol())
                .append("\",\"rG\":\"").append(sender.getConfigure().getRegionName())
                .append("\"");
        if (sender instanceof TcpMsgSender) {
            TcpMsgSenderConfig tcpConfig = (TcpMsgSenderConfig) sender.getConfigure();
            strBuff.append(",\"mT\":").append(tcpConfig.getSdkMsgType().getValue())
                    .append(",\"cp\":").append(tcpConfig.isEnableDataCompress())
                    .append(",\"mCp\":").append(tcpConfig.getMinCompEnableLength())
                    .append(",\"lf\":").append(tcpConfig.isSeparateEventByLF())
                    .append(",\"nWk\":").append(tcpConfig.getNettyWorkerThreadNum())
                    .append(",\"sB\":").append(tcpConfig.getSendBufferSize())
                    .append(",\"rB\":").append(tcpConfig.getRcvBufferSize())
                    .append(",\"cOt\":").append(tcpConfig.getConnectTimeoutMs())
                    .append(",\"rOt\":").append(tcpConfig.getRequestTimeoutMs())
                    .append(",\"syOt\":").append(tcpConfig.getMaxAllowedSyncMsgTimeoutCnt());
        } else {
            HttpMsgSenderConfig httpConfig = (HttpMsgSenderConfig) sender.getConfigure();
            strBuff.append(",\"iHs\":").append(httpConfig.isRptDataByHttps())
                    .append(",\"sOt\":").append(httpConfig.getHttpSocketTimeoutMs())
                    .append(",\"cOt\":").append(httpConfig.getHttpConTimeoutMs())
                    .append(",\"aWk\":").append(httpConfig.getHttpAsyncRptWorkerNum())
                    .append(",\"aC\":").append(httpConfig.getHttpAsyncRptCacheSize());
        }
        String content = strBuff.append("}}").toString();
        strBuff.delete(0, strBuff.length());
        return content;
    }
}
