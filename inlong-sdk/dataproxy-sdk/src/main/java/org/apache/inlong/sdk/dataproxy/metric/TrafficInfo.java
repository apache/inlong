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

import org.apache.commons.lang3.StringUtils;

import java.util.concurrent.atomic.LongAdder;

public class TrafficInfo {

    // gId
    private final String groupId;
    // sId
    private final String streamId;

    // sPs
    private final LongAdder syncSendPkgCnt = new LongAdder();
    // sMs
    private final LongAdder syncSendMsgCount = new LongAdder();
    // sPf
    private final LongAdder syncFailedPgkCount = new LongAdder();
    // sucMs: sync call time cost in Ms
    private final TimeCostInfo syncSendCostMs = new TimeCostInfo("sSucMs");
    // http
    // apPs
    private final LongAdder aSyncHttpPkgPutCnt = new LongAdder();
    // apMs
    private final LongAdder aSyncHttpMsgPutCnt = new LongAdder();
    // apPf
    private final LongAdder aSyncHttpFailPkgPutCnt = new LongAdder();
    // agPs
    private final LongAdder aSyncHttpPkgGetCnt = new LongAdder();
    // agMs
    private final LongAdder aSyncHttpMsgGetCnt = new LongAdder();
    // aPs
    private final LongAdder aSyncSendPkgCount = new LongAdder();
    // aMs
    private final LongAdder aSyncSendMsgCount = new LongAdder();
    // aPf
    private final LongAdder aSyncFailedPgkCnt = new LongAdder();
    // arPs
    private final LongAdder aRcvPkgCount = new LongAdder();
    // arMs
    private final LongAdder aRcvMsgCount = new LongAdder();
    // arPf
    private final LongAdder aRcvFailedPgkCount = new LongAdder();
    // arMf
    private final LongAdder aRcvFailedMsgCount = new LongAdder();
    // sucMs: async received time cost in Ms
    private final TimeCostInfo asyncSucCostMs = new TimeCostInfo("aSucMs");
    // call back call count
    private final LongAdder cbCallCount = new LongAdder();
    // cbMs: call back time cost in Ms
    private final TimeCostInfo callbackCostMs = new TimeCostInfo("cbMs");

    public TrafficInfo(String groupId, String streamId) {
        this.groupId = groupId;
        this.streamId = streamId;
    }

    public void addSyncSucMsgInfo(int msgCnt, long costMs) {
        syncSendPkgCnt.increment();
        syncSendMsgCount.add(msgCnt);
        syncSendCostMs.addTimeCostInMs(costMs);
    }

    public void addSyncFailMsgInfo(int msgCnt) {
        syncFailedPgkCount.increment();
    }

    public void addAsyncSucSendInfo(int msgCnt) {
        aSyncSendPkgCount.increment();
        aSyncSendMsgCount.add(msgCnt);
    }

    public void addAsyncFailSendInfo(int msgCnt) {
        aSyncFailedPgkCnt.increment();
    }

    public void addAsyncHttpSucPutInfo(int msgCnt) {
        aSyncHttpPkgPutCnt.increment();
        aSyncHttpMsgPutCnt.add(msgCnt);
    }

    public void addAsyncHttpFailPutInfo(int msgCnt) {
        aSyncHttpFailPkgPutCnt.increment();
    }

    public void addAsyncHttpSucGetInfo(int msgCnt) {
        aSyncHttpPkgGetCnt.increment();
        aSyncHttpMsgGetCnt.add(msgCnt);
    }

    public void addAsyncSucRspInfo(int msgCnt, long sdCostMs, long cbCostMs) {
        aRcvPkgCount.increment();
        aRcvMsgCount.add(msgCnt);
        asyncSucCostMs.addTimeCostInMs(sdCostMs);
        cbCallCount.increment();
        callbackCostMs.addTimeCostInMs(cbCostMs);
    }

    public void addAsyncFailRspInfo(int msgCnt, long cbCostMs) {
        aRcvFailedPgkCount.increment();
        aRcvFailedMsgCount.add(msgCnt);
        cbCallCount.increment();
        callbackCostMs.addTimeCostInMs(cbCostMs);
    }

    public void getAndResetValue(StringBuilder strBuff) {
        strBuff.append("{\"gId\":\"").append(this.groupId);
        if (StringUtils.isNotBlank(this.streamId)) {
            strBuff.append("\",\"sId\":\"").append(this.streamId);
        }
        strBuff.append("\",\"sPs\":").append(syncSendPkgCnt.sumThenReset())
                .append(",\"sPf\":").append(syncFailedPgkCount.sumThenReset())
                .append(",\"sMs\":").append(syncSendMsgCount.sumThenReset()).append(",");
        this.syncSendCostMs.getAndResetValue(strBuff);
        strBuff.append(",\"apPs\":").append(aSyncHttpPkgPutCnt.sumThenReset())
                .append(",\"apPf\":").append(aSyncHttpFailPkgPutCnt.sumThenReset())
                .append(",\"agPs\":").append(aSyncHttpPkgGetCnt.sumThenReset())
                .append(",\"aPs\":").append(aSyncSendPkgCount.sumThenReset())
                .append(",\"aPf\":").append(aSyncFailedPgkCnt.sumThenReset())
                .append(",\"arPs\":").append(aRcvPkgCount.sumThenReset())
                .append(",\"arPf\":").append(aRcvFailedPgkCount.sumThenReset())
                .append(",\"cbCt\":").append(cbCallCount.sumThenReset())
                .append(",\"apMs\":").append(aSyncHttpMsgPutCnt.sumThenReset())
                .append(",\"agMs\":").append(aSyncHttpMsgGetCnt.sumThenReset())
                .append(",\"aMs\":").append(aSyncSendMsgCount.sumThenReset())
                .append(",\"arMs\":").append(aRcvMsgCount.sumThenReset())
                .append(",\"arMf\":").append(aRcvFailedMsgCount.sumThenReset()).append(",");
        this.asyncSucCostMs.getAndResetValue(strBuff);
        strBuff.append(",");
        this.callbackCostMs.getAndResetValue(strBuff);
        strBuff.append("}");
    }

    public void clear() {
        this.syncSendPkgCnt.reset();
        this.syncSendMsgCount.reset();
        this.syncFailedPgkCount.reset();
        this.syncSendCostMs.clear();
        //
        this.aSyncSendPkgCount.reset();
        this.aSyncSendMsgCount.reset();
        this.aSyncFailedPgkCnt.reset();
        this.aSyncHttpPkgPutCnt.reset();
        this.aSyncHttpMsgPutCnt.reset();
        this.aSyncHttpFailPkgPutCnt.reset();
        this.aSyncHttpPkgGetCnt.reset();
        this.aSyncHttpMsgGetCnt.reset();
        //
        this.aRcvPkgCount.reset();
        this.aRcvMsgCount.reset();
        this.aRcvFailedPgkCount.reset();
        this.aRcvFailedMsgCount.reset();
        this.asyncSucCostMs.clear();
        this.cbCallCount.reset();
        this.callbackCostMs.clear();
    }
}
