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
    // sPkg
    private final LongAdder sendPkgCount = new LongAdder();
    // sMsg
    private final LongAdder sendMsgCount = new LongAdder();
    // fMsg
    private final LongAdder failedPgkCount = new LongAdder();
    // fMsg
    private final LongAdder failedMsgCount = new LongAdder();
    // sucMs: send success time cost in Ms
    private final TimeCostInfo sendCostMs = new TimeCostInfo("sucMs");
    // cbMs: call back time cost in Ms
    private final TimeCostInfo callbackCostMs = new TimeCostInfo("cbMs");

    public TrafficInfo(String groupId, String streamId) {
        this.groupId = groupId;
        this.streamId = streamId;
    }

    public void addSucMsgInfo(int msgCnt, long costMs) {
        sendPkgCount.add(1);
        sendMsgCount.add(msgCnt);
        sendCostMs.addTimeCostInMs(costMs);
    }

    public void addSucMsgInfo(int msgCnt, long sdCostMs, long cbCostMs) {
        sendPkgCount.add(1);
        sendMsgCount.add(msgCnt);
        sendCostMs.addTimeCostInMs(sdCostMs);
        callbackCostMs.addTimeCostInMs(cbCostMs);
    }

    public void addFailMsgInfo(int msgCnt) {
        failedPgkCount.add(1);
        failedMsgCount.add(msgCnt);
    }

    public void addFailMsgInfo(int msgCnt, long cbCostMs) {
        failedPgkCount.add(1);
        failedMsgCount.add(msgCnt);
        callbackCostMs.addTimeCostInMs(cbCostMs);
    }

    public void getAndResetValue(StringBuilder strBuff) {
        strBuff.append("{\"gId\":\"").append(this.groupId);
        if (StringUtils.isNotBlank(this.streamId)) {
            strBuff.append("\",\"sId\":\"").append(this.streamId);
        }
        strBuff.append("\",\"sPkg\":").append(sendPkgCount.sumThenReset())
                .append(",\"sMsg\":").append(sendMsgCount.sumThenReset())
                .append(",\"fPkg\":").append(failedPgkCount.sumThenReset())
                .append(",\"fMsg\":").append(failedMsgCount.sumThenReset())
                .append(",");
        this.sendCostMs.getAndResetValue(strBuff);
        strBuff.append(",");
        this.callbackCostMs.getAndResetValue(strBuff);
        strBuff.append("}");
    }

    public void clear() {
        this.sendPkgCount.reset();
        this.sendMsgCount.reset();
        this.failedPgkCount.reset();
        this.failedMsgCount.reset();
        this.sendCostMs.clear();
        this.callbackCostMs.clear();
    }
}
