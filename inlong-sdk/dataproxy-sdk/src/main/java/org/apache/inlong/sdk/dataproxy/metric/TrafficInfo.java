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
    // sucMs
    private final TimeWastInfo sendWastMs = new TimeWastInfo("sucMs");
    // cbMs
    private final TimeWastInfo callbackWastMs = new TimeWastInfo("cbMs");

    public TrafficInfo(String groupId, String streamId) {
        this.groupId = groupId;
        this.streamId = streamId;
    }

    public void addSucMsgInfo(int msgCnt, long wastMs) {
        sendPkgCount.add(1);
        sendMsgCount.add(msgCnt);
        sendWastMs.addTimeWastMs(wastMs);
    }

    public void addSucMsgInfo(int msgCnt, long sdWastMs, long cbWastMs) {
        sendPkgCount.add(1);
        sendMsgCount.add(msgCnt);
        sendWastMs.addTimeWastMs(sdWastMs);
        callbackWastMs.addTimeWastMs(cbWastMs);
    }

    public void addFailMsgInfo(int msgCnt) {
        failedPgkCount.add(1);
        failedMsgCount.add(msgCnt);
    }

    public void addFailMsgInfo(int msgCnt, long cbWastMs) {
        failedPgkCount.add(1);
        failedMsgCount.add(msgCnt);
        callbackWastMs.addTimeWastMs(cbWastMs);
    }

    public void getAndResetValue(StringBuilder strBuff) {
        strBuff.append("{\"gId\":\"").append(this.groupId);
        if (StringUtils.isNotBlank(this.streamId)) {
            strBuff.append("\",\"sId\":\"").append(this.streamId);
        }
        strBuff.append("\",\"sPkg\":").append(sendPkgCount.sumThenReset())
                .append(",\"sMsg\":").append(sendMsgCount.sumThenReset())
                .append(",\"fMsg\":").append(failedPgkCount.sumThenReset())
                .append(",\"fMsg\":").append(failedMsgCount.sumThenReset())
                .append(",");
        this.sendWastMs.getAndResetValue(strBuff);
        strBuff.append(",");
        this.callbackWastMs.getAndResetValue(strBuff);
        strBuff.append("}");
    }

    public void clear() {
        this.sendPkgCount.reset();
        this.sendMsgCount.reset();
        this.failedPgkCount.reset();
        this.failedMsgCount.reset();
        this.sendWastMs.clear();
        this.callbackWastMs.clear();
    }
}
