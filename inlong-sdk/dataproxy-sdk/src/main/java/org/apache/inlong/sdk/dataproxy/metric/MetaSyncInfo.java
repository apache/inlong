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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

public class MetaSyncInfo {

    // meta error info
    private final Map<Integer, LongAdder> syncErrInfo = new ConcurrentHashMap<>();
    // msMs: meta sync cost in Ms
    private final TimeCostInfo syncCostMs = new TimeCostInfo("msMs");

    public MetaSyncInfo() {
    }

    public void addSucMsgInfo(int errCode, long wastMs) {
        LongAdder longCount = this.syncErrInfo.get(errCode);
        if (longCount == null) {
            LongAdder tmpCount = new LongAdder();
            longCount = this.syncErrInfo.putIfAbsent(errCode, tmpCount);
            if (longCount == null) {
                longCount = tmpCount;
            }
        }
        longCount.increment();
        syncCostMs.addTimeCostInMs(wastMs);
    }

    public void getAndResetValue(StringBuilder strBuff) {
        if (syncErrInfo.isEmpty()) {
            strBuff.append("\"ms\":{\"errT\":{},");
            syncCostMs.getAndResetValue(strBuff);
            strBuff.append("}");
        } else {
            long curCnt = 0;
            strBuff.append("\"ms\":{\"errT\":{");
            for (Map.Entry<Integer, LongAdder> entry : syncErrInfo.entrySet()) {
                if (entry == null
                        || entry.getKey() == null
                        || entry.getValue() == null) {
                    continue;
                }
                if (curCnt++ > 0) {
                    strBuff.append(",");
                }
                strBuff.append("\"e").append(entry.getKey()).append("\":").append(entry.getValue());
            }
            strBuff.append("},");
            syncCostMs.getAndResetValue(strBuff);
            strBuff.append("}");
            syncErrInfo.clear();
        }
    }

    public void clear() {
        syncCostMs.clear();
        syncErrInfo.clear();
    }
}
