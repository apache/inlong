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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class TimeWastInfo {

    private final String name;
    // bucketT
    private final ConcurrentHashMap<String, LongAdder> sendTimeBucketT = new ConcurrentHashMap<>();
    // avgT = sumTime/totalCnt
    private final LongAdder sumTime = new LongAdder();
    private final AtomicLong maxValue = new AtomicLong(Long.MIN_VALUE);
    private final AtomicLong minValue = new AtomicLong(Long.MAX_VALUE);
    private final LongAdder totalCnt = new LongAdder();

    public TimeWastInfo(String name) {
        this.name = name;
    }

    public void addTimeWastMs(long timeMs) {
        this.sumTime.add(timeMs);
        this.totalCnt.increment();
        this.updateMin(timeMs);
        this.updateMax(timeMs);
        if (timeMs < 2) {
            addTimeWastBucketT("0t2");
        } else if (timeMs < 4) {
            addTimeWastBucketT("2t4");
        } else if (timeMs < 8) {
            addTimeWastBucketT("4t8");
        } else if (timeMs < 16) {
            addTimeWastBucketT("8t16");
        } else if (timeMs < 32) {
            addTimeWastBucketT("16t32");
        } else if (timeMs < 96) {
            addTimeWastBucketT("32t96");
        } else if (timeMs < 128) {
            addTimeWastBucketT("96t128");
        } else if (timeMs < 256) {
            addTimeWastBucketT("128t256");
        } else if (timeMs < 512) {
            addTimeWastBucketT("256t512");
        } else if (timeMs < 1024) {
            addTimeWastBucketT("512t1024");
        } else if (timeMs < 20480) {
            addTimeWastBucketT("1024t20480");
        } else {
            addTimeWastBucketT("20480t+∞");
        }
    }

    public void getAndResetValue(StringBuilder strBuff) {
        long curCnt = totalCnt.sumThenReset();
        if (curCnt == 0) {
            strBuff.append("\"").append(name)
                    .append("\":{\"bucketT\":{},\"min\":0,\"max\":0,\"avgT\":0,\"cnt\":0}");
        } else {
            curCnt = 0;
            strBuff.append("\"").append(name).append("\":{\"bucketT\":{");
            for (Map.Entry<String, LongAdder> entry : sendTimeBucketT.entrySet()) {
                if (curCnt++ > 0) {
                    strBuff.append(",");
                }
                strBuff.append("\"").append(entry.getKey()).append("\":").append(entry.getValue());
            }
            strBuff.append("},\"min\":").append(this.minValue.getAndSet(Long.MAX_VALUE))
                    .append(",\"max\":").append(this.maxValue.getAndSet(Long.MIN_VALUE))
                    .append(",\"avgT\":").append(sumTime.sumThenReset() / curCnt).append("}");
            sendTimeBucketT.clear();
        }
    }

    public void clear() {
        this.sendTimeBucketT.clear();
        this.totalCnt.reset();
        this.sumTime.reset();
        this.minValue.set(Long.MAX_VALUE);
        this.maxValue.set(Long.MIN_VALUE);
    }

    private void updateMin(long newValue) {
        while (true) {
            long cur = this.minValue.get();
            if (newValue >= cur) {
                break;
            }
            if (this.minValue.compareAndSet(cur, newValue)) {
                break;
            }
        }
    }

    private void updateMax(long newValue) {
        while (true) {
            long cur = this.maxValue.get();
            if (newValue <= cur) {
                break;
            }
            if (this.maxValue.compareAndSet(cur, newValue)) {
                break;
            }
        }
    }

    private void addTimeWastBucketT(String key) {
        LongAdder longCount = this.sendTimeBucketT.get(key);
        if (longCount == null) {
            LongAdder tmpCount = new LongAdder();
            longCount = this.sendTimeBucketT.putIfAbsent(key, tmpCount);
            if (longCount == null) {
                longCount = tmpCount;
            }
        }
        longCount.increment();
    }
}
