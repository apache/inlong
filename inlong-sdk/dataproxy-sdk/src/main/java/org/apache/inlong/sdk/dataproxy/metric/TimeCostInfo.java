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

public class TimeCostInfo {

    private final String name;
    // bucketT
    private final ConcurrentHashMap<String, LongAdder> sendTimeBucketT = new ConcurrentHashMap<>();
    // avgT = sumTime/totalCnt
    private final LongAdder sumTime = new LongAdder();
    private final AtomicLong maxValue = new AtomicLong(Long.MIN_VALUE);
    private final AtomicLong minValue = new AtomicLong(Long.MAX_VALUE);
    private final LongAdder totalCnt = new LongAdder();

    public TimeCostInfo(String name) {
        this.name = name;
    }

    public void addTimeCostInMs(long timeInMs) {
        this.sumTime.add(timeInMs);
        this.totalCnt.increment();
        this.updateMin(timeInMs);
        this.updateMax(timeInMs);
        if (timeInMs < 2) {
            addTimeCostBucketT("0t2");
        } else if (timeInMs < 4) {
            addTimeCostBucketT("2t4");
        } else if (timeInMs < 8) {
            addTimeCostBucketT("4t8");
        } else if (timeInMs < 16) {
            addTimeCostBucketT("8t16");
        } else if (timeInMs < 32) {
            addTimeCostBucketT("16t32");
        } else if (timeInMs < 96) {
            addTimeCostBucketT("32t96");
        } else if (timeInMs < 128) {
            addTimeCostBucketT("96t128");
        } else if (timeInMs < 256) {
            addTimeCostBucketT("128t256");
        } else if (timeInMs < 512) {
            addTimeCostBucketT("256t512");
        } else if (timeInMs < 1024) {
            addTimeCostBucketT("512t1024");
        } else if (timeInMs < 20480) {
            addTimeCostBucketT("1024t20480");
        } else {
            addTimeCostBucketT("20480t+∞");
        }
    }

    public void getAndResetValue(StringBuilder strBuff) {
        long curTotalCnt = totalCnt.sumThenReset();
        if (curTotalCnt == 0) {
            strBuff.append("\"").append(name)
                    .append("\":{\"bucketT\":{},\"min\":0,\"max\":0,\"avgT\":0}");
        } else {
            long bucketCnt = 0;
            strBuff.append("\"").append(name).append("\":{\"bucketT\":{");
            for (Map.Entry<String, LongAdder> entry : sendTimeBucketT.entrySet()) {
                if (bucketCnt++ > 0) {
                    strBuff.append(",");
                }
                strBuff.append("\"").append(entry.getKey()).append("\":").append(entry.getValue());
            }
            strBuff.append("},\"min\":").append(this.minValue.getAndSet(Long.MAX_VALUE))
                    .append(",\"max\":").append(this.maxValue.getAndSet(Long.MIN_VALUE))
                    .append(",\"avgT\":").append(sumTime.sumThenReset() / curTotalCnt).append("}");
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

    private void addTimeCostBucketT(String key) {
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
