/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.inlong.sort.stat;

import java.util.concurrent.atomic.AtomicLongArray;

public class SortClientStateCounter {

    private final AtomicLongArray count = new AtomicLongArray(20);
    public String sortId;
    public String clusterId;
    public String topic;
    public int partitionId;

    public SortClientStateCounter(String sortId, String clusterId, String topic, int partitionId) {
        this.sortId = sortId;
        this.clusterId = clusterId;
        this.topic = topic;
        this.partitionId = partitionId;
    }

    public SortClientStateCounter reset() {
        SortClientStateCounter counter = new SortClientStateCounter(sortId, clusterId, topic, partitionId);
        for (int i = 0, len = counter.count.length(); i < len; i++) {
            counter.count.set(i, this.count.getAndSet(i, 0));
        }
        return counter;
    }

    public double[] getStatvalue() {
        double[] vals = new double[this.count.length()];
        for (int i = 0, len = this.count.length(); i < len; i++) {
            vals[i] = this.count.get(i);
        }
        return vals;
    }

    public SortClientStateCounter addConsumeSize(long num) {
        count.getAndAdd(0, num);
        return this;
    }

    public SortClientStateCounter addCallbackTimes(int num) {
        count.getAndAdd(1, num);
        return this;
    }

    public SortClientStateCounter addCallbackTimeCost(long num) {
        count.getAndAdd(2, num);
        return this;
    }


    public SortClientStateCounter addTopicOnlineTimes(int num) {
        count.getAndAdd(3, num);
        return this;
    }

    public SortClientStateCounter addTopicOfflineTimes(int num) {
        count.getAndAdd(4, num);
        return this;
    }

    public SortClientStateCounter addRequestManagerTimes(int num) {
        count.getAndAdd(5, num);
        return this;
    }

    public SortClientStateCounter addRequestManagerTimeCost(long num) {
        count.getAndAdd(6, num);
        return this;
    }

    public SortClientStateCounter addRequestManagerFailTimes(long num) {
        count.getAndAdd(7, num);
        return this;
    }

    public SortClientStateCounter addCallbackErrorTimes(long num) {
        count.getAndAdd(8, num);
        return this;
    }

    public SortClientStateCounter addAckFailTimes(long num) {
        count.getAndAdd(9, num);
        return this;
    }

    public SortClientStateCounter addAckSuccTimes(long num) {
        count.getAndAdd(10, num);
        return this;
    }

    public SortClientStateCounter addCallbackDoneTimes(int num) {
        count.getAndAdd(11, num);
        return this;
    }

    public SortClientStateCounter addMsgCount(int num) {
        count.getAndAdd(12, num);
        return this;
    }

    public SortClientStateCounter addManagerConfChangedTimes(int num) {
        count.getAndAdd(13, num);
        return this;
    }

    public SortClientStateCounter addRequestManagerCommonErrorTimes(int num) {
        count.getAndAdd(14, num);
        return this;
    }

    public SortClientStateCounter addRequestManagerParamErrorTimes(int num) {
        count.getAndAdd(15, num);
        return this;
    }

}
