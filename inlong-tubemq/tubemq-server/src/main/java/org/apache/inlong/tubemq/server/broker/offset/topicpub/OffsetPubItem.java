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

package org.apache.inlong.tubemq.server.broker.offset.topicpub;

/**
 * The offset snapshot of the topic store.
 */
public class OffsetPubItem {

    private int storeId;
    // current number of partitions
    private int numPart = 0;
    // store min index offset
    private long indexMin = 0L;
    // store max index offset
    private long indexMax = 0L;
    // store min data offset
    private long dataMin = 0L;
    // store max data offset
    private long dataMax = 0L;

    public OffsetPubItem(int storeId, int numPart,
            long indexMinOffset, long indexMaxOffset,
            long dataMinOffset, long dataMaxOffset) {
        this.storeId = storeId;
        this.numPart = numPart;
        this.indexMin = indexMinOffset;
        this.indexMax = indexMaxOffset;
        this.dataMin = dataMinOffset;
        this.dataMax = dataMaxOffset;
    }

    public int getStoreId() {
        return storeId;
    }

    public int getNumPart() {
        return numPart;
    }

    public long getIndexMin() {
        return indexMin;
    }

    public long getIndexMax() {
        return indexMax;
    }

    public long getDataMin() {
        return dataMin;
    }

    public long getDataMax() {
        return dataMax;
    }
}
