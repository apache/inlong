/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.tubemq.client.consumer;

import com.tencent.tubemq.corebase.TBaseConstants;
import com.tencent.tubemq.corebase.TokenConstants;


public class ConsumeOffsetInfo {
    private String partitionKey;
    private long currOffset = TBaseConstants.META_VALUE_UNDEFINED;

    public ConsumeOffsetInfo(String partitionKey, Long currOffset) {
        this.partitionKey = partitionKey;
        if (currOffset != null) {
            this.currOffset = currOffset;
        }
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public long getCurrOffset() {
        return currOffset;
    }

    @Override
    public String toString() {
        return this.partitionKey + TokenConstants.SEGMENT_SEP + this.currOffset;
    }
}
