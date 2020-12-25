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

package org.apache.tubemq.server.broker.utils;

import org.apache.tubemq.corebase.TBaseConstants;



public class TopicPubStoreInfo {

    public String topicName = null;
    public int storeId = TBaseConstants.META_VALUE_UNDEFINED;
    public int partitionId = TBaseConstants.META_VALUE_UNDEFINED;
    public long indexStart = 0L;
    public long indexEnd = 0L;
    public long dataStart = 0L;
    public long dataEnd = 0L;

    public TopicPubStoreInfo(String topicName, int storeId, int partitionId,
                             long indexStart, long indexEnd, long dataStart, long dataEnd) {
        this.topicName = topicName;
        this.storeId = storeId;
        this.partitionId = partitionId;
        this.indexStart = indexStart;
        this.indexEnd = indexEnd;
        this.dataStart = dataStart;
        this.dataEnd = dataEnd;
    }

}
