/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.tubemq.server.broker.nodeinfo;

import org.apache.inlong.tubemq.corebase.TBaseConstants;
import org.apache.inlong.tubemq.server.broker.msgstore.MessageStoreManager;


public class ReplicaNodeInfo {

    private final MessageStoreManager storeManager;
    // replicaNode id
    private String replicaNodeId;
    // replica node's address
    private String rmtAddrInfo;
    private String topicName;
    private int storeId;
    private String storeKey;
    private long createTime = System.currentTimeMillis();
    private long lastGetTime = 0L;
    private long lastDataRdOffset = TBaseConstants.META_VALUE_UNDEFINED;
    private int sentMsgSize = 0;

    public ReplicaNodeInfo(MessageStoreManager storeManager,
                           String replicaNodeId, String topicName, int storeId) {
        this.storeManager = storeManager;
        this.replicaNodeId = replicaNodeId;
        this.topicName = topicName;
        this.storeId = storeId;
    }

    public long getLastGetTime() {
        return lastGetTime;
    }

    public void setLastGetTime(long lastGetTime) {
        this.lastGetTime = lastGetTime;
    }

    public long getLastDataRdOffset() {
        return lastDataRdOffset;
    }

    public void setLastDataRdOffset(long lastDataRdOffset) {
        this.lastDataRdOffset = lastDataRdOffset;
    }
}
