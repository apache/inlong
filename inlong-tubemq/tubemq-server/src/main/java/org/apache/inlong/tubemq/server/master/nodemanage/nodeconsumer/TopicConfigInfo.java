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

package org.apache.inlong.tubemq.server.master.nodemanage.nodeconsumer;

import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.TopicDeployEntity;

public class TopicConfigInfo implements Comparable<TopicConfigInfo> {
    private final int brokerId;
    private final String topicName;
    private final int numStore;
    private final int numPart;
    private boolean acceptSub;

    public TopicConfigInfo(TopicDeployEntity topicDeployEntity) {
        this.brokerId = topicDeployEntity.getBrokerId();
        this.topicName = topicDeployEntity.getTopicName();
        this.numStore = topicDeployEntity.getNumTopicStores();
        this.numPart = topicDeployEntity.getNumPartitions();
        this.acceptSub = false;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public String getTopicName() {
        return topicName;
    }

    public int getNumStore() {
        return numStore;
    }

    public void setAcceptSub(boolean acceptSub) {
        this.acceptSub = acceptSub;
    }

    public boolean isAcceptSub() {
        return acceptSub;
    }

    public int getNumPart() {
        return numPart;
    }

    @Override
    public int compareTo(TopicConfigInfo o) {
        return Integer.compare(this.brokerId, o.brokerId);
    }
}
