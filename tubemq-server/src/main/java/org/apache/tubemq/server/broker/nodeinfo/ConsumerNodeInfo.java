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

package org.apache.tubemq.server.broker.nodeinfo;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.corebase.TokenConstants;
import org.apache.tubemq.server.broker.metadata.MetadataManager;

/***
 * Consumer node info, which broker contains.
 */
public class ConsumerNodeInfo {
    // partition string format
    private String partStr = "";
    // consumer id
    private String consumerId = "";
    private String groupName = "";
    private String topicName = "";
    private String offsetCacheKey;
    private String heartbeatNodeId;
    private String rmtAddrInfo;
    private boolean overTls = false;
    private int partitionId
            = TBaseConstants.META_VALUE_UNDEFINED;
    // is filter consumer or not
    private boolean isFilterConsume = false;
    // filter conditions in string format
    private Set<String> filterCondStrs
            = new HashSet<>(10);
    // filter conditions in int format
    private Set<Integer> filterCondCode
            = new HashSet<>(10);
    // consume priority
    private AtomicInteger qryPriorityId =
            new AtomicInteger(TBaseConstants.META_VALUE_UNDEFINED);
    // registered subscribe info
    private AssignInfo assignInfo = new AssignInfo();
    // flow controller
    private FlowLimitInfo flowLimitInfo;


    public ConsumerNodeInfo(final MetadataManager metaManager,
                            int maxXfeSize, Set<String> filterCodes) {
        if (filterCodes != null) {
            for (String filterItem : filterCodes) {
                this.filterCondStrs.add(filterItem);
                this.filterCondCode.add(filterItem.hashCode());
            }
            if (!filterCodes.isEmpty()) {
                this.isFilterConsume = true;
            }
        }
        this.qryPriorityId.set(TBaseConstants.META_VALUE_UNDEFINED);
        this.flowLimitInfo =
                new FlowLimitInfo(metaManager, false, maxXfeSize);
    }

    public ConsumerNodeInfo(String partStr, boolean isRegister,
                            String consumerId, String groupName, String topicName,
                            int partitionId, Set<String> filterCodes, boolean overTls) {
        this.partStr = partStr;
        this.consumerId = consumerId;
        this.groupName = groupName;
        this.topicName = topicName;
        this.partitionId = partitionId;
        this.overTls = overTls;
        buildSearchKeyInfo();
        if (isRegister && filterCodes != null) {
            for (String filterItem : filterCodes) {
                this.filterCondStrs.add(filterItem);
                this.filterCondCode.add(filterItem.hashCode());
            }
            if (!filterCodes.isEmpty()) {
                this.isFilterConsume = true;
            }
        }
    }

    public void setConsumeInfo(final MetadataManager metaManager, int maxXfeSize,
                               boolean spLimit, AssignInfo assignInfo, int qryPriorityId) {
        this.assignInfo = assignInfo;
        this.qryPriorityId.set(qryPriorityId);
        this.flowLimitInfo = new FlowLimitInfo(metaManager, spLimit, maxXfeSize);
    }

    public long getLeftOffset() {
        return assignInfo.getLeftOffset();
    }

    public String getPartStr() {
        return partStr;
    }

    public int getQryPriorityId() {
        return qryPriorityId.get();
    }

    public void setQryPriorityId(int qryPriorityId) {
        this.qryPriorityId.set(qryPriorityId);
    }

    public String getConsumerId() {
        return consumerId;
    }

    public String getGroupName() {
        return groupName;
    }

    public String getTopicName() {
        return topicName;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public boolean isOverTls() {
        return overTls;
    }

    public String getOffsetCacheKey() {
        return offsetCacheKey;
    }

    public String getHeartbeatNodeId() {
        return heartbeatNodeId;
    }

    public Set<Integer> getFilterCondCodeSet() {
        return this.filterCondCode;
    }

    public Set<String> getFilterCondStrs() {
        return filterCondStrs;
    }

    public String getRmtAddrInfo() {
        return this.rmtAddrInfo;
    }

    public boolean isFilterConsume() {
        return isFilterConsume;
    }

    public FlowLimitInfo getFlowLimitInfo() {
        return flowLimitInfo;
    }

    public void recordConsumeInfo(long lastDataOffset, int msgSize) {
        flowLimitInfo.recordConsumeInfo(lastDataOffset, msgSize);
    }

    public int getAllowedQuota(long maxDataOffset, boolean escFlowCtrl) {
        return this.flowLimitInfo.getAllowedQuota(maxDataOffset, escFlowCtrl);
    }

    public boolean isSpLimit() {
        return this.flowLimitInfo.isSpLimit();
    }

    public long getLastRecordTime() {
        return this.flowLimitInfo.getLastRecordTime();
    }

    public long getNextStatTime() {
        return this.flowLimitInfo.getNextSegUpdTime();
    }

    public long getNextLimitUpdateTime() {
        return this.flowLimitInfo.getNextStageUpdTime();
    }

    public long getLastDataRdOffset() {
        return this.flowLimitInfo.getLastDataOffset();
    }

    private void buildSearchKeyInfo() {
        StringBuilder sBuilder = new StringBuilder(512);
        this.offsetCacheKey = sBuilder.append(topicName)
                .append(TokenConstants.HYPHEN).append(partitionId).toString();
        sBuilder.delete(0, sBuilder.length());
        this.heartbeatNodeId = sBuilder.append(consumerId)
                .append(TokenConstants.SEGMENT_SEP).append(partStr).toString();
        sBuilder.delete(0, sBuilder.length());
        if (consumerId.lastIndexOf("_") != -1) {
            String targetStr = consumerId.substring(consumerId.lastIndexOf("_") + 1);
            String[] strInfos = targetStr.split("-");
            if (strInfos.length > 2) {
                this.rmtAddrInfo = sBuilder.append(strInfos[0])
                        .append("#").append(strInfos[1]).toString();
            }
        }
    }

}
