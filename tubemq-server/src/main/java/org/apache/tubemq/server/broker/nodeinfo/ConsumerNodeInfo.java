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
import org.apache.tubemq.corebase.policies.FlowCtrlResult;
import org.apache.tubemq.corebase.policies.FlowCtrlRuleHandler;
import org.apache.tubemq.server.broker.msgstore.MessageStoreManager;
import org.apache.tubemq.server.common.TServerConstants;

/***
 * Consumer node info, which broker contains.
 */
public class ConsumerNodeInfo {

    // partition string format
    private final String partStr;
    private final MessageStoreManager storeManager;
    // consumer id
    private String consumerId;
    private String sessionKey;
    private long sessionTime;
    // is filter consumer or not
    private boolean isFilterConsume = false;
    // filter conditions in string format
    private Set<String> filterCondStrs = new HashSet<>(10);
    // filter conditions in int format
    private Set<Integer> filterCondCode = new HashSet<>(10);
    // consumer's address
    private String rmtAddrInfo;
    private boolean isSupportLimit = false;
    private long nextStatTime = 0L;
    private long lastGetTime = 0L;
    private long lastDataRdOffset = TBaseConstants.META_VALUE_UNDEFINED;
    private int sentMsgSize = 0;
    private int sentUnit = TServerConstants.CFG_STORE_DEFAULT_MSG_READ_UNIT;
    private long totalUnitSec = 0L;
    private long totalUnitMin = 0L;
    private FlowCtrlResult curFlowCtrlVal =
            new FlowCtrlResult(Long.MAX_VALUE, 0);
    private long nextLimitUpdateTime = 0;
    private AtomicInteger qryPriorityId =
            new AtomicInteger(TBaseConstants.META_VALUE_UNDEFINED);
    private long createTime = System.currentTimeMillis();


    public ConsumerNodeInfo(final MessageStoreManager storeManager,
                            final String consumerId, Set<String> filterCodes,
                            final String sessionKey, long sessionTime, final String partStr) {
        this(storeManager, TBaseConstants.META_VALUE_UNDEFINED, consumerId,
            filterCodes, sessionKey, sessionTime, false, partStr);
    }

    public ConsumerNodeInfo(final MessageStoreManager storeManager,
                            final int qryPriorityId, final String consumerId,
                            Set<String> filterCodes, final String sessionKey,
                            long sessionTime, boolean isSupportLimit,
                            final String partStr) {
        setConsumerId(consumerId);
        if (filterCodes != null) {
            for (String filterItem : filterCodes) {
                this.filterCondStrs.add(filterItem);
                this.filterCondCode.add(filterItem.hashCode());
            }
        }
        this.sessionKey = sessionKey;
        this.sessionTime = sessionTime;
        this.qryPriorityId.set(qryPriorityId);
        this.storeManager = storeManager;
        this.partStr = partStr;
        this.createTime = System.currentTimeMillis();
        if (filterCodes != null && !filterCodes.isEmpty()) {
            this.isFilterConsume = true;
        }
        this.isSupportLimit = isSupportLimit;
    }

    // #lizard forgives
    public int getCurrentAllowedSize(final String storeKey,
                                     final FlowCtrlRuleHandler flowCtrlRuleHandler,
                                     final long currMaxDataOffset, int maxMsgTransferSize,
                                     boolean isEscFlowCtrl) {
        if (lastDataRdOffset >= 0) {
            long curDataDlt = currMaxDataOffset - lastDataRdOffset;
            long currTime = System.currentTimeMillis();
            recalcMsgLimitValue(curDataDlt,
                    currTime, maxMsgTransferSize, flowCtrlRuleHandler);
            if (isEscFlowCtrl
                    || (totalUnitSec > sentMsgSize
                    && this.curFlowCtrlVal.dataLtInSize > totalUnitMin)) {
                return this.sentUnit;
            } else {
                if (this.isSupportLimit) {
                    return -this.curFlowCtrlVal.freqLtInMs;
                } else {
                    return 0;
                }
            }
        } else {
            return this.sentUnit;
        }
    }

    public String getPartStr() {
        return partStr;
    }

    public int getSentMsgSize() {
        return sentMsgSize;
    }

    public boolean isSupportLimit() {
        return isSupportLimit;
    }

    public int getQryPriorityId() {
        return qryPriorityId.get();
    }

    public void setQryPriorityId(int qryPriorityId) {
        this.qryPriorityId.set(qryPriorityId);
    }

    public long getNextStatTime() {
        return nextStatTime;
    }

    public long getLastDataRdOffset() {
        return lastDataRdOffset;
    }

    public int getSentUnit() {
        return sentUnit;
    }

    public long getTotalUnitSec() {
        return totalUnitSec;
    }

    public long getTotalUnitMin() {
        return totalUnitMin;
    }

    public FlowCtrlResult getCurFlowCtrlVal() {
        return curFlowCtrlVal;
    }

    public long getNextLimitUpdateTime() {
        return nextLimitUpdateTime;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(String consumerId) {
        this.consumerId = consumerId;
        if (consumerId.lastIndexOf("_") != -1) {
            String targetStr = consumerId.substring(consumerId.lastIndexOf("_") + 1);
            String[] strInfos = targetStr.split("-");
            if (strInfos.length > 2) {
                this.rmtAddrInfo = new StringBuilder(256)
                        .append(strInfos[0]).append("#").append(strInfos[1]).toString();
            }
        }
    }

    public Set<Integer> getFilterCondCodeSet() {
        return this.filterCondCode;
    }

    public Set<String> getFilterCondStrs() {
        return filterCondStrs;
    }

    public long getCurFlowCtrlLimitSize() {
        return this.curFlowCtrlVal.dataLtInSize / 1024 / 1024;
    }

    public int getCurFlowCtrlFreqLimit() {
        return this.curFlowCtrlVal.freqLtInMs;
    }

    public boolean isFilterConsume() {
        return isFilterConsume;
    }

    public long getLastGetTime() {
        return lastGetTime;
    }

    public String getSessionKey() {
        return sessionKey;
    }

    public long getSessionTime() {
        return sessionTime;
    }

    public void setLastProcInfo(long lastGetTime, long lastRdDataOffset, int totalMsgSize) {
        this.lastGetTime = lastGetTime;
        this.lastDataRdOffset = lastRdDataOffset;
        this.sentMsgSize += totalMsgSize;
        this.totalUnitMin += totalMsgSize;

    }

    public String getRmtAddrInfo() {
        return this.rmtAddrInfo;
    }

    /***
     * Recalculate message limit value.
     *
     * @param curDataDlt
     * @param currTime
     * @param maxMsgTransferSize
     * @param flowCtrlRuleHandler
     */
    private void recalcMsgLimitValue(long curDataDlt, long currTime, int maxMsgTransferSize,
                                     final FlowCtrlRuleHandler flowCtrlRuleHandler) {
        if (currTime > nextLimitUpdateTime) {
            this.curFlowCtrlVal = flowCtrlRuleHandler.getCurDataLimit(curDataDlt);
            if (this.curFlowCtrlVal == null) {
                this.curFlowCtrlVal = new FlowCtrlResult(Long.MAX_VALUE, 0);
            }
            currTime = System.currentTimeMillis();
            this.sentMsgSize = 0;
            this.totalUnitMin = 0;
            this.nextStatTime =
                    currTime + TBaseConstants.CFG_FC_MAX_SAMPLING_PERIOD;
            this.nextLimitUpdateTime =
                    currTime + TBaseConstants.CFG_FC_MAX_LIMITING_DURATION;
            this.totalUnitSec = this.curFlowCtrlVal.dataLtInSize / 12;
            this.sentUnit =
                    totalUnitSec > maxMsgTransferSize ? maxMsgTransferSize : (int) totalUnitSec;
        } else if (currTime > nextStatTime) {
            sentMsgSize = 0;
            nextStatTime =
                    currTime + TBaseConstants.CFG_FC_MAX_SAMPLING_PERIOD;
        }
    }

}
