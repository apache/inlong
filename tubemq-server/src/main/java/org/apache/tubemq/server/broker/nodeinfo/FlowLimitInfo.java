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

import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.corebase.policies.FlowCtrlResult;
import org.apache.tubemq.server.broker.metadata.MetadataManager;
import org.apache.tubemq.server.common.TServerConstants;

/***
 * Flow control restriction information,
 * consumption flow restriction processing
 * according to the definition of flow control strategy.
 */
public class FlowLimitInfo {
    private boolean spLimit = false;
    private final MetadataManager metaManager;
    private int maxXfeSize = 0;
    private long lastRecordTime = 0L;
    private long lastDataOffset = TBaseConstants.META_VALUE_UNDEFINED;
    private int curSegCumCnt = 0;
    private long curStageCumCnt = 0L;
    private long nextSegUpdTime = 0L;
    private long nextStageUpdTime = 0L;
    private int sliceLimit = TServerConstants.CFG_STORE_DEFAULT_MSG_READ_UNIT;
    private long segLimit = Long.MAX_VALUE;
    private FlowCtrlResult curFlowCtrlVal =
            new FlowCtrlResult(Long.MAX_VALUE, 0);


    public FlowLimitInfo(final MetadataManager metaManager,
                         boolean spLimit, int maxXfeSize) {
        this.metaManager = metaManager;
        this.spLimit = spLimit;
        this.maxXfeSize = maxXfeSize;
    }

    public void recordConsumeInfo(long lastDataOffset, int msgSize) {
        this.lastRecordTime = System.currentTimeMillis();
        this.lastDataOffset = lastDataOffset;
        this.curSegCumCnt += msgSize;
        this.curStageCumCnt += msgSize;
    }

    public int getAllowedQuota(long maxDataOffset, boolean escFlowCtrl) {
        // #lizard forgives
        if (lastDataOffset < 0) {
            return this.sliceLimit;
        }
        long currTime = System.currentTimeMillis();
        if (currTime > nextStageUpdTime) {
            // Recalculate message limit value
            this.curFlowCtrlVal =
                    metaManager.getFlowCtrlRuleHandler()
                            .getCurDataLimit(maxDataOffset - lastDataOffset);
            if (this.curFlowCtrlVal == null) {
                this.curFlowCtrlVal = new FlowCtrlResult(Long.MAX_VALUE, 0);
            }
            this.curSegCumCnt = 0;
            this.curStageCumCnt = 0;
            this.nextSegUpdTime = currTime + TBaseConstants.CFG_FC_MAX_SAMPLING_PERIOD;
            this.nextStageUpdTime = currTime + TBaseConstants.CFG_FC_MAX_LIMITING_DURATION;
            this.segLimit = this.curFlowCtrlVal.dataLtInSize / 12;
            this.sliceLimit = segLimit > maxXfeSize ? maxXfeSize : (int) segLimit;
        } else if (currTime > nextSegUpdTime) {
            curSegCumCnt = 0;
            nextSegUpdTime = currTime + TBaseConstants.CFG_FC_MAX_SAMPLING_PERIOD;
        }
        if (escFlowCtrl || (segLimit > curSegCumCnt
                && this.curFlowCtrlVal.dataLtInSize > curStageCumCnt)) {
            return this.sliceLimit;
        } else {
            if (this.spLimit) {
                return -this.curFlowCtrlVal.freqLtInMs;
            } else {
                return 0;
            }
        }
    }

    public boolean isSpLimit() {
        return spLimit;
    }

    public long getLastRecordTime() {
        return lastRecordTime;
    }

    public long getLastDataOffset() {
        return lastDataOffset;
    }

    public int getCurSegCumCnt() {
        return curSegCumCnt;
    }

    public long getCurStageCumCnt() {
        return curStageCumCnt;
    }

    public long getNextSegUpdTime() {
        return nextSegUpdTime;
    }

    public long getNextStageUpdTime() {
        return nextStageUpdTime;
    }

    public int getSliceLimit() {
        return sliceLimit;
    }

    public long getSegLimit() {
        return segLimit;
    }

    public long getPolicySizeLimit() {
        return curFlowCtrlVal.dataLtInSize / 1024 / 1024;
    }

    public int getPolicyFreqLimit() {
        return curFlowCtrlVal.freqLtInMs;
    }

}
