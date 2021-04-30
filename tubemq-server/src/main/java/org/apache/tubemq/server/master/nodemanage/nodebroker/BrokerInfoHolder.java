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

package org.apache.tubemq.server.master.nodemanage.nodebroker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.corebase.cluster.BrokerInfo;
import org.apache.tubemq.corebase.protobuf.generated.ClientMaster;
import org.apache.tubemq.server.common.statusdef.ManageStatus;
import org.apache.tubemq.server.common.utils.ProcessResult;
import org.apache.tubemq.server.master.metamanage.MetaDataManager;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.BaseEntity;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.BrokerConfEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BrokerInfoHolder {
    private static final Logger logger =
            LoggerFactory.getLogger(BrokerInfoHolder.class);
    private final ConcurrentHashMap<Integer/* brokerId */, BrokerInfo> brokerInfoMap =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer/* brokerId */, BrokerAbnInfo> brokerAbnormalMap =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer/* brokerId */, BrokerFbdInfo> brokerForbiddenMap =
            new ConcurrentHashMap<>();
    private final int maxAutoForbiddenCnt;
    private final MetaDataManager metaDataManager;
    private AtomicInteger brokerTotalCount = new AtomicInteger(0);
    private AtomicInteger brokerForbiddenCount = new AtomicInteger(0);


    public BrokerInfoHolder(final int maxAutoForbiddenCnt,
                            final MetaDataManager metaDataManager) {
        this.maxAutoForbiddenCnt = maxAutoForbiddenCnt;
        this.metaDataManager = metaDataManager;
    }

    public BrokerInfo getBrokerInfo(int brokerId) {
        return brokerInfoMap.get(brokerId);
    }

    public void setBrokerInfo(int brokerId, BrokerInfo brokerInfo) {
        if (brokerInfoMap.put(brokerId, brokerInfo) == null) {
            this.brokerTotalCount.incrementAndGet();
        }
    }

    public void updateBrokerReportStatus(int brokerId,
                                         int reportReadStatus,
                                         int reportWriteStatus) {
        StringBuilder sBuffer = new StringBuilder(512);
        if (reportReadStatus == 0 && reportWriteStatus == 0) {
            BrokerAbnInfo brokerAbnInfo = brokerAbnormalMap.get(brokerId);
            if (brokerAbnInfo != null) {
                if (brokerForbiddenMap.get(brokerId) == null) {
                    brokerAbnInfo = brokerAbnormalMap.remove(brokerId);
                    if (brokerAbnInfo != null) {
                        logger.warn(sBuffer.append("[Broker AutoForbidden] broker ")
                                .append(brokerId).append(" return to normal!").toString());
                        sBuffer.delete(0, sBuffer.length());
                    }
                }
            }
            return;
        }
        BrokerConfEntity curEntry =
                metaDataManager.getBrokerConfByBrokerId(brokerId);
        if (curEntry == null) {
            return;
        }
        ManageStatus reqMngStatus =
                getManageStatus(reportWriteStatus, reportReadStatus);
        if ((curEntry.getManageStatus() == reqMngStatus)
            || ((reqMngStatus == ManageStatus.STATUS_MANAGE_OFFLINE)
            && (curEntry.getManageStatus().getCode() < ManageStatus.STATUS_MANAGE_ONLINE.getCode()))) {
            return;
        }
        BrokerAbnInfo brokerAbnInfo = brokerAbnormalMap.get(brokerId);
        if (brokerAbnInfo == null) {
            if (brokerAbnormalMap.putIfAbsent(brokerId,
                new BrokerAbnInfo(brokerId, reportReadStatus, reportWriteStatus)) == null) {
                logger.warn(sBuffer.append("[Broker AutoForbidden] broker report abnormal, ")
                        .append(brokerId).append("'s reportReadStatus=")
                        .append(reportReadStatus).append(", reportWriteStatus=")
                        .append(reportWriteStatus).toString());
                sBuffer.delete(0, sBuffer.length());
            }
        } else {
            brokerAbnInfo.updateLastRepStatus(reportReadStatus, reportWriteStatus);
        }
        BrokerFbdInfo brokerFbdInfo = brokerForbiddenMap.get(brokerId);
        if (brokerFbdInfo == null) {
            BrokerFbdInfo tmpBrokerFbdInfo =
                new BrokerFbdInfo(brokerId, curEntry.getManageStatus(),
                        reqMngStatus, System.currentTimeMillis());
            if (reportReadStatus > 0 || reportWriteStatus > 0) {
                if (updateCurManageStatus(brokerId, reqMngStatus, sBuffer)) {
                    if (brokerForbiddenMap.putIfAbsent(brokerId, tmpBrokerFbdInfo) == null) {
                        brokerForbiddenCount.incrementAndGet();
                        logger.warn(sBuffer.
                                append("[Broker AutoForbidden] master add missing forbidden broker, ")
                                .append(brokerId).append("'s manage status to ")
                                .append(reqMngStatus.getDescription()).toString());
                        sBuffer.delete(0, sBuffer.length());
                    }
                }
            } else {
                if (brokerForbiddenCount.incrementAndGet() > maxAutoForbiddenCnt) {
                    brokerForbiddenCount.decrementAndGet();
                    return;
                }
                if (updateCurManageStatus(brokerId, reqMngStatus, sBuffer)) {
                    if (brokerForbiddenMap.putIfAbsent(brokerId, tmpBrokerFbdInfo) != null) {
                        brokerForbiddenCount.decrementAndGet();
                        return;
                    }
                    logger.warn(sBuffer.
                            append("[Broker AutoForbidden] master auto forbidden broker, ")
                            .append(brokerId).append("'s manage status to ")
                            .append(reqMngStatus.getDescription()).toString());
                    sBuffer.delete(0, sBuffer.length());
                } else {
                    brokerForbiddenCount.decrementAndGet();
                }
            }
        } else {
            if (updateCurManageStatus(brokerId, reqMngStatus, sBuffer)) {
                brokerFbdInfo.updateInfo(curEntry.getManageStatus(), reqMngStatus);
            }
        }
    }

    public void setBrokerHeartBeatReqStatus(int brokerId,
                                            ClientMaster.HeartResponseM2B.Builder builder) {
        BrokerFbdInfo brokerFbdInfo = brokerForbiddenMap.get(brokerId);
        if (brokerFbdInfo == null) {
            builder.setStopWrite(false);
            builder.setStopRead(false);
        } else {
            switch (brokerFbdInfo.newStatus) {
                case STATUS_MANAGE_ONLINE_NOT_READ: {
                    builder.setStopWrite(false);
                    builder.setStopRead(true);
                }
                break;
                case STATUS_MANAGE_ONLINE_NOT_WRITE: {
                    builder.setStopWrite(true);
                    builder.setStopRead(false);
                }
                break;
                case STATUS_MANAGE_OFFLINE: {
                    builder.setStopWrite(true);
                    builder.setStopRead(true);
                }
                break;
                case STATUS_MANAGE_ONLINE:
                default: {
                    builder.setStopWrite(false);
                    builder.setStopRead(false);
                }
            }
        }
    }

    public Map<Integer, BrokerInfo> getBrokerInfos(Collection<Integer> brokerIds) {
        HashMap<Integer, BrokerInfo> brokerMap = new HashMap<>();
        for (Integer brokerId : brokerIds) {
            brokerMap.put(brokerId, brokerInfoMap.get(brokerId));
        }
        return brokerMap;
    }

    /**
     * Remove broker info and decrease total broker count and forbidden broker count
     *
     * @param brokerId
     * @return the deleted broker info
     */
    public BrokerInfo removeBroker(Integer brokerId) {
        BrokerInfo brokerInfo = brokerInfoMap.remove(brokerId);
        brokerAbnormalMap.remove(brokerId);
        BrokerFbdInfo brokerFbdInfo = brokerForbiddenMap.remove(brokerId);
        if (brokerInfo != null) {
            this.brokerTotalCount.decrementAndGet();
        }
        if (brokerFbdInfo != null) {
            this.brokerForbiddenCount.decrementAndGet();
        }
        return brokerInfo;
    }

    public Map<Integer, BrokerInfo> getBrokerInfoMap() {
        return brokerInfoMap;
    }

    public void clear() {
        brokerInfoMap.clear();
        brokerForbiddenCount.set(0);
        brokerAbnormalMap.clear();
        brokerForbiddenMap.clear();

    }

    public int getCurrentBrokerCount() {
        return this.brokerForbiddenCount.get();
    }

    /**
     * Deduce status according to publish status and subscribe status
     *
     * @param repWriteStatus
     * @param repReadStatus
     * @return
     */
    private ManageStatus getManageStatus(int repWriteStatus, int repReadStatus) {
        ManageStatus manageStatus;
        if (repWriteStatus == 0 && repReadStatus == 0) {
            manageStatus = ManageStatus.STATUS_MANAGE_ONLINE;
        } else if (repReadStatus != 0) {
            manageStatus = ManageStatus.STATUS_MANAGE_OFFLINE;
        } else {
            manageStatus = ManageStatus.STATUS_MANAGE_ONLINE_NOT_WRITE;
        }
        return manageStatus;
    }

    /**
     * Update broker status, if broker is not online, this operator will fail
     *
     * @param brokerId
     * @param newMngStatus
     * @return true if success otherwise false
     */
    private boolean updateCurManageStatus(int brokerId,
                                          ManageStatus newMngStatus,
                                          StringBuilder sBuffer) {
        ProcessResult result = new ProcessResult();
        Set<Integer> brokerIdSet = new HashSet<>(1);
        brokerIdSet.add(brokerId);
        BaseEntity opEntity =
                new BaseEntity(TBaseConstants.META_VALUE_UNDEFINED,
                        "Broker AutoReport", new Date());
        metaDataManager.changeBrokerConfStatus(opEntity,
                brokerIdSet, newMngStatus, sBuffer, result);
        return result.isSuccess();
    }

    public Map<Integer, BrokerAbnInfo> getBrokerAbnormalMap() {
        return brokerAbnormalMap;
    }

    public BrokerFbdInfo getAutoForbiddenBrokerInfo(int brokerId) {
        return this.brokerForbiddenMap.get(brokerId);
    }

    public Map<Integer, BrokerFbdInfo> getAutoForbiddenBrokerMapInfo() {
        return this.brokerForbiddenMap;
    }

    /**
     * Release forbidden broker info
     *
     * @param brokerIdSet
     * @param reason
     */
    public void relAutoForbiddenBrokerInfo(Set<Integer> brokerIdSet, String reason) {
        if (brokerIdSet == null || brokerIdSet.isEmpty()) {
            return;
        }
        List<BrokerFbdInfo> brokerFbdInfos = new ArrayList<>();
        for (Integer brokerId : brokerIdSet) {
            BrokerFbdInfo fbdInfo = this.brokerForbiddenMap.remove(brokerId);
            if (fbdInfo != null) {
                brokerFbdInfos.add(fbdInfo);
                this.brokerAbnormalMap.remove(brokerId);
                this.brokerForbiddenCount.decrementAndGet();
            }
        }
        if (!brokerFbdInfos.isEmpty()) {
            logger.info(new StringBuilder(512)
                    .append("[Broker AutoForbidden] remove forbidden brokers by reason ")
                    .append(reason).append(", release list is ").append(brokerFbdInfos.toString()).toString());
        }
    }

    public static class BrokerAbnInfo {
        private int brokerId;
        private int abnStatus;  // 0 normal , -100 read abnormal, -1 write abnormal, -101 r & w abnormal
        private long firstRepTime;
        private long lastRepTime;

        public BrokerAbnInfo(int brokerId, int reportReadStatus, int reportWriteStatus) {
            this.brokerId = brokerId;
            this.abnStatus = reportReadStatus * 100 + reportWriteStatus;
            this.firstRepTime = System.currentTimeMillis();
            this.lastRepTime = this.firstRepTime;
        }

        public void updateLastRepStatus(int reportReadStatus, int reportWriteStatus) {
            this.abnStatus = reportReadStatus * 100 + reportWriteStatus;
            this.lastRepTime = System.currentTimeMillis();
        }

        public int getAbnStatus() {
            return abnStatus;
        }

        public long getFirstRepTime() {
            return firstRepTime;
        }

        public long getLastRepTime() {
            return lastRepTime;
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("brokerId", brokerId)
                    .append("abnStatus", abnStatus)
                    .append("firstRepTime", firstRepTime)
                    .append("lastRepTime", lastRepTime)
                    .toString();
        }
    }

    public static class BrokerFbdInfo {
        private int brokerId;
        private ManageStatus befStatus;
        private ManageStatus newStatus;
        private long forbiddenTime;
        private long lastUpdateTime;

        public BrokerFbdInfo(int brokerId, ManageStatus befStatus,
                             ManageStatus newStatus, long forbiddenTime) {
            this.brokerId = brokerId;
            this.befStatus = befStatus;
            this.newStatus = newStatus;
            this.forbiddenTime = forbiddenTime;
            this.lastUpdateTime = forbiddenTime;
        }

        public void updateInfo(ManageStatus befStatus, ManageStatus newStatus) {
            this.befStatus = befStatus;
            this.newStatus = newStatus;
            this.lastUpdateTime = System.currentTimeMillis();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                .append("brokerId", brokerId)
                .append("befStatus", befStatus)
                .append("newStatus", newStatus)
                .append("forbiddenTime", forbiddenTime)
                .append("lastUpdateTime", lastUpdateTime)
                .toString();
        }
    }

}
