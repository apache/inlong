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

package org.apache.inlong.tubemq.server.master.nodemanage.nodebroker;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.inlong.tubemq.corebase.TokenConstants;
import org.apache.inlong.tubemq.corebase.utils.CheckSum;
import org.apache.inlong.tubemq.corebase.utils.TStringUtils;
import org.apache.inlong.tubemq.corebase.utils.Tuple2;
import org.apache.inlong.tubemq.server.broker.metadata.ClusterConfigHolder;
import org.apache.inlong.tubemq.server.common.TServerConstants;
import org.apache.inlong.tubemq.server.common.TStatusConstants;
import org.apache.inlong.tubemq.server.master.bdbstore.bdbentitys.BdbBrokerConfEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BrokerSyncStatusInfo {
    private static final Logger logger =
            LoggerFactory.getLogger(BrokerSyncStatusInfo.class);

    private boolean isFirstInit = true;
    private int brokerId = -2;
    private String brokerIp;        //broker ip
    private int brokerPort;         //broker port
    private int brokerTLSPort;      //broker tls port

    /* Broker manager stratus ：-2:undefine，1:pending for approval，5:online，7:offline */
    private int brokerManageStatus = -2;

    /* Broker run status, -2:undefine, 31: online(wait-server), 32:online(readonly), 33:online(read&write)
    *  51: offline(unreadable&not writeable), 52: offline(wait load balance) */
    private int brokerRunStatus = -2;

    private long subStepOpTimeInMills = 0;
    private boolean isBrokerRegister = false;       //broker register flag
    private boolean isBrokerOnline = false;         //broker online flag
    private boolean isOverTLS = false;              //enable tls
    private boolean isBrokerConfChanged = false;     //config change flag
    private boolean isBrokerLoaded = false;         //broker load status
    private boolean isFastStart = false;            //enable fast start

    private long lastPushBrokerConfId = -2;
    private int lastPushBrokerCheckSumId = -2;
    private long lastDataPushInMills = 0;
    private String lastPushBrokerDefaultConfInfo;
    private List<String> lastPushBrokerTopicSetConfInfo =
            new ArrayList<>();

    private long reportedBrokerConfId = -2;
    private int reportedBrokerCheckSumId = -2;
    private long lastDataReportInMills = 0;
    private String reportedBrokerDefaultConfInfo;
    private List<String> reportedBrokerTopicSetConfInfo =
            new ArrayList<>();

    private AtomicLong currBrokerConfId = new AtomicLong(0);
    private int currBrokerCheckSumId = 0;
    private String curBrokerDefaultConfInfo;
    private List<String> curBrokerTopicSetConfInfo =
            new ArrayList<>();

    private int numPartitions = 1;              //partition number
    private int numTopicStores = 1;             //store number
    private int unFlushDataHold = TServerConstants.CFG_DEFAULT_DATA_UNFLUSH_HOLD;
    private int unflushThreshold = 1000;        //flush threshold
    private int unflushInterval = 10000;        //flush interval
    private int memCacheMsgSizeInMB = 3;        //memory cache size
    private int memCacheMsgCntInK = 10;         //memory cache message count
    private int memCacheFlushIntvl = 20000;     //memory cache flush interval
    private String deletePolicy = "delete,168h";    //data delete policy
    private String deleteWhen = "0 0 6,18 * * ?";   //date delete policy execute time
    private boolean acceptPublish = true;           //accept publish
    private boolean acceptSubscribe = true;         //accept subscribe

    //Constructor
    public BrokerSyncStatusInfo(final BdbBrokerConfEntity bdbEntity,
                                List<String> brokerTopicSetConfInfo) {
        updateBrokerConfigureInfo(bdbEntity.getBrokerDefaultConfInfo(),
                brokerTopicSetConfInfo);
        this.brokerManageStatus = bdbEntity.getManageStatus();
        this.isBrokerConfChanged = bdbEntity.isConfDataUpdated();
        this.isBrokerLoaded = bdbEntity.isBrokerLoaded();
        this.brokerId = bdbEntity.getBrokerId();
        this.brokerIp = bdbEntity.getBrokerIp();
        this.brokerPort = bdbEntity.getBrokerPort();
        this.brokerTLSPort = bdbEntity.getBrokerTLSPort();
        this.isFastStart = false;
        if (this.brokerManageStatus > TStatusConstants.STATUS_MANAGE_APPLY) {
            currBrokerConfId.incrementAndGet();
        }
    }

    /**
     * Update current broker config info
     *
     * @param brokerManageStatus     broker status
     * @param isBrokerConfChanged
     * @param isBrokerLoaded
     * @param brokerDefaultConfInfo  broker default config
     * @param brokerTopicSetConfInfo topic config
     * @param isOnlineUpdate
     */
    public void updateCurrBrokerConfInfo(int brokerManageStatus, boolean isBrokerConfChanged,
                                         boolean isBrokerLoaded, String brokerDefaultConfInfo,
                                         List<String> brokerTopicSetConfInfo,
                                         boolean isOnlineUpdate) {
        this.brokerManageStatus = brokerManageStatus;
        this.currBrokerConfId.incrementAndGet();
        this.isBrokerConfChanged = isBrokerConfChanged;
        this.isBrokerLoaded = isBrokerLoaded;
        updateBrokerConfigureInfo(brokerDefaultConfInfo, brokerTopicSetConfInfo);
        this.lastPushBrokerConfId = this.currBrokerConfId.get();
        this.lastPushBrokerCheckSumId = this.currBrokerCheckSumId;
        this.lastDataPushInMills = System.currentTimeMillis();
        this.lastPushBrokerDefaultConfInfo = this.curBrokerDefaultConfInfo;
        this.lastPushBrokerTopicSetConfInfo = this.curBrokerTopicSetConfInfo;
        switch (this.brokerManageStatus) {
            case TStatusConstants.STATUS_MANAGE_ONLINE: {
                this.brokerRunStatus = isOnlineUpdate
                        ? TStatusConstants.STATUS_SERVICE_TOONLINE_PART_WAIT_REGISTER
                        : TStatusConstants.STATUS_SERVICE_TOONLINE_WAIT_REGISTER;
                break;
            }
            case TStatusConstants.STATUS_MANAGE_ONLINE_NOT_WRITE: {
                this.brokerRunStatus = TStatusConstants.STATUS_SERVICE_TOONLINE_ONLY_READ;
                break;
            }
            case TStatusConstants.STATUS_MANAGE_ONLINE_NOT_READ: {
                this.brokerRunStatus = TStatusConstants.STATUS_SERVICE_TOONLINE_ONLY_WRITE;
                break;
            }
            default: {
                if (this.isBrokerRegister) {
                    this.brokerRunStatus = TStatusConstants.STATUS_SERVICE_TOOFFLINE_NOT_WRITE;
                }
            }
        }
        this.subStepOpTimeInMills = System.currentTimeMillis();
    }

    /**
     * Reset broker report info
     */
    public void resetBrokerReportInfo() {
        this.reportedBrokerConfId = -2;
        this.reportedBrokerCheckSumId = -2;
        this.reportedBrokerDefaultConfInfo = "";
        this.reportedBrokerTopicSetConfInfo =
                new ArrayList<>();
        this.isBrokerRegister = false;
        this.isBrokerOnline = false;
        this.isFastStart = false;
        this.brokerRunStatus = TStatusConstants.STATUS_SERVICE_UNDEFINED;
        this.subStepOpTimeInMills = System.currentTimeMillis();
    }

    /**
     * Set broker report info
     *
     * @param isRegister
     * @param reportConfigId
     * @param reportCheckSumId
     * @param isTackData
     * @param reportDefaultConfInfo
     * @param reportTopicSetConfInfo
     * @param isBrokerRegister
     * @param isBrokerOnline
     * @param isOverTLS
     */
    public void setBrokerReportInfo(boolean isRegister, long reportConfigId,
                                    int reportCheckSumId, boolean isTackData,
                                    String reportDefaultConfInfo,
                                    List<String> reportTopicSetConfInfo,
                                    boolean isBrokerRegister, boolean isBrokerOnline, boolean isOverTLS) {
        this.reportedBrokerConfId = reportConfigId;
        this.reportedBrokerCheckSumId = reportCheckSumId;
        if (isTackData) {
            this.reportedBrokerDefaultConfInfo = reportDefaultConfInfo;
            if (reportTopicSetConfInfo == null) {
                this.reportedBrokerTopicSetConfInfo = new ArrayList<>();
            } else {
                this.reportedBrokerTopicSetConfInfo = reportTopicSetConfInfo;
            }
            this.lastDataReportInMills = System.currentTimeMillis();
        }
        this.isBrokerRegister = isBrokerRegister;
        this.isBrokerOnline = isBrokerOnline;
        this.isOverTLS = isOverTLS;
        if (isRegister) {
            if (this.isBrokerOnline) {
                if (this.reportedBrokerConfId <= 0) {
                    if (this.isBrokerConfChanged
                            || !this.isBrokerLoaded
                            || this.reportedBrokerCheckSumId != this.lastPushBrokerCheckSumId
                            || !isFirstInit) {
                        return;
                    }
                    this.lastPushBrokerConfId = this.reportedBrokerConfId;
                    this.currBrokerConfId.set(this.lastPushBrokerConfId);
                    this.lastPushBrokerCheckSumId = this.currBrokerCheckSumId;
                    this.lastDataPushInMills = System.currentTimeMillis();
                    this.brokerRunStatus =
                            TStatusConstants.STATUS_SERVICE_TOONLINE_ONLY_READ;
                    this.subStepOpTimeInMills = System.currentTimeMillis();
                    this.isFirstInit = false;
                } else {
                    this.isFirstInit = false;
                    this.isFastStart = true;
                    switch (this.brokerManageStatus) {
                        case TStatusConstants.STATUS_MANAGE_ONLINE: {
                            this.brokerRunStatus =
                                    TStatusConstants.STATUS_SERVICE_TOONLINE_ONLY_READ;
                            break;
                        }
                        case TStatusConstants.STATUS_MANAGE_ONLINE_NOT_WRITE: {
                            this.brokerRunStatus =
                                    TStatusConstants.STATUS_SERVICE_TOONLINE_ONLY_READ;
                            break;
                        }
                        case TStatusConstants.STATUS_MANAGE_ONLINE_NOT_READ: {
                            this.brokerRunStatus =
                                    TStatusConstants.STATUS_SERVICE_TOONLINE_ONLY_WRITE;
                            break;
                        }
                        default: {
                            this.brokerRunStatus =
                                    TStatusConstants.STATUS_SERVICE_TOOFFLINE_NOT_WRITE;
                        }
                    }
                    this.subStepOpTimeInMills = 0;
                }
            }
        }
    }

    /**
     * Set broker status to offline
     */
    public void setBrokerOffline() {
        if (this.brokerManageStatus != TStatusConstants.STATUS_MANAGE_OFFLINE) {
            this.brokerManageStatus = TStatusConstants.STATUS_MANAGE_OFFLINE;
        }
        if (this.isBrokerOnline) {
            this.brokerRunStatus = TStatusConstants.STATUS_SERVICE_TOOFFLINE_NOT_WRITE;
        } else if (this.isBrokerRegister) {
            this.brokerRunStatus = TStatusConstants.STATUS_SERVICE_TOOFFLINE_NOT_READ_WRITE;
        } else {
            this.brokerRunStatus = TStatusConstants.STATUS_SERVICE_TOOFFLINE_WAIT_REBALANCE;
        }
        this.subStepOpTimeInMills = System.currentTimeMillis();
    }

    /**
     * Check if need sync config data to broker
     *
     * @return true if need otherwise false
     */
    public boolean needSyncConfDataToBroker() {
        if (this.lastPushBrokerConfId != this.reportedBrokerConfId
                || this.lastPushBrokerCheckSumId != this.reportedBrokerCheckSumId) {
            return true;
        }
        return false;
    }

    /**
     * According to last report time and current time to decide if need to report data
     *
     * @return true if need report data otherwise false
     */
    public boolean needReportData() {
        if (System.currentTimeMillis() - this.lastDataPushInMills
                > TServerConstants.CFG_REPORT_DEFAULT_SYNC_DURATION) {
            this.lastDataPushInMills = System.currentTimeMillis();
            return true;
        }
        return false;
    }

    public void forceSyncConfDataToBroker() {
        this.isFastStart = true;
        this.lastPushBrokerConfId = this.currBrokerConfId.incrementAndGet();
        this.lastPushBrokerCheckSumId = this.currBrokerCheckSumId;
        this.lastPushBrokerDefaultConfInfo = this.curBrokerDefaultConfInfo;
        this.lastPushBrokerTopicSetConfInfo = this.curBrokerTopicSetConfInfo;
        this.lastDataPushInMills = System.currentTimeMillis();
    }

    public Long getCurrBrokerConfId() {
        return currBrokerConfId.get();
    }

    public int getCurrBrokerCheckSumId() {
        return currBrokerCheckSumId;
    }

    public String getCurBrokerDefaultConfInfo() {
        return curBrokerDefaultConfInfo;
    }

    public List<String> getCurBrokerTopicSetConfInfo() {
        return curBrokerTopicSetConfInfo;
    }

    public long getLastDataReportInMills() {
        return lastDataReportInMills;
    }

    public void setLastDataReportInMills(long lastDataReportInMills) {
        this.lastDataReportInMills = lastDataReportInMills;
    }

    public boolean isFastStart() {
        return isFastStart;
    }

    public void setFastStart(boolean isFastStart) {
        this.isFastStart = isFastStart;
    }

    public boolean isBrokerOnline() {
        return this.isBrokerOnline;
    }

    public void setBrokerOnline(boolean isBrokerOnline) {
        this.isBrokerOnline = isBrokerOnline;
    }

    public boolean isBrokerRegister() {
        return this.isBrokerRegister;
    }

    public void setBrokerRunStatus(boolean isBrokerRegister,
                                   boolean isBrokerOnline) {
        this.isBrokerRegister = isBrokerRegister;
        this.isBrokerOnline = isBrokerOnline;
    }

    public void setBrokerRunStatus(int brokerRunStatus) {
        this.brokerRunStatus = brokerRunStatus;
        this.subStepOpTimeInMills = System.currentTimeMillis();
    }

    public long getReportedBrokerConfId() {
        return reportedBrokerConfId;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public int getUnflushThreshold() {
        return unflushThreshold;
    }

    public int getUnflushInterval() {
        return unflushInterval;
    }

    public String getDeletePolicy() {
        return deletePolicy;
    }

    public String getDeleteWhen() {
        return deleteWhen;
    }

    public boolean isAcceptPublish() {
        return acceptPublish;
    }

    public boolean isAcceptSubscribe() {
        return acceptSubscribe;
    }

    public int getBrokerRunStatus() {
        return brokerRunStatus;
    }

    public boolean isOverTLS() {
        return isOverTLS;
    }

    public void setOverTLS(boolean overTLS) {
        isOverTLS = overTLS;
    }

    public int getBrokerTLSPort() {
        return brokerTLSPort;
    }

    public void setBrokerTLSPort(int brokerTLSPort) {
        this.brokerTLSPort = brokerTLSPort;
    }

    public long getSubStepOpTimeInMills() {
        return subStepOpTimeInMills;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public String getBrokerIp() {
        return brokerIp;
    }

    public int getBrokerPort() {
        return brokerPort;
    }

    public int getBrokerManageStatus() {
        return brokerManageStatus;
    }

    private int calculateConfigCrc32Value(final String brokerDefaultConfInfo,
                                          final List<String> brokerTopicSetConfInfo) {
        int result = -1;
        int capacity = 0;
        Collections.sort(brokerTopicSetConfInfo);
        capacity += brokerDefaultConfInfo.length();
        for (String itemStr : brokerTopicSetConfInfo) {
            capacity += itemStr.length();
        }
        capacity *= 2;
        for (int i = 1; i < 3; i++) {
            result = inCalcBufferResult(capacity, brokerDefaultConfInfo, brokerTopicSetConfInfo);
            if (result >= 0) {
                return result;
            }
            capacity *= i + 1;
        }
        logger.error("Calc BrokerConfigure Crc error!");
        return 0;
    }

    private int inCalcBufferResult(int capacity, final String brokerDefaultConfInfo,
                                   final List<String> brokerTopicSetConfInfo) {
        final ByteBuffer buffer = ByteBuffer.allocate(capacity);
        buffer.put(StringUtils.getBytesUtf8(brokerDefaultConfInfo));
        for (String itemStr : brokerTopicSetConfInfo) {
            byte[] itemData = StringUtils.getBytesUtf8(itemStr);
            if (itemData.length > buffer.remaining()) {
                return -1;
            }
            buffer.put(itemData);
        }
        return CheckSum.crc32(buffer.array());
    }

    /**
     * Update broker config, field will set to default value if brokerDefaultConfInfo is empty,
     * else will parse the string value and then set broker config
     *
     * @param brokerDefaultConfInfo  a string, field join with ":",
     * @param brokerTopicSetConfInfo
     */
    private void updateBrokerConfigureInfo(String brokerDefaultConfInfo,
                                           List<String> brokerTopicSetConfInfo) {
        int crc32CheckSum =
                calculateConfigCrc32Value(brokerDefaultConfInfo, brokerTopicSetConfInfo);
        if (crc32CheckSum != this.currBrokerCheckSumId) {
            this.currBrokerCheckSumId = crc32CheckSum;
            this.curBrokerTopicSetConfInfo = brokerTopicSetConfInfo;
            this.curBrokerDefaultConfInfo = brokerDefaultConfInfo;
            if (TStringUtils.isBlank(brokerDefaultConfInfo)) {
                this.numPartitions = 1;
                this.numTopicStores = 1;
                this.unflushThreshold = 1000;
                this.unflushInterval = 10000;
                this.unFlushDataHold = TServerConstants.CFG_DEFAULT_DATA_UNFLUSH_HOLD;
                this.deletePolicy = "delete,168h";
                this.deleteWhen = "0 0 6,18 * * ?";
                this.acceptPublish = true;
                this.acceptSubscribe = true;
                this.memCacheFlushIntvl = 20000;
                this.memCacheMsgCntInK = 10;
                this.memCacheMsgSizeInMB = 3;
            } else {
                String[] brokerDefaultConfInfoArr =
                        brokerDefaultConfInfo.split(TokenConstants.ATTR_SEP);
                this.numPartitions = Integer.parseInt(brokerDefaultConfInfoArr[0]);
                this.acceptPublish = Boolean.parseBoolean(brokerDefaultConfInfoArr[1]);
                this.acceptSubscribe = Boolean.parseBoolean(brokerDefaultConfInfoArr[2]);
                this.unflushThreshold = Integer.parseInt(brokerDefaultConfInfoArr[3]);
                this.unflushInterval = Integer.parseInt(brokerDefaultConfInfoArr[4]);
                this.deleteWhen = brokerDefaultConfInfoArr[5];
                this.deletePolicy = brokerDefaultConfInfoArr[6];
                if (!TStringUtils.isBlank(brokerDefaultConfInfoArr[7])) {
                    this.numTopicStores = Integer.parseInt(brokerDefaultConfInfoArr[7]);
                }
                if (!TStringUtils.isBlank(brokerDefaultConfInfoArr[8])) {
                    this.unFlushDataHold = Integer.parseInt(brokerDefaultConfInfoArr[8]);
                }
                if (!TStringUtils.isBlank(brokerDefaultConfInfoArr[9])) {
                    this.memCacheMsgSizeInMB = Integer.parseInt(brokerDefaultConfInfoArr[9]);
                }
                if (!TStringUtils.isBlank(brokerDefaultConfInfoArr[10])) {
                    this.memCacheMsgCntInK = Integer.parseInt(brokerDefaultConfInfoArr[10]);
                }
                if (!TStringUtils.isBlank(brokerDefaultConfInfoArr[11])) {
                    this.memCacheFlushIntvl = Integer.parseInt(brokerDefaultConfInfoArr[11]);
                }
            }
        }
    }

    public long getLastPushBrokerConfId() {
        return lastPushBrokerConfId;
    }

    public void setLastPushBrokerConfId(long lastPushBrokerConfId) {
        this.lastPushBrokerConfId = lastPushBrokerConfId;
    }

    public int getLastPushBrokerCheckSumId() {
        return lastPushBrokerCheckSumId;
    }

    public void setLastPushBrokerCheckSumId(int lastPushBrokerCheckSumId) {
        this.lastPushBrokerCheckSumId = lastPushBrokerCheckSumId;
    }

    public long getLastDataPushInMills() {
        return lastDataPushInMills;
    }

    public void setLastDataPushInMills(long lastDataPushInMills) {
        this.lastDataPushInMills = lastDataPushInMills;
    }

    public String getLastPushBrokerDefaultConfInfo() {
        return lastPushBrokerDefaultConfInfo;
    }

    public void setLastPushBrokerDefaultConfInfo(String lastPushBrokerDefaultConfInfo) {
        this.lastPushBrokerDefaultConfInfo = lastPushBrokerDefaultConfInfo;
    }

    public List<String> getLastPushBrokerTopicSetConfInfo() {
        return lastPushBrokerTopicSetConfInfo;
    }

    public void setLastPushBrokerTopicSetConfInfo(List<String> lastPushBrokerTopicSetConfInfo) {
        this.lastPushBrokerTopicSetConfInfo = lastPushBrokerTopicSetConfInfo;
    }

    public String getReportedBrokerDefaultConfInfo() {
        return reportedBrokerDefaultConfInfo;
    }

    public void setReportedBrokerDefaultConfInfo(String reportedBrokerDefaultConfInfo) {
        this.reportedBrokerDefaultConfInfo = reportedBrokerDefaultConfInfo;
    }

    public List<String> getReportedBrokerTopicSetConfInfo() {
        return reportedBrokerTopicSetConfInfo;
    }

    public void setReportedBrokerTopicSetConfInfo(List<String> reportedBrokerTopicSetConfInfo) {
        this.reportedBrokerTopicSetConfInfo = reportedBrokerTopicSetConfInfo;
    }

    public boolean isBrokerConfChanged() {
        return isBrokerConfChanged;
    }

    public void setBrokerConfChanged() {
        this.isBrokerConfChanged = true;
        this.isBrokerLoaded = false;
    }

    public boolean isBrokerLoaded() {
        return isBrokerLoaded;
    }

    public void setBrokerLoaded() {
        this.isBrokerLoaded = true;
        this.isBrokerConfChanged = false;
    }

    // #lizard forgives
    private StringBuilder getBrokerAndTopicConfJsonInfo(String brokerConfInfo,
                                                        String brokerJsonKey,
                                                        List<String> topicConfInfoList,
                                                        String topicListJsonKey,
                                                        final StringBuilder strBuffer) {
        // format config to json
        strBuffer.append(",\"").append(brokerJsonKey).append("\":");
        if (TStringUtils.isBlank(brokerConfInfo)) {
            strBuffer.append("{},\"").append(topicListJsonKey).append("\":[]");
            return strBuffer;
        }
        // broker default metadata
        String[] brokerDefaultConfInfoArr =
                brokerConfInfo.split(TokenConstants.ATTR_SEP);
        final int numPartitions = Integer.parseInt(brokerDefaultConfInfoArr[0]);
        final boolean acceptPublish = Boolean.parseBoolean(brokerDefaultConfInfoArr[1]);
        final boolean acceptSubscribe = Boolean.parseBoolean(brokerDefaultConfInfoArr[2]);
        final int unflushThreshold = Integer.parseInt(brokerDefaultConfInfoArr[3]);
        final int unflushInterval = Integer.parseInt(brokerDefaultConfInfoArr[4]);
        final String deleteWhen = brokerDefaultConfInfoArr[5];
        final String deletePolicy = brokerDefaultConfInfoArr[6];
        int numTopicStores = 1;
        int unFlushDataHold = TServerConstants.CFG_DEFAULT_DATA_UNFLUSH_HOLD;
        int memCacheMsgSizeInMB = 3;
        int memCacheMsgCntInK = 10;
        int memCacheFlushIntvl = 20000;
        if (!TStringUtils.isBlank(brokerDefaultConfInfoArr[7])) {
            numTopicStores =
                    Integer.parseInt(brokerDefaultConfInfoArr[7]);
        }
        if (!TStringUtils.isBlank(brokerDefaultConfInfoArr[8])) {
            unFlushDataHold =
                    Integer.parseInt(brokerDefaultConfInfoArr[8]);
        }
        if (!TStringUtils.isBlank(brokerDefaultConfInfoArr[9])) {
            memCacheMsgSizeInMB =
                    Integer.parseInt(brokerDefaultConfInfoArr[9]);
        }
        if (!TStringUtils.isBlank(brokerDefaultConfInfoArr[10])) {
            memCacheMsgCntInK =
                    Integer.parseInt(brokerDefaultConfInfoArr[10]);
        }
        if (!TStringUtils.isBlank(brokerDefaultConfInfoArr[11])) {
            memCacheFlushIntvl =
                    Integer.parseInt(brokerDefaultConfInfoArr[11]);
        }
        //format broker config to json
        strBuffer.append("{\"numPartitions\":").append(numPartitions)
                .append(",\"acceptPublish\":").append(acceptPublish)
                .append(",\"acceptSubscribe\":").append(acceptSubscribe)
                .append(",\"unflushThreshold\":").append(unflushThreshold)
                .append(",\"unflushInterval\":").append(unflushInterval)
                .append(",\"deleteWhen\":\"").append(deleteWhen)
                .append("\",\"deletePolicy\":\"").append(deletePolicy)
                .append("\",\"numTopicStores\":").append(numTopicStores)
                .append(",\"unflushDataHold\":").append(unFlushDataHold)
                .append(",\"memCacheMsgSizeInMB\":").append(memCacheMsgSizeInMB)
                .append(",\"memCacheMsgCntInK\":").append(memCacheMsgCntInK)
                .append(",\"memCacheFlushIntvl\":").append(memCacheFlushIntvl)
                .append("}");
        strBuffer.append(",\"").append(topicListJsonKey).append("\":[");
        if (topicConfInfoList == null
                || topicConfInfoList.isEmpty()) {
            strBuffer.append("]");
            return strBuffer;
        }
        // topic config metadata in the broker
        // format topic metadata
        int count = 0;
        for (String strTopicConfInfo : topicConfInfoList) {
            if (TStringUtils.isBlank(strTopicConfInfo)) {
                continue;
            }
            String[] topicConfInfoArr =
                    strTopicConfInfo.split(TokenConstants.ATTR_SEP);
            if (count++ > 0) {
                strBuffer.append(",");
            }
            strBuffer.append("{\"topicName\":\"").append(topicConfInfoArr[0]).append("\"");
            int tmpPartNum = numPartitions;
            if (!TStringUtils.isBlank(topicConfInfoArr[1])) {
                tmpPartNum = Integer.parseInt(topicConfInfoArr[1]);
            }
            strBuffer.append(",\"numPartitions\":").append(tmpPartNum);
            boolean tmpAcceptPublish = acceptPublish;
            if (!TStringUtils.isBlank(topicConfInfoArr[2])) {
                tmpAcceptPublish = Boolean.parseBoolean(topicConfInfoArr[2]);
            }
            strBuffer.append(",\"acceptPublish\":").append(tmpAcceptPublish);
            boolean tmpAcceptSubscribe = acceptSubscribe;
            if (!TStringUtils.isBlank(topicConfInfoArr[3])) {
                tmpAcceptSubscribe = Boolean.parseBoolean(topicConfInfoArr[3]);
            }
            strBuffer.append(",\"acceptSubscribe\":").append(tmpAcceptSubscribe);
            int tmpUnflushThreshold = unflushThreshold;
            if (!TStringUtils.isBlank(topicConfInfoArr[4])) {
                tmpUnflushThreshold = Integer.parseInt(topicConfInfoArr[4]);
            }
            strBuffer.append(",\"unflushThreshold\":").append(tmpUnflushThreshold);
            int tmpUnflushInterval = unflushInterval;
            if (!TStringUtils.isBlank(topicConfInfoArr[5])) {
                tmpUnflushInterval = Integer.parseInt(topicConfInfoArr[5]);
            }
            strBuffer.append(",\"unflushInterval\":").append(tmpUnflushInterval);
            String tmpDeleteWhen = deleteWhen;
            if (!TStringUtils.isBlank(topicConfInfoArr[6])) {
                tmpDeleteWhen = topicConfInfoArr[6];
            }
            strBuffer.append(",\"deleteWhen\":\"").append(tmpDeleteWhen).append("\"");
            String tmpDeletePolicy = deletePolicy;
            if (!TStringUtils.isBlank(topicConfInfoArr[7])) {
                tmpDeletePolicy = topicConfInfoArr[7];
            }
            int tmpNumTopicStores = numTopicStores;
            if (!TStringUtils.isBlank(topicConfInfoArr[8])) {
                tmpNumTopicStores = Integer.parseInt(topicConfInfoArr[8]);
            }
            strBuffer.append(",\"numTopicStores\":").append(tmpNumTopicStores);
            strBuffer.append(",\"deletePolicy\":\"").append(tmpDeletePolicy).append("\"");
            int topicStatusId = TStatusConstants.STATUS_TOPIC_OK;
            if (!TStringUtils.isBlank(topicConfInfoArr[9])) {
                topicStatusId = Integer.parseInt(topicConfInfoArr[9]);
            }
            int tmpunFlushDataHold = unFlushDataHold;
            if (!TStringUtils.isBlank(topicConfInfoArr[10])) {
                tmpunFlushDataHold = Integer.parseInt(topicConfInfoArr[10]);
            }
            strBuffer.append(",\"unflushDataHold\":").append(tmpunFlushDataHold);
            int tmpmemCacheMsgSizeInMB = memCacheMsgSizeInMB;
            int tmpmemCacheMsgCntInK = memCacheMsgCntInK;
            int tmpmemCacheFlushIntvl = memCacheFlushIntvl;
            int tmpMaxMsgSize = ClusterConfigHolder.getMaxMsgSize();
            int tmpMinMemCacheSize = ClusterConfigHolder.getMinMemCacheSize();
            if (!TStringUtils.isBlank(topicConfInfoArr[11])) {
                tmpmemCacheMsgSizeInMB = Integer.parseInt(topicConfInfoArr[11]);
            }
            if (!TStringUtils.isBlank(topicConfInfoArr[12])) {
                tmpmemCacheMsgCntInK = Integer.parseInt(topicConfInfoArr[12]);
            }
            if (!TStringUtils.isBlank(topicConfInfoArr[13])) {
                tmpmemCacheFlushIntvl = Integer.parseInt(topicConfInfoArr[13]);
            }
            if (topicConfInfoArr.length > 14) {
                if (TStringUtils.isNotBlank(topicConfInfoArr[14])) {
                    tmpMaxMsgSize = Integer.parseInt(topicConfInfoArr[14]);
                    Tuple2<Integer, Integer> calcResult =
                            ClusterConfigHolder.calcMaxMsgSize(tmpMaxMsgSize);
                    tmpMaxMsgSize = calcResult.getF0();
                    tmpMinMemCacheSize = calcResult.getF1();
                }
            }
            strBuffer.append(",\"memCacheMsgSizeInMB\":").append(tmpmemCacheMsgSizeInMB);
            strBuffer.append(",\"memCacheMsgCntInK\":").append(tmpmemCacheMsgCntInK);
            strBuffer.append(",\"memCacheFlushIntvl\":").append(tmpmemCacheFlushIntvl);
            strBuffer.append(",\"maxMsgSize\":").append(tmpMaxMsgSize);
            strBuffer.append(",\"minMemCacheSize\":").append(tmpMinMemCacheSize);
            strBuffer.append(",\"topicStatusId\":").append(topicStatusId);
            strBuffer.append("}");
        }
        strBuffer.append("]");
        return strBuffer;
    }

    /* Format to json */
    public StringBuilder toJsonString(StringBuilder strBuffer, boolean isOrig) {
        strBuffer.append("\"BrokerSyncStatusInfo\":{\"type\":\"BrokerSyncStatusInfo\",\"brokerId\":").append(brokerId)
                .append(",\"brokerAddress\":\"").append(brokerIp).append(":").append(brokerPort)
                .append("\",\"brokerManageStatus\":").append(brokerManageStatus)
                .append(",\"brokerRunStatus\":").append(brokerRunStatus)
                .append(",\"subStepOpTimeInMills\":").append(subStepOpTimeInMills)
                .append(",\"lastDataReportInMills\":").append(lastDataReportInMills)
                .append(",\"isBrokerRegister\":").append(isBrokerRegister)
                .append(",\"isBrokerOnline\":").append(isBrokerOnline)
                .append(",\"isFirstInit\":").append(isFirstInit)
                .append(",\"isBrokerConfChanged\":").append(isBrokerConfChanged)
                .append(",\"isBrokerLoaded\":").append(isBrokerLoaded)
                .append(",\"isFastStart\":").append(isFastStart);
        if (isOrig) {
            strBuffer.append(",\"currBrokerConfId\":").append(currBrokerConfId.get())
                    .append(",\"currBrokerCheckSumId\":").append(currBrokerCheckSumId)
                    .append(",\"curBrokerDefaultConfInfo\":\"").append(curBrokerDefaultConfInfo)
                    .append("\",\"curBrokerTopicSetConfInfo\":\"").append(curBrokerTopicSetConfInfo.toString())
                    .append("\",\"lastPushBrokerConfId\":").append(lastPushBrokerConfId)
                    .append(",\"lastPushBrokerCheckSumId\":").append(lastPushBrokerCheckSumId)
                    .append(",\"lastPushBrokerDefaultConfInfo\":\"").append(lastPushBrokerDefaultConfInfo)
                    .append("\",\"lastPushBrokerTopicSetConfInfo\":\"")
                    .append(lastPushBrokerTopicSetConfInfo.toString())
                    .append(",\"reportedBrokerConfId\":").append(reportedBrokerConfId)
                    .append(",\"reportedBrokerCheckSumId\":").append(reportedBrokerCheckSumId)
                    .append(",\"reportedBrokerDefaultConfInfo\":\"").append(reportedBrokerDefaultConfInfo)
                    .append("\",\"reportedBrokerTopicSetConfInfo\":\"")
                    .append(reportedBrokerTopicSetConfInfo.toString())
                    .append("}");
        } else {
            strBuffer.append(",\"currBrokerConfId\":").append(currBrokerConfId.get())
                    .append(",\"currBrokerCheckSumId\":").append(currBrokerCheckSumId);
            strBuffer = getBrokerAndTopicConfJsonInfo(curBrokerDefaultConfInfo,
                    "curBrokerDefaultConfInfo",
                    curBrokerTopicSetConfInfo,
                    "curBrokerTopicSetConfInfo",
                    strBuffer);
            strBuffer.append(",\"lastPushBrokerConfId\":").append(lastPushBrokerConfId)
                    .append(",\"lastPushBrokerCheckSumId\":").append(lastPushBrokerCheckSumId);
            strBuffer = getBrokerAndTopicConfJsonInfo(lastPushBrokerDefaultConfInfo,
                    "lastPushBrokerDefaultConfInfo",
                    lastPushBrokerTopicSetConfInfo,
                    "lastPushBrokerTopicSetConfInfo",
                    strBuffer);
            strBuffer.append(",\"reportedBrokerConfId\":").append(reportedBrokerConfId)
                    .append(",\"reportedBrokerCheckSumId\":").append(reportedBrokerCheckSumId);
            strBuffer = getBrokerAndTopicConfJsonInfo(reportedBrokerDefaultConfInfo,
                    "reportedBrokerDefaultConfInfo",
                    reportedBrokerTopicSetConfInfo,
                    "reportedBrokerTopicSetConfInfo",
                    strBuffer);
            strBuffer.append("}");
        }
        return strBuffer;
    }


}
