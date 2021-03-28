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

package org.apache.tubemq.server.master.metamanage.metastore.dao.entity;

import java.util.Date;
import java.util.Objects;
import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.corebase.TokenConstants;
import org.apache.tubemq.corebase.utils.TStringUtils;
import org.apache.tubemq.server.common.statusdef.ManageStatus;
import org.apache.tubemq.server.master.bdbstore.bdbentitys.BdbBrokerConfEntity;



/*
 * store the broker default setting
 *
 */

public class BrokerConfEntity extends BaseEntity  {
    // Primary Key
    private int brokerId = TBaseConstants.META_VALUE_UNDEFINED;
    private String brokerIp = "";
    private int brokerPort = TBaseConstants.META_VALUE_UNDEFINED;
    // broker tls port
    private int brokerTLSPort = TBaseConstants.META_VALUE_UNDEFINED;
    // broker web port
    private int brokerWebPort = TBaseConstants.META_VALUE_UNDEFINED;
    private ManageStatus manageStatus = ManageStatus.STATUS_MANAGE_UNDEFINED;
    private boolean isConfDataUpdated = false;  //conf data update flag
    private boolean isBrokerLoaded = false;     //broker conf load flag
    private int regionId = TBaseConstants.META_VALUE_UNDEFINED;
    private int groupId = TBaseConstants.META_VALUE_UNDEFINED;
    private TopicPropGroup topicProps = null;
    private String brokerAddress = "";       // broker ip:port
    private String brokerFullInfo = "";      // broker brokerId:ip:port
    private String brokerSimpleInfo = "";    // broker brokerId:ip:
    private String brokerTLSSimpleInfo = ""; //tls simple info
    private String brokerTLSFullInfo = "";   //tls full info


    public BrokerConfEntity() {
        super();
    }


    public BrokerConfEntity(int brokerId, String brokerIp, int brokerPort,
                            int brokerTLSPort, int brokerWebPort, ManageStatus manageStatus,
                            int regionId, int groupId, TopicPropGroup defTopicProps,
                            boolean isConfDataUpdated, boolean isBrokerLoaded,
                            long dataVersionId, String createUser,
                            Date createDate, String modifyUser, Date modifyDate) {
        super(dataVersionId, createUser, createDate, modifyUser, modifyDate);
        setBrokerIpAndAllPort(brokerId, brokerIp, brokerPort, brokerTLSPort);
        this.regionId = regionId;
        this.groupId = groupId;
        this.brokerWebPort = brokerWebPort;
        this.topicProps = defTopicProps;
        this.manageStatus = manageStatus;
        this.isConfDataUpdated = isConfDataUpdated;
        this.isBrokerLoaded = isBrokerLoaded;
    }

    public BrokerConfEntity(BdbBrokerConfEntity bdbEntity) {
        super(bdbEntity.getDataVerId(), bdbEntity.getRecordCreateUser(),
                bdbEntity.getRecordCreateDate(), bdbEntity.getRecordModifyUser(),
                bdbEntity.getRecordModifyDate());
        setBrokerIpAndAllPort(bdbEntity.getBrokerId(), bdbEntity.getBrokerIp(),
                bdbEntity.getBrokerPort(), bdbEntity.getBrokerTLSPort());
        this.regionId = bdbEntity.getRegionId();
        this.groupId = bdbEntity.getBrokerGroupId();
        this.brokerWebPort = bdbEntity.getBrokerWebPort();
        this.topicProps =
                new TopicPropGroup(bdbEntity.getNumTopicStores(), bdbEntity.getDftNumPartitions(),
                        bdbEntity.getDftUnflushThreshold(), bdbEntity.getDftUnflushInterval(),
                        bdbEntity.getDftUnFlushDataHold(), bdbEntity.getDftMemCacheMsgSizeInMB(),
                        bdbEntity.getDftMemCacheMsgCntInK(), bdbEntity.getDftMemCacheFlushIntvl(),
                        bdbEntity.isAcceptPublish(), bdbEntity.isAcceptSubscribe(),
                        bdbEntity.getDftDeletePolicy(), bdbEntity.getDataStoreType(),
                        bdbEntity.getDataPath());
        this.manageStatus = ManageStatus.valueOf(bdbEntity.getManageStatus());
        this.isConfDataUpdated = bdbEntity.isConfDataUpdated();
        this.isBrokerLoaded = bdbEntity.isBrokerLoaded();
        setAttributes(bdbEntity.getAttributes());
    }

    // build bdb object from current info
    public BdbBrokerConfEntity buildBdbBrokerConfEntity() {
        BdbBrokerConfEntity bdbEntity = new BdbBrokerConfEntity(brokerId, brokerIp, brokerPort,
                topicProps.getNumPartitions(), topicProps.getUnflushThreshold(),
                topicProps.getUnflushInterval(), "", topicProps.getDeletePolicy(),
                manageStatus.getCode(), topicProps.isAcceptPublish(),
                topicProps.isAcceptSubscribe(), getAttributes(), isConfDataUpdated,
                isBrokerLoaded, getCreateUser(), getCreateDate(),
                getModifyUser(), getModifyDate());
        bdbEntity.setDataVerId(getDataVersionId());
        bdbEntity.setRegionId(regionId);
        bdbEntity.setBrokerGroupId(groupId);
        bdbEntity.setBrokerTLSPort(brokerTLSPort);
        bdbEntity.setBrokerWebPort(brokerWebPort);
        bdbEntity.setNumTopicStores(topicProps.getNumTopicStores());
        bdbEntity.setDftMemCacheMsgSizeInMB(topicProps.getMemCacheMsgSizeInMB());
        bdbEntity.setDftMemCacheMsgCntInK(topicProps.getMemCacheMsgCntInK());
        bdbEntity.setDftMemCacheFlushIntvl(topicProps.getMemCacheFlushIntvl());
        bdbEntity.setDftUnFlushDataHold(topicProps.getUnflushDataHold());
        bdbEntity.setDataStore(topicProps.getDataStoreType(), topicProps.getDataPath());
        return bdbEntity;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(int brokerId) {
        this.brokerId = brokerId;
    }

    public ManageStatus getManageStatus() {
        return manageStatus;
    }



    public void setManageStatus(ManageStatus manageStatus) {
        this.manageStatus = manageStatus;
    }

    public void setConfDataUpdated() {
        this.isBrokerLoaded = false;
        this.isConfDataUpdated = true;
    }

    public void setBrokerLoaded() {
        this.isBrokerLoaded = true;
        this.isConfDataUpdated = false;
    }

    public boolean isConfDataUpdated() {
        return this.isConfDataUpdated;
    }

    public boolean isBrokerLoaded() {
        return this.isBrokerLoaded;
    }

    public void setBrokerIpAndPort(String brokerIp, int brokerPort) {
        this.brokerPort = brokerPort;
        this.brokerIp = brokerIp;
        this.buildStrInfo();
    }

    public String getBrokerAddress() {
        return brokerAddress;
    }

    public String getBrokerIp() {
        return brokerIp;
    }

    public int getBrokerPort() {
        return brokerPort;
    }

    public void setBrokerIpAndAllPort(int brokerId, String brokerIp,
                                      int brokerPort, int brokerTLSPort) {
        this.brokerId = brokerId;
        this.brokerIp = brokerIp;
        this.brokerPort = brokerPort;
        this.brokerTLSPort = brokerTLSPort;
        this.buildStrInfo();
    }

    public int getBrokerWebPort() {
        return brokerWebPort;
    }

    public void setBrokerWebPort(int brokerWebPort) {
        this.brokerWebPort = brokerWebPort;
    }

    public int getBrokerTLSPort() {
        return brokerTLSPort;
    }

    public String getBrokerIdAndAddress() {
        return brokerFullInfo;
    }

    public String getSimpleBrokerInfo() {
        if (this.brokerPort == TBaseConstants.META_DEFAULT_BROKER_PORT) {
            return this.brokerSimpleInfo;
        } else {
            return this.brokerFullInfo;
        }
    }

    public String getSimpleTLSBrokerInfo() {
        if (getBrokerTLSPort() == TBaseConstants.META_DEFAULT_BROKER_PORT) {
            return this.brokerTLSSimpleInfo;
        } else {
            return this.brokerTLSFullInfo;
        }
    }

    public String getBrokerTLSFullInfo() {
        return brokerTLSFullInfo;
    }

    public int getRegionId() {
        return regionId;
    }

    public void setRegionId(int regionId) {
        this.regionId = regionId;
    }

    public int getGroupId() {
        return groupId;
    }

    public void setGroupId(int groupId) {
        this.groupId = groupId;
    }

    public TopicPropGroup getTopicProps() {
        return topicProps;
    }

    public void setTopicProps(TopicPropGroup topicProps) {
        this.topicProps = topicProps;
    }

    private void buildStrInfo() {
        StringBuilder sBuilder = new StringBuilder(512);
        this.brokerAddress = sBuilder.append(this.brokerIp)
                .append(TokenConstants.ATTR_SEP)
                .append(this.brokerPort).toString();
        sBuilder.delete(0, sBuilder.length());
        this.brokerSimpleInfo = sBuilder.append(this.brokerId)
                .append(TokenConstants.ATTR_SEP).append(this.brokerIp)
                .append(TokenConstants.ATTR_SEP).append(" ").toString();
        sBuilder.delete(0, sBuilder.length());
        this.brokerFullInfo = sBuilder.append(this.brokerId)
                .append(TokenConstants.ATTR_SEP).append(this.brokerIp)
                .append(TokenConstants.ATTR_SEP).append(this.brokerPort).toString();
        sBuilder.delete(0, sBuilder.length());
        this.brokerTLSSimpleInfo = sBuilder.append(this.brokerId)
                .append(TokenConstants.ATTR_SEP).append(this.brokerIp)
                .append(TokenConstants.ATTR_SEP).append(" ").toString();
        sBuilder.delete(0, sBuilder.length());
        this.brokerTLSFullInfo = sBuilder.append(this.brokerId)
                .append(TokenConstants.ATTR_SEP).append(this.brokerIp)
                .append(TokenConstants.ATTR_SEP).append(brokerTLSPort).toString();
    }

    /**
     * Check whether the specified query item value matches
     * Allowed query items:
     *   brokerId, brokerIp, brokerPort, brokerTLSPort, regionId, groupId
     *   manageStatus, brokerWebPort
     * @return true: matched, false: not match
     */
    public boolean isMatched(BrokerConfEntity target) {
        if (target == null) {
            return true;
        }
        if (!super.isMatched(target)) {
            return false;
        }
        if ((target.getBrokerId() != TBaseConstants.META_VALUE_UNDEFINED
                && target.getBrokerId() != this.brokerId)
                || (TStringUtils.isNotBlank(target.getBrokerIp())
                && !target.getBrokerIp().equals(this.brokerIp))
                || (target.getBrokerPort() != TBaseConstants.META_VALUE_UNDEFINED
                && target.getBrokerPort() != this.brokerPort)
                || (target.getBrokerTLSPort() != TBaseConstants.META_VALUE_UNDEFINED
                && target.getBrokerTLSPort() != this.brokerTLSPort)
                || (target.getRegionId() != TBaseConstants.META_VALUE_UNDEFINED
                && target.getRegionId() != this.regionId)
                || (target.getGroupId() != TBaseConstants.META_VALUE_UNDEFINED
                && target.getGroupId() != this.groupId)
                || (target.getManageStatus() != ManageStatus.STATUS_MANAGE_UNDEFINED
                && target.getManageStatus() != this.manageStatus)
                || (target.getBrokerWebPort() != TBaseConstants.META_VALUE_UNDEFINED
                && target.getBrokerWebPort() != this.brokerWebPort)) {
            return false;
        }
        return true;
    }

    /**
     * Get broker config string
     *
     * @return config string
     */
    public String getBrokerDefaultConfInfo() {
        return new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                .append(topicProps.getNumPartitions()).append(TokenConstants.ATTR_SEP)
                .append(topicProps.isAcceptPublish()).append(TokenConstants.ATTR_SEP)
                .append(topicProps.isAcceptSubscribe()).append(TokenConstants.ATTR_SEP)
                .append(topicProps.getUnflushThreshold()).append(TokenConstants.ATTR_SEP)
                .append(topicProps.getUnflushInterval()).append(TokenConstants.ATTR_SEP)
                .append(" ").append(TokenConstants.ATTR_SEP)
                .append(topicProps.getDeletePolicy()).append(TokenConstants.ATTR_SEP)
                .append(topicProps.getNumTopicStores()).append(TokenConstants.ATTR_SEP)
                .append(topicProps.getUnflushDataHold()).append(TokenConstants.ATTR_SEP)
                .append(topicProps.getMemCacheMsgSizeInMB()).append(TokenConstants.ATTR_SEP)
                .append(topicProps.getMemCacheMsgCntInK()).append(TokenConstants.ATTR_SEP)
                .append(topicProps.getMemCacheFlushIntvl()).toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BrokerConfEntity)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        BrokerConfEntity entity = (BrokerConfEntity) o;
        return brokerId == entity.brokerId &&
                brokerPort == entity.brokerPort &&
                brokerTLSPort == entity.brokerTLSPort &&
                brokerWebPort == entity.brokerWebPort &&
                isConfDataUpdated == entity.isConfDataUpdated &&
                isBrokerLoaded == entity.isBrokerLoaded &&
                regionId == entity.regionId &&
                groupId == entity.groupId &&
                Objects.equals(brokerIp, entity.brokerIp) &&
                manageStatus == entity.manageStatus &&
                Objects.equals(topicProps, entity.topicProps) &&
                Objects.equals(brokerAddress, entity.brokerAddress) &&
                Objects.equals(brokerFullInfo, entity.brokerFullInfo) &&
                Objects.equals(brokerSimpleInfo, entity.brokerSimpleInfo) &&
                Objects.equals(brokerTLSSimpleInfo, entity.brokerTLSSimpleInfo) &&
                Objects.equals(brokerTLSFullInfo, entity.brokerTLSFullInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), brokerId, brokerIp, brokerPort, brokerTLSPort,
                brokerWebPort, manageStatus, isConfDataUpdated, isBrokerLoaded, regionId,
                groupId, topicProps, brokerAddress, brokerFullInfo, brokerSimpleInfo,
                brokerTLSSimpleInfo, brokerTLSFullInfo);
    }

}
