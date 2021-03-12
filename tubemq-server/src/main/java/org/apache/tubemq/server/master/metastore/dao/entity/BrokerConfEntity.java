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

package org.apache.tubemq.server.master.metastore.dao.entity;

import java.util.Date;
import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.corebase.TokenConstants;
import org.apache.tubemq.server.common.statusdef.ManageStatus;



/*
 * store the broker default setting
 *
 */

public class BrokerConfEntity extends BaseEntity  {
    private int brokerId = TBaseConstants.META_VALUE_UNDEFINED;
    private String brokerIp = "";
    private int brokerPort = TBaseConstants.META_VALUE_UNDEFINED;
    //broker tls port
    private int brokerTLSPort = TBaseConstants.META_VALUE_UNDEFINED;
    private String brokerAddress = "";       // broker ip:port
    private String brokerFullInfo = "";      // broker brokerId:ip:port
    private String brokerSimpleInfo = "";    // broker brokerId:ip:
    private String brokerTLSSimpleInfo = ""; //tls simple info
    private String brokerTLSFullInfo = "";   //tls full info
    private int regionId = TBaseConstants.META_VALUE_UNDEFINED;
    private int groupId = TBaseConstants.META_VALUE_UNDEFINED;
    private ManageStatus manageStatus = ManageStatus.STATUS_MANAGE_UNDEFINED;
    private boolean isConfDataUpdated = false;  //conf data update flag
    private boolean isBrokerLoaded = false;     //broker conf load flag
    private TopicPropGroup defTopicPropGroup = null;


    public BrokerConfEntity() {
        super();
    }


    public BrokerConfEntity(int brokerId, String brokerIp, int brokerPort,
                            int brokerTLSPort, ManageStatus manageStatus,
                            boolean isConfDataUpdated, boolean isBrokerLoaded,
                            TopicPropGroup defTopicPropGroup,
                            String createUser, Date createDate,
                            String modifyUser, Date modifyDate) {
        super(createUser, createDate, modifyUser, modifyDate);
        setBrokerIpAndAllPort(brokerId, brokerIp, brokerPort, brokerTLSPort);
        this.manageStatus = manageStatus;
        this.isConfDataUpdated = isConfDataUpdated;
        this.isBrokerLoaded = isBrokerLoaded;
        this.defTopicPropGroup = defTopicPropGroup;
        this.buildStrInfo();
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

    public TopicPropGroup getDefTopicPropGroup() {
        return defTopicPropGroup;
    }

    public void setDefTopicPropGroup(TopicPropGroup defTopicPropGroup) {
        this.defTopicPropGroup = defTopicPropGroup;
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

}
