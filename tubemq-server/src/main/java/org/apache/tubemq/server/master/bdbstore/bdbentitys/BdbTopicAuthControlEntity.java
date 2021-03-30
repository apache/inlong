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

package org.apache.tubemq.server.master.bdbstore.bdbentitys;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import java.io.Serializable;
import java.util.Date;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.corebase.utils.TStringUtils;
import org.apache.tubemq.server.common.utils.WebParameterUtils;
import org.apache.tubemq.server.master.metamanage.metastore.TStoreConstants;


@Entity
public class BdbTopicAuthControlEntity implements Serializable {

    private static final long serialVersionUID = 7356175918639562340L;
    @PrimaryKey
    private String topicName;
    private int enableAuthControl = -1; // -1 : undefine； 0：disable， 1：enable
    private String createUser;
    private Date createDate;
    private String attributes;

    public BdbTopicAuthControlEntity() {

    }

    public BdbTopicAuthControlEntity(String topicName, boolean enableAuthControl,
                                     String createUser, Date createDate) {
        this.topicName = topicName;
        if (enableAuthControl) {
            this.enableAuthControl = 1;
        } else {
            this.enableAuthControl = 0;
        }
        this.createUser = createUser;
        this.createDate = createDate;
    }

    public BdbTopicAuthControlEntity(String topicName, boolean enableAuthControl,
                                     String attributes,  String createUser, Date createDate) {
        this.topicName = topicName;
        if (enableAuthControl) {
            this.enableAuthControl = 1;
        } else {
            this.enableAuthControl = 0;
        }
        this.attributes = attributes;
        this.createUser = createUser;
        this.createDate = createDate;
    }


    public String getAttributes() {
        return attributes;
    }

    public void setAttributes(String attributes) {
        this.attributes = attributes;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public boolean isEnableAuthControl() {
        return enableAuthControl == 1;
    }

    public int getEnableAuthControl() {
        return this.enableAuthControl;
    }

    public void setEnableAuthControl(boolean enableAuthControl) {
        if (enableAuthControl) {
            this.enableAuthControl = 1;
        } else {
            this.enableAuthControl = 0;
        }
    }

    public String getCreateUser() {
        return createUser;
    }

    public void setCreateUser(String createUser) {
        this.createUser = createUser;
    }

    public Date getCreateDate() {
        return createDate;
    }

    public void setCreateDate(Date createDate) {
        this.createDate = createDate;
    }

    public long getDataVerId() {
        String atrVal =
                TStringUtils.getAttrValFrmAttributes(this.attributes,
                        TStoreConstants.TOKEN_DATA_VERSION_ID);
        if (atrVal != null) {
            return Long.parseLong(atrVal);
        }
        return TBaseConstants.META_VALUE_UNDEFINED;
    }

    public void setDataVerId(long dataVerId) {
        this.attributes =
                TStringUtils.setAttrValToAttributes(this.attributes,
                        TStoreConstants.TOKEN_DATA_VERSION_ID,
                        String.valueOf(dataVerId));
    }

    public int getTopicId() {
        String atrVal =
                TStringUtils.getAttrValFrmAttributes(this.attributes,
                        TStoreConstants.TOKEN_TOPICNAME_ID);
        if (atrVal != null) {
            return Integer.parseInt(atrVal);
        }
        return TBaseConstants.META_VALUE_UNDEFINED;
    }

    public void setTopicId(int topicId) {
        this.attributes =
                TStringUtils.setAttrValToAttributes(this.attributes,
                        TStoreConstants.TOKEN_TOPICNAME_ID,
                        String.valueOf(topicId));
    }


    public int getMaxMsgSize() {
        String atrVal =
                TStringUtils.getAttrValFrmAttributes(this.attributes,
                        TStoreConstants.TOKEN_MAX_MSG_SIZE);
        if (atrVal != null) {
            return Integer.parseInt(atrVal);
        }
        return TBaseConstants.META_VALUE_UNDEFINED;
    }

    public void setMaxMsgSize(int maxMsgSize) {
        this.attributes =
                TStringUtils.setAttrValToAttributes(this.attributes,
                        TStoreConstants.TOKEN_MAX_MSG_SIZE,
                        String.valueOf(maxMsgSize));
    }

    public StringBuilder toJsonString(final StringBuilder sBuilder) {
        return sBuilder.append("{\"type\":\"BdbConsumerGroupEntity\",")
                .append("\"topicName\":\"").append(topicName)
                .append("\",\"enableAuthControl\":\"").append(enableAuthControl)
                .append("\",\"createUser\":\"").append(createUser)
                .append("\",\"createDate\":\"")
                .append(WebParameterUtils.date2yyyyMMddHHmmss(createDate))
                .append("\",\"attributes\":\"").append(attributes).append("\"}");
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("topicName", topicName)
                .append("enableAuthControl", enableAuthControl)
                .append("createUser", createUser)
                .append("createDate", createDate)
                .append("attributes", attributes)
                .toString();
    }
}
