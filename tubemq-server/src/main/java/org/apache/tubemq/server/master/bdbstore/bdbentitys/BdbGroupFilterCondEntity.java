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
import org.apache.tubemq.corebase.TokenConstants;
import org.apache.tubemq.corebase.utils.TStringUtils;
import org.apache.tubemq.server.common.utils.WebParameterUtils;
import org.apache.tubemq.server.master.metastore.TStoreConstants;


@Entity
public class BdbGroupFilterCondEntity implements Serializable {
    private static final long serialVersionUID = 5305233169489425210L;

    @PrimaryKey
    private String recordKey;
    private String topicName;
    private String consumerGroupName;
    private int controlStatus = -2;   // -2: undefine; 0: not started; 1:started, not limited; 2: started, limited
    private String attributes;
    private String createUser;
    private Date createDate;

    public BdbGroupFilterCondEntity() {

    }

    public BdbGroupFilterCondEntity(String topicName, String consumerGroupName,
                                    int controlStatus, String filterCondStr,
                                    String createUser, Date createDate) {
        this.recordKey =
                new StringBuilder(512)
                        .append(topicName)
                        .append(TokenConstants.ATTR_SEP)
                        .append(consumerGroupName).toString();
        this.topicName = topicName;
        this.consumerGroupName = consumerGroupName;
        this.controlStatus = controlStatus;
        setFilterCondStr(filterCondStr);
        this.createUser = createUser;
        this.createDate = createDate;
    }

    public BdbGroupFilterCondEntity(String topicName, String consumerGroupName,
                                    int controlStatus, String filterCondStr,
                                    String attributes, String createUser, Date createDate) {
        this.recordKey =
                new StringBuilder(512)
                        .append(topicName)
                        .append(TokenConstants.ATTR_SEP)
                        .append(consumerGroupName).toString();
        this.topicName = topicName;
        this.consumerGroupName = consumerGroupName;
        this.controlStatus = controlStatus;
        this.createUser = createUser;
        this.createDate = createDate;
        this.attributes = attributes;
        setFilterCondStr(filterCondStr);
    }

    public String getFilterCondStr() {
        if (TStringUtils.isNotBlank(attributes)
                && attributes.contains(TokenConstants.EQ)) {
            return TStringUtils.getAttrValFrmAttributes(
                    this.attributes, TStoreConstants.TOKEN_FILTER_COND_STR);
        } else {
            return attributes;
        }
    }

    public void setFilterCondStr(String filterCondStr) {
        this.attributes =
                TStringUtils.setAttrValToAttributes(this.attributes,
                        TStoreConstants.TOKEN_FILTER_COND_STR, filterCondStr);
    }

    public String getRecordKey() {
        return recordKey;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public String getConsumerGroupName() {
        return consumerGroupName;
    }

    public void setConsumerGroupName(String consumerGroupName) {
        this.consumerGroupName = consumerGroupName;
    }

    public int getControlStatus() {
        return controlStatus;
    }

    public void setControlStatus(int controlStatus) {
        this.controlStatus = controlStatus;
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

    public String getAttributes() {
        if (TStringUtils.isNotBlank(attributes)
                && !attributes.contains(TokenConstants.EQ)) {
            return attributes;
        } else {
            return "";
        }
    }

    public void setAttributes(String attributes) {
        this.attributes = attributes;
    }

    public long getDataVerId() {
        if (TStringUtils.isNotBlank(attributes)
                && attributes.contains(TokenConstants.EQ)) {
            String atrVal =
                    TStringUtils.getAttrValFrmAttributes(this.attributes,
                            TStoreConstants.TOKEN_DATA_VERSION_ID);
            if (atrVal != null) {
                return Long.parseLong(atrVal);
            }
        }
        return TBaseConstants.META_VALUE_UNDEFINED;
    }

    public void setDataVerId(long dataVerId) {
        if (TStringUtils.isNotBlank(attributes)
                && !attributes.contains(TokenConstants.EQ)) {
            setFilterCondStr(attributes);
        }
        this.attributes =
                TStringUtils.setAttrValToAttributes(this.attributes,
                        TStoreConstants.TOKEN_DATA_VERSION_ID,
                        String.valueOf(dataVerId));
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("recordKey", recordKey)
                .append("topicName", topicName)
                .append("consumerGroupName", consumerGroupName)
                .append("controlStatus", controlStatus)
                .append("attributes", attributes)
                .append("createUser", createUser)
                .append("createDate", createDate)
                .toString();
    }

    public StringBuilder toJsonString(final StringBuilder sBuilder) {
        return sBuilder.append("{\"type\":\"BdbGroupFilterCondEntity\",")
                .append("\"recordKey\":\"").append(recordKey)
                .append("\",\"topicName\":\"").append(topicName)
                .append("\",\"consumerGroupName\":\"").append(consumerGroupName)
                .append("\",\"filterConds\":\"").append(attributes)
                .append("\",\"condStatus\":").append(controlStatus)
                .append(",\"createUser\":\"").append(createUser)
                .append("\",\"createDate\":\"")
                .append(WebParameterUtils.date2yyyyMMddHHmmss(createDate))
                .append("\"}");
    }
}
