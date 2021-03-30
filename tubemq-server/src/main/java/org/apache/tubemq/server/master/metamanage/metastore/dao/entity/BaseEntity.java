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

import com.google.gson.Gson;
import java.io.Serializable;
import java.util.Date;
import java.util.Objects;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.corebase.utils.TStringUtils;
import org.apache.tubemq.server.common.TServerConstants;
import org.apache.tubemq.server.common.utils.WebParameterUtils;


// AbstractEntity: entity's abstract class
public class BaseEntity implements Serializable, Cloneable {

    private long dataVersionId =
            TServerConstants.DEFAULT_DATA_VERSION;    // 0: default version， other: version
    private String createUser = "";      //create user
    private Date createDate = null;        //create date
    private String modifyUser = "";      //modify user
    private Date modifyDate = null;        //modify date
    private String attributes = "";        //attribute info


    public BaseEntity() {

    }

    public BaseEntity(long dataVersionId) {
        this.dataVersionId = dataVersionId;
    }

    public BaseEntity(String createUser, Date createDate) {
        this(TServerConstants.DEFAULT_DATA_VERSION,
                createUser, createDate, createUser, createDate);
    }

    public BaseEntity(long dataVersionId, String createUser, Date createDate) {
        this(dataVersionId, createUser, createDate, createUser, createDate);
    }

    public BaseEntity(String createUser, Date createDate,
                      String modifyUser, Date modifyDate) {
        this(TServerConstants.DEFAULT_DATA_VERSION,
                createUser, createDate, modifyUser, modifyDate);
    }

    public BaseEntity(long dataVersionId,
                      String createUser, Date createDate,
                      String modifyUser, Date modifyDate) {
        this.dataVersionId = dataVersionId;
        this.createUser = createUser;
        this.createDate = createDate;
        this.modifyUser = modifyUser;
        this.modifyDate = modifyDate;
    }

    public void setModifyInfo(long dataVersionId, boolean isAdd,
                               String operator, Date opDate) {
        this.dataVersionId = dataVersionId;
        if (isAdd) {
            this.createUser = operator;
            this.createDate = opDate;
        }
        this.modifyUser = operator;
        this.modifyDate = opDate;
    }

    public void setDataVersionId() {
        setDataVersionId(System.currentTimeMillis());
    }

    public void setDataVersionId(long dataVersionId) {
        this.dataVersionId = dataVersionId;
    }

    public void setKeyAndVal(String key, String value) {
        this.attributes =
                TStringUtils.setAttrValToAttributes(this.attributes, key, value);
    }

    public String getValueByKey(String key) {
        return TStringUtils.getAttrValFrmAttributes(this.attributes, key);
    }

    public String getAttributes() {
        return attributes;
    }

    public void setAttributes(String attributes) {
        this.attributes = attributes;
    }

    public String getCreateUser() {
        return createUser;
    }

    public Date getCreateDate() {
        return createDate;
    }

    public long getDataVersionId() {
        return dataVersionId;
    }

    public String getModifyUser() {
        return modifyUser;
    }

    public Date getModifyDate() {
        return modifyDate;
    }

    public String toJsonString(Gson gson) {
        return gson.toJson(this);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    /**
     * Check whether the specified query item value matches
     * Allowed query items:
     *   dataVersionId, createUser, modifyUser
     * @return true: matched, false: not match
     */
    public boolean isMatched(BaseEntity target) {
        if (target == null) {
            return true;
        }
        if ((target.getDataVersionId() != TBaseConstants.META_VALUE_UNDEFINED
                && this.getDataVersionId() != target.getDataVersionId())
                || (TStringUtils.isNotBlank(target.getCreateUser())
                && !target.getCreateUser().equals(createUser))
                || (TStringUtils.isNotBlank(target.getModifyUser())
                && !target.getModifyUser().equals(modifyUser))) {
            return false;
        }
        return true;
    }

    /**
     * Serialize field to json format
     *
     * @param sBuilder   build container
     * @param isLongName if return field key is long name
     * @return
     */
    StringBuilder toWebJsonStr(StringBuilder sBuilder, boolean isLongName) {
        if (isLongName) {
            sBuilder.append(",\"dataVersionId\":").append(dataVersionId)
                    .append(",\"createUser\":\"").append(createUser).append("\"")
                    .append(",\"createDate\":\"")
                    .append(WebParameterUtils.date2yyyyMMddHHmmss(createDate)).append("\"")
                    .append(",\"modifyUser\":\"").append(modifyUser).append("\"")
                    .append(",\"modifyDate\":\"")
                    .append(WebParameterUtils.date2yyyyMMddHHmmss(modifyDate)).append("\"")
                    .append(",\"attributes\":\"").append(attributes).append("\"");
        } else {
            sBuilder.append(",\"dVerId\":").append(dataVersionId)
                    .append(",\"cur\":\"").append(createUser).append("\"")
                    .append(",\"cDate\":\"")
                    .append(WebParameterUtils.date2yyyyMMddHHmmss(createDate)).append("\"")
                    .append(",\"mur\":\"").append(modifyUser).append("\"")
                    .append(",\"mDate\":\"")
                    .append(WebParameterUtils.date2yyyyMMddHHmmss(modifyDate)).append("\"")
                    .append(",\"attrs\":\"").append(attributes).append("\"");
        }
        return sBuilder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BaseEntity)) {
            return false;
        }
        BaseEntity that = (BaseEntity) o;
        return dataVersionId == that.dataVersionId &&
                Objects.equals(createUser, that.createUser) &&
                Objects.equals(createDate, that.createDate) &&
                Objects.equals(modifyUser, that.modifyUser) &&
                Objects.equals(modifyDate, that.modifyDate) &&
                Objects.equals(attributes, that.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataVersionId, createUser,
                createDate, modifyUser, modifyDate, attributes);
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
