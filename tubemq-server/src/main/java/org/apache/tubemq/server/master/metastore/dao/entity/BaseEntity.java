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

import com.google.gson.Gson;
import java.io.Serializable;
import java.util.Date;
import org.apache.tubemq.corebase.utils.TStringUtils;
import org.apache.tubemq.server.common.TServerConstants;



// AbstractEntity: entity's abstract class
public class BaseEntity implements Serializable {

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

    public void setCreateUserInfo(String createUser, Date createDate) {
        this.createUser = createUser;
        this.createDate = createDate;
        this.modifyUser = createUser;
        this.modifyDate = createDate;
    }

    public void setModifyUserInfo(String modifyUser, Date modifyDate) {
        this.modifyUser = modifyUser;
        this.modifyDate = modifyDate;
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

}
