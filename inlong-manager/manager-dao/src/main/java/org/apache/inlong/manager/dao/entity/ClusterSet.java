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

package org.apache.inlong.manager.dao.entity;

/**
 * ClusterSet
 */
public class ClusterSet {
    private String setName;
    private String cnName;
    private String description;
    private String middlewareType;
    private String inCharges;
    private String followers;
    private int status;
    private int isDeleted;
    private String creator;
    private String modifier;

    /**
     * get setName
     * 
     * @return the setName
     */
    public String getSetName() {
        return setName;
    }

    /**
     * set setName
     * 
     * @param setName the setName to set
     */
    public void setSetName(String setName) {
        this.setName = setName;
    }

    /**
     * get cnName
     * 
     * @return the cnName
     */
    public String getCnName() {
        return cnName;
    }

    /**
     * set cnName
     * 
     * @param cnName the cnName to set
     */
    public void setCnName(String cnName) {
        this.cnName = cnName;
    }

    /**
     * get description
     * 
     * @return the description
     */
    public String getDescription() {
        return description;
    }

    /**
     * set description
     * 
     * @param description the description to set
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * get middlewareType
     * 
     * @return the middlewareType
     */
    public String getMiddlewareType() {
        return middlewareType;
    }

    /**
     * set middlewareType
     * 
     * @param middlewareType the middlewareType to set
     */
    public void setMiddlewareType(String middlewareType) {
        this.middlewareType = middlewareType;
    }

    /**
     * get inCharges
     * 
     * @return the inCharges
     */
    public String getInCharges() {
        return inCharges;
    }

    /**
     * set inCharges
     * 
     * @param inCharges the inCharges to set
     */
    public void setInCharges(String inCharges) {
        this.inCharges = inCharges;
    }

    /**
     * get followers
     * 
     * @return the followers
     */
    public String getFollowers() {
        return followers;
    }

    /**
     * set followers
     * 
     * @param followers the followers to set
     */
    public void setFollowers(String followers) {
        this.followers = followers;
    }

    /**
     * get status
     * 
     * @return the status
     */
    public int getStatus() {
        return status;
    }

    /**
     * set status
     * 
     * @param status the status to set
     */
    public void setStatus(int status) {
        this.status = status;
    }

    /**
     * get isDeleted
     * 
     * @return the isDeleted
     */
    public int getIsDeleted() {
        return isDeleted;
    }

    /**
     * set isDeleted
     * 
     * @param isDeleted the isDeleted to set
     */
    public void setIsDeleted(int isDeleted) {
        this.isDeleted = isDeleted;
    }

    /**
     * get creator
     * 
     * @return the creator
     */
    public String getCreator() {
        return creator;
    }

    /**
     * set creator
     * 
     * @param creator the creator to set
     */
    public void setCreator(String creator) {
        this.creator = creator;
    }

    /**
     * get modifier
     * 
     * @return the modifier
     */
    public String getModifier() {
        return modifier;
    }

    /**
     * set modifier
     * 
     * @param modifier the modifier to set
     */
    public void setModifier(String modifier) {
        this.modifier = modifier;
    }

}
