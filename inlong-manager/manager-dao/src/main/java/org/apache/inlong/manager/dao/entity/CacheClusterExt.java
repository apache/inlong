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
 * CacheClusterExt
 */
public class CacheClusterExt {

    private String clusterName;
    private String keyName;
    private String keyValue;
    private Integer isDeleted;

    /**
     * get clusterName
     * 
     * @return the clusterName
     */
    public String getClusterName() {
        return clusterName;
    }

    /**
     * set clusterName
     * 
     * @param clusterName the clusterName to set
     */
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    /**
     * get keyName
     * 
     * @return the keyName
     */
    public String getKeyName() {
        return keyName;
    }

    /**
     * set keyName
     * 
     * @param keyName the keyName to set
     */
    public void setKeyName(String keyName) {
        this.keyName = keyName;
    }

    /**
     * get keyValue
     * 
     * @return the keyValue
     */
    public String getKeyValue() {
        return keyValue;
    }

    /**
     * set keyValue
     * 
     * @param keyValue the keyValue to set
     */
    public void setKeyValue(String keyValue) {
        this.keyValue = keyValue;
    }

    /**
     * getJIsDeleted
     * 
     * @return
     */
    public Integer getJIsDeleted() {
        return isDeleted;
    }

    /**
     * setIsDeleted
     * 
     * @param isDeleted
     */
    public void setIsDeleted(Integer isDeleted) {
        this.isDeleted = isDeleted;
    }

}
