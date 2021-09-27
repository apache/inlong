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
 * CacheCluster
 */
public class CacheCluster {
    // `cluster_name` varchar(128) NOT NULL COMMENT 'CacheCluster name, English,
    // numbers and underscore',
    private String clusterName;
    // `set_name` varchar(128) NOT NULL COMMENT 'ClusterSet name, English, numbers
    // and underscore',
    private String setName;
    // `zone` varchar(128) NOT NULL COMMENT 'Zone, sz/sh/tj',
    private String zone;

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
     * get zone
     * 
     * @return the zone
     */
    public String getZone() {
        return zone;
    }

    /**
     * set zone
     * 
     * @param zone the zone to set
     */
    public void setZone(String zone) {
        this.zone = zone;
    }

}
