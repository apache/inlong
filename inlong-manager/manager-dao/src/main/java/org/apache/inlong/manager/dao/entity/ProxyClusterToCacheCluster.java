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
 * ProxyClusterToCacheCluster
 */
public class ProxyClusterToCacheCluster {
    private String proxyClusterName;
    private String cacheClusterName;

    /**
     * get proxyClusterName
     * 
     * @return the proxyClusterName
     */
    public String getProxyClusterName() {
        return proxyClusterName;
    }

    /**
     * set proxyClusterName
     * 
     * @param proxyClusterName the proxyClusterName to set
     */
    public void setProxyClusterName(String proxyClusterName) {
        this.proxyClusterName = proxyClusterName;
    }

    /**
     * get cacheClusterName
     * 
     * @return the cacheClusterName
     */
    public String getCacheClusterName() {
        return cacheClusterName;
    }

    /**
     * set cacheClusterName
     * 
     * @param cacheClusterName the cacheClusterName to set
     */
    public void setCacheClusterName(String cacheClusterName) {
        this.cacheClusterName = cacheClusterName;
    }

}
