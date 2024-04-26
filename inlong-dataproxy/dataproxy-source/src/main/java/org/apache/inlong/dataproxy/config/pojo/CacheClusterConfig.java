/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.dataproxy.config.pojo;

import org.apache.commons.lang.builder.ToStringBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 
 * CacheClusterConfig
 */
public class CacheClusterConfig {

    private String clusterName;
    private String token;
    private Map<String, String> params = new HashMap<>();

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
     * get token
     *
     * @return the token
     */
    public String getToken() {
        return token;
    }

    /**
     * set token
     *
     * @param token the token to set
     */
    public void setToken(String token) {
        this.token = token;
    }

    /**
     * get params
     * 
     * @return the params
     */
    public Map<String, String> getParams() {
        return params;
    }

    /**
     * set params
     * 
     * @param params the params to set
     */
    public void setParams(Map<String, String> params) {
        this.params = params;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("clusterName", clusterName)
                .append("token", token)
                .append("params", params)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CacheClusterConfig)) {
            return false;
        }
        CacheClusterConfig that = (CacheClusterConfig) o;
        return Objects.equals(clusterName, that.clusterName)
                && Objects.equals(token, that.token)
                && Objects.equals(params, that.params);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterName, token, params);
    }
}
