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

package org.apache.inlong.sort.entity;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CacheZone implements Serializable {

    //inlong zoneName
    private String zoneName;
    //mq serviceUrl
    private String serviceUrl;
    private String authentication;
    private List<Topic> topics;
    private Map<String, String> cacheZoneProperties;
    // pulsar,kafka,tube
    private String zoneType;

    public String getZoneName() {
        return zoneName;
    }

    public void setZoneName(String zoneName) {
        this.zoneName = zoneName;
    }

    public String getServiceUrl() {
        return serviceUrl;
    }

    public void setServiceUrl(String serviceUrl) {
        this.serviceUrl = serviceUrl;
    }

    public String getAuthentication() {
        return authentication;
    }

    public void setAuthentication(String authentication) {
        this.authentication = authentication;
    }

    public Map<String, String> getCacheZoneProperties() {
        return cacheZoneProperties;
    }

    public void setCacheZoneProperties(Map<String, String> cacheZoneProperties) {
        this.cacheZoneProperties = cacheZoneProperties;
    }

    public List<Topic> getTopics() {
        return topics;
    }

    public void setTopics(List<Topic> topics) {
        this.topics = topics;
    }

    public String getZoneType() {
        return zoneType;
    }

    public void setZoneType(String zoneType) {
        this.zoneType = zoneType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CacheZone cacheZone = (CacheZone) o;
        return zoneName.equals(cacheZone.zoneName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(zoneName);
    }

    @Override
    public String toString() {
        return "CacheZone{"
                + "zoneName='" + zoneName
                + ", serviceUrl='" + serviceUrl
                + ", authentication='" + authentication
                + ", topics=" + topics
                + ", cacheZoneProperties=" + cacheZoneProperties
                + ", zoneType='" + zoneType
                + '}';
    }
}
