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
import java.util.Map;

public class CacheZoneConfig implements Serializable {

    private String sortClusterName;
    private String sortTaskId;
    private Map<String, CacheZone> cacheZones;

    public String getSortClusterName() {
        return sortClusterName;
    }

    public void setSortClusterName(String sortClusterName) {
        this.sortClusterName = sortClusterName;
    }

    public String getSortTaskId() {
        return sortTaskId;
    }

    public void setSortTaskId(String sortTaskId) {
        this.sortTaskId = sortTaskId;
    }

    public Map<String, CacheZone> getCacheZones() {
        return cacheZones;
    }

    public void setCacheZones(Map<String, CacheZone> cacheZones) {
        this.cacheZones = cacheZones;
    }

    @Override
    public String toString() {
        return "SortSourceConfig{"
                + "sortClusterName='" + sortClusterName + '\''
                + ", sortTaskId='" + sortTaskId + '\''
                + ", cacheZones=" + cacheZones
                + '}';
    }
}
