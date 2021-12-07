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

package org.apache.inlong.sort.standalone.config.pojo;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * SortClusterConfig
 */
public class SortClusterConfig {

    private String clusterName;
    private List<SortTaskConfig> sortTasks = new ArrayList<>();

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
     * get sortTasks
     * 
     * @return the sortTasks
     */
    public List<SortTaskConfig> getSortTasks() {
        return sortTasks;
    }

    /**
     * set sortTasks
     * 
     * @param sortTasks the sortTasks to set
     */
    public void setSortTasks(List<SortTaskConfig> sortTasks) {
        this.sortTasks = sortTasks;
    }

}
