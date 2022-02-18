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

package org.apache.inlong.manager.service.core;

import org.apache.inlong.manager.common.pojo.cluster.ClusterInfo;
import org.apache.inlong.manager.common.pojo.cluster.ClusterRequest;

import java.util.List;

/**
 * Cluster information service layer interface
 */
public interface ClusterInfoService {

    List<String> listClusterIpByType(String type);

    /**
     * Find cluster information based on type
     *
     * @param request Query conditions
     * @return Cluster information
     */
    List<ClusterInfo> list(ClusterRequest request);

    /**
     * Query cluster information according to the list of cluster IDs
     *
     * @param clusterIdList Cluster ID list
     * @return Cluster information
     */
    List<ClusterInfo> getClusterInfoByIdList(List<Integer> clusterIdList);

    /**
     * Save cluster information
     *
     * @param clusterInfo Cluster information
     * @param operator Current operator
     * @return ID after saving
     */
    Integer save(ClusterInfo clusterInfo, String operator);

    /**
     * Change cluster information
     *
     * @param clusterInfo The information to be modified
     * @param operator Current operator
     * @return Whether succeed
     */
    Boolean update(ClusterInfo clusterInfo, String operator);

    /**
     * Delete cluster information
     *
     * @param id       Cluster ID to be deleted
     * @param operator Current operator
     * @return Whether succeed
     */
    Boolean delete(Integer id, String operator);

    /**
     * Save cluster information
     *
     * @param id Cluster ID
     * @return Cluster information succeed
     */
    ClusterInfo get(Integer id);
}
