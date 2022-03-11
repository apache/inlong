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

import com.github.pagehelper.PageInfo;
import org.apache.inlong.common.pojo.dataproxy.DataProxyConfig;
import org.apache.inlong.common.pojo.dataproxy.ThirdPartyClusterDTO;
import org.apache.inlong.manager.common.pojo.cluster.ClusterPageRequest;
import org.apache.inlong.manager.common.pojo.cluster.ClusterRequest;
import org.apache.inlong.manager.common.pojo.cluster.ClusterResponse;
import org.apache.inlong.manager.common.pojo.dataproxy.DataProxyRequest;
import org.apache.inlong.manager.common.pojo.dataproxy.DataProxyResponse;

import java.util.List;

/**
 * The third party cluster (such as kafka) information service layer interface
 */
public interface ThirdPartyClusterService {


    /**
     * Save cluster info.
     *
     * @param request Cluster info.
     * @param operator Current operator.
     * @return ID after saving.
     */
    Integer save(ClusterRequest request, String operator);

    /**
     * Get cluster info by id.
     *
     * @param id Cluster ID.
     * @return Cluster info.
     */
    ClusterResponse get(Integer id);

    /**
     * Paging query clusters according to conditions.
     *
     * @param request Query conditions.
     * @return Cluster list.
     */
    PageInfo<ClusterResponse> list(ClusterPageRequest request);

    List<String> listClusterIpByType(String type);

    /**
     * Change cluster information
     *
     * @param request The cluster info to be modified
     * @param operator Current operator
     * @return Whether succeed
     */
    Boolean update(ClusterRequest request, String operator);

    /**
     * Delete cluster information
     *
     * @param id Cluster ID to be deleted
     * @param operator Current operator
     * @return Whether succeed
     */
    Boolean delete(Integer id, String operator);

    /**
     * Query data proxy ip list
     *
     * @param request query request param
     * @return data proxy ip list
     */
    List<DataProxyResponse> getIpList(DataProxyRequest request);

    /**
     * query data proxy config by cluster id
     *
     * @return data proxy config
     */
    List<DataProxyConfig> getConfig();

    /**
     * query data proxy config by cluster id, result includes pulsar cluster configs and topic etc
     */
    ThirdPartyClusterDTO getConfigV2(String dataproxyClusterName);

}
