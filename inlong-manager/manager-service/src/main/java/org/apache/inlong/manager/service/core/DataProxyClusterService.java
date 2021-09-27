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
import java.util.List;
import org.apache.inlong.manager.common.pojo.cluster.DataProxyClusterInfo;
import org.apache.inlong.manager.common.pojo.cluster.DataProxyClusterPageRequest;
import org.apache.inlong.manager.common.pojo.dataproxy.DataProxyIpRequest;
import org.apache.inlong.manager.common.pojo.dataproxy.DataProxyIpResponse;
import org.apache.inlong.manager.dao.entity.DataProxyConfig;

/**
 * DataProxy cluster service layer interface
 */
public interface DataProxyClusterService {

    /**
     * Save DataProxy cluster information
     *
     * @param clusterInfo Cluster information
     * @param operator    Current operator
     * @return ID after saving
     */
    Integer save(DataProxyClusterInfo clusterInfo, String operator);

    /**
     * Save DataProxy cluster information
     *
     * @param id Cluster ID
     * @return Cluster information succeed
     */
    DataProxyClusterInfo get(Integer id);

    /**
     * Query DataProxy cluster list according to conditions
     *
     * @param request Query conditions
     * @return DataProxy cluster list
     */
    PageInfo<DataProxyClusterInfo> listByCondition(DataProxyClusterPageRequest request);

    /**
     * Change DataProxy cluster information
     *
     * @param clusterInfo The information to be modified
     * @param operator    Current operator
     * @return Whether succeed
     */
    Boolean update(DataProxyClusterInfo clusterInfo, String operator);

    /**
     * Delete DataProxy cluster information
     *
     * @param id       Cluster ID to be deleted
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
    List<DataProxyIpResponse> getIpList(DataProxyIpRequest request);

    /**
     * query data proxy config by cluster id
     *
     * @return data proxy config
     */
    List<DataProxyConfig> getConfig();

    /**
     * query data proxy config by cluster id
     * 
     * @param clusterName
     * @param setName
     * @param md5
     * @return data proxy config
     */
    String getAllConfig(String clusterName, String setName, String md5);
}
