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

package org.apache.inlong.manager.client.api.impl;

import org.apache.inlong.manager.client.api.InlongCluster;
import org.apache.inlong.manager.client.api.inner.InnerInlongManagerClient;
import org.apache.inlong.manager.common.pojo.cluster.ClusterInfo;
import org.apache.inlong.manager.common.pojo.cluster.ClusterNodeResponse;
import org.apache.inlong.manager.common.pojo.cluster.ClusterPageRequest;
import org.apache.inlong.manager.common.pojo.cluster.ClusterRequest;

import java.util.List;

public class InlongClusterImpl implements InlongCluster {

    private InnerInlongManagerClient managerClient;

    public InlongClusterImpl(InlongClientImpl inlongClient) {
        if (this.managerClient == null) {
            this.managerClient = new InnerInlongManagerClient(inlongClient.getConfiguration());
        }
    }

    @Override
    public List<ClusterNodeResponse> listNodes(String clusterName, String clusterType) {
        ClusterPageRequest request = new ClusterPageRequest();
        request.setName(clusterName);
        request.setType(clusterType);
        return managerClient.listClusterNodes(request);
    }

    @Override
    public List<ClusterNodeResponse> listNodes(String clusterName, String clusterType, List<String> clusterTags) {
        ClusterPageRequest request = new ClusterPageRequest();
        request.setName(clusterName);
        request.setType(clusterType);
        request.setClusterTagList(clusterTags);
        return managerClient.listClusterNodes(request);
    }

    @Override
    public ClusterInfo saveCluster(ClusterRequest clusterRequest) {
        int clusterIndex = managerClient.saveCluster(clusterRequest);
        return managerClient.getCluster(clusterIndex);
    }
}
