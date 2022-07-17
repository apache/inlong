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

package org.apache.inlong.manager.client.api.inner.client;

import org.apache.inlong.manager.client.api.ClientConfiguration;
import org.apache.inlong.manager.client.api.service.InlongClusterApi;
import org.apache.inlong.manager.client.api.util.ClientUtils;
import org.apache.inlong.manager.common.beans.Response;
import org.apache.inlong.manager.common.pojo.cluster.ClusterRequest;
import org.apache.inlong.manager.common.util.Preconditions;

/**
 * Client for {@link InlongClusterApi}.
 */
public class InlongClusterClient {

    private final InlongClusterApi inlongClusterApi;

    public InlongClusterClient(ClientConfiguration configuration) {
        inlongClusterApi = ClientUtils.createRetrofit(configuration).create(InlongClusterApi.class);
    }

    /**
     * Save component cluster for Inlong
     *
     * @param request cluster create request
     * @return clusterIndex
     */
    public Integer saveCluster(ClusterRequest request) {
        Preconditions.checkNotEmpty(request.getName(), "cluster name should not be empty");
        Preconditions.checkNotEmpty(request.getType(), "cluster type should not be empty");
        Preconditions.checkNotEmpty(request.getClusterTags(), "cluster tags should not be empty");
        Response<Integer> clusterIndexResponse = ClientUtils.executeHttpCall(inlongClusterApi.save(request));
        ClientUtils.assertRespSuccess(clusterIndexResponse);
        return clusterIndexResponse.getData();
    }
}
