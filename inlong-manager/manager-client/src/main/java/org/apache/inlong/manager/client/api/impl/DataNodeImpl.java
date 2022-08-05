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

import com.github.pagehelper.PageInfo;
import org.apache.inlong.manager.client.api.ClientConfiguration;
import org.apache.inlong.manager.client.api.DataNode;
import org.apache.inlong.manager.client.api.inner.client.ClientFactory;
import org.apache.inlong.manager.client.api.inner.client.DataNodeClient;
import org.apache.inlong.manager.client.api.util.ClientUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.pojo.node.DataNodeRequest;
import org.apache.inlong.manager.pojo.node.DataNodeResponse;

public class DataNodeImpl implements DataNode {

    private final DataNodeClient dataNodeClient;

    public DataNodeImpl(ClientConfiguration configuration) {
        ClientFactory clientFactory = ClientUtils.getClientFactory(configuration);
        this.dataNodeClient = clientFactory.getDataNodeClient();
    }

    @Override
    public Integer save(DataNodeRequest request) {
        Preconditions.checkNotEmpty(request.getName(), "data node name should not be empty");
        Preconditions.checkNotEmpty(request.getType(), "data node type should not be empty");
        return dataNodeClient.save(request);
    }

    @Override
    public DataNodeResponse get(Integer id) {
        Preconditions.checkNotNull(id, "data node id should not be empty");
        return dataNodeClient.get(id);
    }

    @Override
    public PageInfo<DataNodeResponse> list(DataNodeRequest request) {
        return dataNodeClient.list(request);
    }

    @Override
    public Boolean update(DataNodeRequest request) {
        Preconditions.checkNotEmpty(request.getName(), "data node name should not be empty");
        Preconditions.checkNotEmpty(request.getType(), "data node type should not be empty");
        Preconditions.checkNotNull(request.getId(), "data node id should not be empty");
        return dataNodeClient.update(request);
    }

    @Override
    public Boolean delete(Integer id) {
        Preconditions.checkNotNull(id, "data node id should not be empty");
        return dataNodeClient.delete(id);
    }
}
