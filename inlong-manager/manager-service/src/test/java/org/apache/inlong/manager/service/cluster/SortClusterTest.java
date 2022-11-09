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

package org.apache.inlong.manager.service.cluster;

import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.pojo.cluster.ClusterInfo;
import org.apache.inlong.manager.pojo.cluster.cls.ClsClusterRequest;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests for Sort clusters
 */
public class SortClusterTest extends ServiceBaseTest {

    @Autowired
    private InlongClusterService clusterService;

    @Test
    public void testCreateClsCluster() {
        ClsClusterRequest request = new ClsClusterRequest();
        request.setName(GLOBAL_CLUSTER_NAME);
        request.setInCharges(GLOBAL_OPERATOR);
        int id = clusterService.save(request, GLOBAL_OPERATOR);
        ClusterInfo info = clusterService.get(id, GLOBAL_OPERATOR);
        Assertions.assertEquals(GLOBAL_CLUSTER_NAME, info.getName());
        Assertions.assertEquals(ClusterType.CLS, info.getType());
    }

}
