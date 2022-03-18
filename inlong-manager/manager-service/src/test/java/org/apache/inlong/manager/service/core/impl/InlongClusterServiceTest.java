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

package org.apache.inlong.manager.service.core.impl;

import org.apache.inlong.manager.common.enums.Constant;
import org.apache.inlong.manager.common.pojo.cluster.ClusterRequest;
import org.apache.inlong.manager.common.pojo.dataproxy.DataProxyResponse;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.apache.inlong.manager.service.core.ThirdPartyClusterService;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

/**
 * Inlong cluster service test.
 */
public class InlongClusterServiceTest extends ServiceBaseTest {

    private static final String CLUSTER_NAME = "test_data_proxy";
    private static final String CLUSTER_IP = "127.0.0.1";
    private static final Integer CLUSTER_PORT = 8088;

    @Autowired
    private ThirdPartyClusterService clusterService;

    public Integer saveOpt(String clusterName, String type, String ip, Integer port) {
        ClusterRequest request = new ClusterRequest();
        request.setName(clusterName);
        request.setType(type);
        request.setIp(ip);
        request.setPort(port);
        request.setInCharges(globalOperator);
        return clusterService.save(request, globalOperator);
    }

    public Boolean deleteOpt(Integer id) {
        return clusterService.delete(id, globalOperator);
    }

    @Test
    public void testSaveAndDelete() {
        Integer id = this.saveOpt(CLUSTER_NAME, Constant.CLUSTER_DATA_PROXY, CLUSTER_IP, CLUSTER_PORT);
        Assert.assertNotNull(id);

        Boolean success = this.deleteOpt(id);
        Assert.assertTrue(success);
    }

    @Test
    public void testGetDataProxy() {
        // Save the url with port p1, default port is p2
        Integer p1 = 46800;
        Integer p2 = 46801;
        String url = "127.0.0.1:" + p1 + ",127.0.0.2";
        Integer id = this.saveOpt(CLUSTER_NAME, Constant.CLUSTER_DATA_PROXY, url, p2);
        Assert.assertNotNull(id);

        // Get the data proxy cluster ip list, the first port should is p1, second port is p2
        List<DataProxyResponse> ipList = clusterService.getIpList(CLUSTER_NAME);
        Assert.assertEquals(ipList.size(), 2);
        Assert.assertEquals(p1, ipList.get(0).getPort());
        Assert.assertEquals(p2, ipList.get(1).getPort());

        this.deleteOpt(id);

        // Save the url without port, default port is p1
        url = "127.0.0.1";
        id = this.saveOpt(CLUSTER_NAME + "2", Constant.CLUSTER_DATA_PROXY, url, p1);
        ipList = clusterService.getIpList(CLUSTER_NAME + "2");
        // The result port is p1
        Assert.assertEquals(p1, ipList.get(0).getPort());

        this.deleteOpt(id);
    }

}
