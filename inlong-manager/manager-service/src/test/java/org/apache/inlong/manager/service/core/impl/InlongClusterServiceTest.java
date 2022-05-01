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

import com.github.pagehelper.PageInfo;
import org.apache.inlong.manager.common.pojo.cluster.ClusterNodeRequest;
import org.apache.inlong.manager.common.pojo.cluster.ClusterNodeResponse;
import org.apache.inlong.manager.common.pojo.cluster.ClusterRequest;
import org.apache.inlong.manager.common.pojo.cluster.InlongClusterPageRequest;
import org.apache.inlong.manager.common.pojo.cluster.InlongClusterRequest;
import org.apache.inlong.manager.common.pojo.cluster.InlongClusterResponse;
import org.apache.inlong.manager.common.pojo.dataproxy.DataProxyResponse;
import org.apache.inlong.manager.common.settings.InlongGroupSettings;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.apache.inlong.manager.service.core.InlongClusterService;
import org.apache.inlong.manager.service.core.ThirdPartyClusterService;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

/**
 * Inlong cluster service test for {@link InlongClusterService}
 */
public class InlongClusterServiceTest extends ServiceBaseTest {

    private static final String CLUSTER_NAME = "test_data_proxy";
    private static final String CLUSTER_IP = "127.0.0.1";
    private static final Integer CLUSTER_PORT = 8088;

    @Autowired
    private ThirdPartyClusterService clusterService;

    @Autowired
    private InlongClusterService inlongClusterService;

    public Integer saveOpt(String clusterName, String type, String ip, Integer port) {
        ClusterRequest request = new ClusterRequest();
        request.setName(clusterName);
        request.setType(type);
        request.setIp(ip);
        request.setPort(port);
        request.setInCharges(GLOBAL_OPERATOR);
        return clusterService.save(request, GLOBAL_OPERATOR);
    }

    public Boolean deleteOpt(Integer id) {
        return clusterService.delete(id, GLOBAL_OPERATOR);
    }

    @Test
    public void testSaveAndDelete() {
        Integer id = this.saveOpt(CLUSTER_NAME, InlongGroupSettings.CLUSTER_DATA_PROXY, CLUSTER_IP, CLUSTER_PORT);
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
        Integer id = this.saveOpt(CLUSTER_NAME, InlongGroupSettings.CLUSTER_DATA_PROXY, url, p2);
        Assert.assertNotNull(id);

        // Get the data proxy cluster ip list, the first port should is p1, second port is p2
        List<DataProxyResponse> ipList = clusterService.getIpList(CLUSTER_NAME);
        Assert.assertEquals(ipList.size(), 2);
        Assert.assertEquals(p1, ipList.get(0).getPort());
        Assert.assertEquals(p2, ipList.get(1).getPort());

        this.deleteOpt(id);
    }

    @Test
    public void testSaveAndGetDirtyDataProxy() {
        // Simulate saving and parsing dirty url without port, default port is p1
        Integer p1 = 46801;
        String url = ":,,, :127.0 .0.1:,: ,,,";
        Integer id = this.saveOpt(CLUSTER_NAME, InlongGroupSettings.CLUSTER_DATA_PROXY, url, p1);
        List<DataProxyResponse> ipList = clusterService.getIpList(CLUSTER_NAME);
        // The result port is p1
        Assert.assertEquals(p1, ipList.get(0).getPort());

        this.deleteOpt(id);
    }

    /**
     * save cluster info.
     */
    public Integer saveCluster(String clusterName, String type, String clusterTag) {
        InlongClusterRequest request = new InlongClusterRequest();
        request.setName(clusterName);
        request.setType(type);
        request.setClusterTag(clusterTag);
        request.setInCharges(GLOBAL_OPERATOR);
        return inlongClusterService.save(request, GLOBAL_OPERATOR);
    }

    /**
     * get cluster list info.
     */
    public PageInfo<InlongClusterResponse> listCluster(String type, String clusterTag) {
        InlongClusterPageRequest request = new InlongClusterPageRequest();
        request.setType(type);
        request.setClusterTag(clusterTag);
        return inlongClusterService.list(request);
    }

    /**
     * update cluster info.
     */
    public Boolean updateCluster(Integer id, String name, String type, String clusterTag) {
        InlongClusterRequest request = new InlongClusterRequest();
        request.setId(id);
        request.setName(name);
        request.setType(type);
        request.setClusterTag(clusterTag);
        request.setInCharges(GLOBAL_OPERATOR);
        return inlongClusterService.update(request, GLOBAL_OPERATOR);
    }

    /**
     * delete cluster info by id.
     */
    public Boolean deleteCluster(Integer id) {
        return inlongClusterService.delete(id, GLOBAL_OPERATOR);
    }

    /**
     * save cluster node info.
     */
    public Integer saveClusterNode(Integer parentId, String type, String ip, Integer port) {
        ClusterNodeRequest request = new ClusterNodeRequest();
        request.setParentId(parentId);
        request.setType(type);
        request.setIp(ip);
        request.setPort(port);
        return inlongClusterService.saveNode(request, GLOBAL_OPERATOR);
    }

    /**
     * update cluster node info.
     */
    public Boolean updateClusterNode(Integer id, Integer parentId, String type, String ip, Integer port) {
        ClusterNodeRequest request = new ClusterNodeRequest();
        request.setId(id);
        request.setParentId(parentId);
        request.setType(type);
        request.setIp(ip);
        request.setPort(port);
        return inlongClusterService.updateNode(request, GLOBAL_OPERATOR);
    }

    /**
     * get cluster node list info.
     */
    public PageInfo<ClusterNodeResponse> listNode(String type, String keyword) {
        InlongClusterPageRequest request = new InlongClusterPageRequest();
        request.setType(type);
        request.setKeyword(keyword);
        return inlongClusterService.listNode(request);
    }

    /**
     * delete cluster node info.
     */
    public Boolean deleteClusterNode(Integer id) {
        return inlongClusterService.deleteNode(id, GLOBAL_OPERATOR);
    }

    /**
     * test cluster interface.
     */
    @Test
    public void testClusterSaveAndDelete() {
        String type = "PULSAR";
        String clusterTag = "default_cluster";
        String ip = "127.0.0.1";
        Integer port = 8080;

        String typeUpdate = "DATA_PROXY";
        String clusterTagUpdate = "default_cluster_2";
        String ipUpdate = "localhost";
        Integer portUpdate = 8083;

        // save cluster
        Integer id = this.saveCluster(CLUSTER_NAME, type, clusterTag);
        Assert.assertNotNull(id);

        // list cluster
        PageInfo<InlongClusterResponse> listCluster = this.listCluster(type, clusterTag);
        Assert.assertEquals(listCluster.getTotal(), 1);

        // update cluster
        Boolean updateSuccess = this.updateCluster(id, CLUSTER_NAME, typeUpdate, clusterTagUpdate);
        Assert.assertTrue(updateSuccess);

        // save cluster node
        Integer parentId = 1;
        Integer nodeId = this.saveClusterNode(parentId, type, ip, port);
        Assert.assertNotNull(nodeId);

        // list cluster node
        PageInfo<ClusterNodeResponse> listNode = this.listNode(type, ip);
        Assert.assertEquals(listNode.getTotal(), 1);

        // update cluster node
        Boolean updateNodeSuccess = this.updateClusterNode(id, parentId, typeUpdate, ipUpdate, portUpdate);
        Assert.assertTrue(updateNodeSuccess);

        // delete cluster node
        Boolean deleteClusterSuccess = this.deleteClusterNode(nodeId);
        Assert.assertTrue(deleteClusterSuccess);

        // delete Cluster
        Boolean success = this.deleteCluster(id);
        Assert.assertTrue(success);
    }

}
