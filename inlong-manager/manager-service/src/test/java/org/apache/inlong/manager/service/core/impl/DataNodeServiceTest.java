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
import org.apache.inlong.manager.common.pojo.node.DataNodePageRequest;
import org.apache.inlong.manager.common.pojo.node.DataNodeRequest;
import org.apache.inlong.manager.common.pojo.node.DataNodeResponse;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.apache.inlong.manager.service.core.DataNodeService;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Data node service test for {@link DataNodeService}
 */
public class DataNodeServiceTest extends ServiceBaseTest {

    @Autowired
    private DataNodeService dataNodeService;

    /**
     * save data node info.
     */
    public Integer saveOpt(String dataNodeName, String type, String url, String userName,
            String password) {
        DataNodeRequest request = new DataNodeRequest();
        request.setName(dataNodeName);
        request.setType(type);
        request.setUrl(url);
        request.setUsername(userName);
        request.setPassword(password);
        request.setInCharges(GLOBAL_OPERATOR);
        return dataNodeService.save(request, GLOBAL_OPERATOR);
    }

    public Boolean deleteOpt(Integer id) {
        return dataNodeService.delete(id, GLOBAL_OPERATOR);
    }

    /**
     * test save and delete data node info.
     */
    @Test
    public void testSaveAndDelete() {
        String dataNodeName = "defaultNodeName";
        String type = "PULSAR";
        String url = "127.0.0.1:8080";
        String useName = "admin";
        String password = "123";

        // test save data node
        Integer id = this.saveOpt(dataNodeName, type, url, useName, password);
        Assert.assertNotNull(id);

        // test get data node
        Assert.assertEquals(type, dataNodeService.get(id).getType());

        // test delete data node
        Boolean success = this.deleteOpt(id);
        Assert.assertTrue(success);
    }

    /**
     * get data node list info.
     */
    public PageInfo<DataNodeResponse> listDataNode(String type, String name) {
        DataNodePageRequest request = new DataNodePageRequest();
        request.setType(type);
        request.setName(name);
        return dataNodeService.list(request);
    }

    @Test
    public void testGetDataNodeList() {
        String dataNodeName = "defaultNodeName";
        String type = "PULSAR";
        String url = "127.0.0.1:8080";
        String useName = "admin";
        String password = "123";

        // test save data node
        Integer id = this.saveOpt(dataNodeName, type, url, useName, password);
        Assert.assertNotNull(id);

        // test get data node
        DataNodeResponse tmp = dataNodeService.get(id);
        Assert.assertEquals(type, dataNodeService.get(id).getType());

        // test get data node list
        PageInfo<DataNodeResponse> listDataNode = this.listDataNode(type, dataNodeName);
        Assert.assertEquals(listDataNode.getTotal(), 1);

        // test delete data node
        Boolean success = this.deleteOpt(id);
        Assert.assertTrue(success);
    }

    /**
     * update data node info.
     */
    public Boolean updateDataNode(Integer id, String dataNodeName, String type, String url, String userName,
            String password) {
        DataNodeRequest request = new DataNodeRequest();
        request.setId(id);
        request.setName(dataNodeName);
        request.setType(type);
        request.setUrl(url);
        request.setUsername(userName);
        request.setPassword(password);
        return dataNodeService.update(request, GLOBAL_OPERATOR);
    }

    @Test
    public void testSaveAndUpdate() {
        String dataNodeName = "defaultNodeName";
        String type = "PULSAR";
        String url = "127.0.0.1:8080";
        String useName = "admin";
        String password = "123";

        String newDataNodeName = "newDefaultNodeName";
        String newType = "TUBE";
        String newUrl = "127.0.0.1:8083";
        String newUseName = "admin2";
        String newPassword = "456";

        // test save data node
        Integer id = this.saveOpt(dataNodeName, type, url, useName, password);
        Assert.assertNotNull(id);

        // test get data node list
        Boolean updateNodeSuccess = this.updateDataNode(id, newDataNodeName, newType, newUrl, newUseName, newPassword);
        Assert.assertTrue(updateNodeSuccess);

        // test delete data node
        Boolean success = this.deleteOpt(id);
        Assert.assertTrue(success);
    }
}
