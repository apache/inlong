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

import org.apache.inlong.manager.common.enums.BizConstant;
import org.apache.inlong.manager.common.pojo.datastorage.StorageResponse;
import org.apache.inlong.manager.common.pojo.datastorage.iceberg.IcebergStorageRequest;
import org.apache.inlong.manager.common.pojo.datastorage.iceberg.IcebergStorageResponse;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.service.storage.StorageService;
import org.apache.inlong.manager.web.WebBaseTest;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class IcebergStorageServiceTest extends WebBaseTest {

    private final String globalGroupId = "b_group1";
    private final String globalStreamId = "stream1";
    private final String globalOperator = "test_user";

    @Autowired
    private StorageService storageService;
    @Autowired
    private DataStreamServiceTest streamServiceTest;

    public Integer saveStorage() {
        streamServiceTest.saveDataStream(globalGroupId, globalStreamId, globalOperator);

        IcebergStorageRequest storageInfo = new IcebergStorageRequest();
        storageInfo.setInlongGroupId(globalGroupId);
        storageInfo.setInlongStreamId(globalStreamId);
        storageInfo.setStorageType(BizConstant.STORAGE_ICEBERG);
        storageInfo.setEnableCreateResource(BizConstant.DISABLE_CREATE_RESOURCE);
        storageInfo.setTableLocation("hdfs://127.0.0.1:8020/data");

        return storageService.save(storageInfo, globalOperator);
    }

    @Test
    public void testSaveAndDelete() {
        Integer id = this.saveStorage();
        Assert.assertNotNull(id);

        boolean result = storageService.delete(id, BizConstant.STORAGE_ICEBERG, globalOperator);
        Assert.assertTrue(result);
    }

    @Test
    public void testListByIdentifier() {
        Integer id = this.saveStorage();

        StorageResponse storage = storageService.get(id, BizConstant.STORAGE_ICEBERG);
        Assert.assertEquals(globalGroupId, storage.getInlongGroupId());

        storageService.delete(id, BizConstant.STORAGE_ICEBERG, globalOperator);
    }

    @Test
    public void testGetAndUpdate() {
        Integer id = this.saveStorage();
        StorageResponse response = storageService.get(id, BizConstant.STORAGE_ICEBERG);
        Assert.assertEquals(globalGroupId, response.getInlongGroupId());

        IcebergStorageResponse icebergStorageResponse = (IcebergStorageResponse) response;
        icebergStorageResponse.setEnableCreateResource(BizConstant.DISABLE_CREATE_RESOURCE);

        IcebergStorageRequest request = CommonBeanUtils.copyProperties(icebergStorageResponse,
                IcebergStorageRequest::new);
        boolean result = storageService.update(request, globalOperator);
        Assert.assertTrue(result);
    }
}
