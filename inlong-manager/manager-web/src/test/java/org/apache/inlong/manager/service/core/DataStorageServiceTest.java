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
import org.apache.inlong.manager.common.pojo.datastorage.BaseStorageResponse;
import org.apache.inlong.manager.common.pojo.datastorage.StorageHiveRequest;
import org.apache.inlong.manager.common.pojo.datastorage.StorageHiveResponse;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.web.ServiceBaseTest;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Data storage service test
 */
public class DataStorageServiceTest extends ServiceBaseTest {

    private final String globalGroupId = "b_group1";
    private final String globalStreamId = "stream1";
    private final String globalOperator = "test_user";

    @Autowired
    private StorageService storageService;
    @Autowired
    private BusinessServiceTest businessServiceTest;
    @Autowired
    private DataStreamServiceTest streamServiceTest;

    public Integer saveStorage() {
        streamServiceTest.saveDataStream(globalGroupId, globalStreamId, globalOperator);

        StorageHiveRequest storageInfo = new StorageHiveRequest();
        storageInfo.setInlongGroupId(globalGroupId);
        storageInfo.setInlongStreamId(globalStreamId);
        storageInfo.setStorageType(BizConstant.STORAGE_HIVE);
        storageInfo.setEnableCreateTable(BizConstant.DISABLE_CREATE_TABLE);

        return storageService.save(storageInfo, globalOperator);
    }

    @Test
    public void testSaveAndDelete() {
        Integer id = this.saveStorage();
        Assert.assertNotNull(id);

        boolean result = storageService.delete(BizConstant.STORAGE_HIVE, id, globalOperator);
        Assert.assertTrue(result);
    }

    @Test
    public void testListByIdentifier() {
        Integer id = this.saveStorage();

        BaseStorageResponse storage = storageService.getById(BizConstant.STORAGE_HIVE, id);
        Assert.assertEquals(globalGroupId, storage.getInlongGroupId());

        storageService.delete(BizConstant.STORAGE_HIVE, id, globalOperator);
    }

    @Test
    public void testGetAndUpdate() {
        Integer id = this.saveStorage();
        BaseStorageResponse storage = storageService.getById(BizConstant.STORAGE_HIVE, id);
        Assert.assertEquals(globalGroupId, storage.getInlongGroupId());

        StorageHiveResponse hiveResponse = (StorageHiveResponse) storage;
        hiveResponse.setEnableCreateTable(BizConstant.DISABLE_CREATE_TABLE);

        StorageHiveRequest request = CommonBeanUtils.copyProperties(hiveResponse, StorageHiveRequest::new);
        boolean result = storageService.update(request, globalOperator);
        Assert.assertTrue(result);
    }

}
