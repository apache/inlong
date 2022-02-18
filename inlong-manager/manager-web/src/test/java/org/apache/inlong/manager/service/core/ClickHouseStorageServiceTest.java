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
import org.apache.inlong.manager.common.pojo.datastorage.ck.ClickHouseStorageRequest;
import org.apache.inlong.manager.common.pojo.datastorage.ck.ClickHouseStorageResponse;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.service.storage.StorageService;
import org.apache.inlong.manager.web.WebBaseTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Data storage service test
 */
public class ClickHouseStorageServiceTest extends WebBaseTest {

    @Autowired
    private StorageService storageService;
    @Autowired
    private DataStreamServiceTest streamServiceTest;

    // Partial test data
    private static final String globalGroupId = "b_group1";
    private static final String globalStreamId = "stream1";
    private static final String globalOperator = "test_user";
    private static final String ckJdbcUrl = "jdbc:clickhouse://127.0.0.1:8123/default";
    private static final String ckUsername = "ck_user";
    private static final String ckDatabaseName = "ck_db";
    private static final String ckTableName = "ck_tbl";

    private static Integer ckStorageId;

    @Before
    public void saveStorage() {
        streamServiceTest.saveDataStream(globalGroupId, globalStreamId, globalOperator);
        ClickHouseStorageRequest storageInfo = new ClickHouseStorageRequest();
        storageInfo.setInlongGroupId(globalGroupId);
        storageInfo.setInlongStreamId(globalStreamId);
        storageInfo.setStorageType(BizConstant.STORAGE_CLICKHOUSE);
        storageInfo.setJdbcUrl(ckJdbcUrl);
        storageInfo.setUsername(ckUsername);
        storageInfo.setDatabaseName(ckDatabaseName);
        storageInfo.setTableName(ckTableName);
        storageInfo.setEnableCreateResource(BizConstant.DISABLE_CREATE_RESOURCE);
        ckStorageId = storageService.save(storageInfo, globalOperator);
    }

    @After
    public void deleteKafkaStorage() {
        boolean result = storageService.delete(ckStorageId, BizConstant.STORAGE_CLICKHOUSE, globalOperator);
        Assert.assertTrue(result);
    }

    @Test
    public void testListByIdentifier() {
        StorageResponse storage = storageService.get(ckStorageId, BizConstant.STORAGE_CLICKHOUSE);
        Assert.assertEquals(globalGroupId, storage.getInlongGroupId());
    }

    @Test
    public void testGetAndUpdate() {
        StorageResponse response = storageService.get(ckStorageId, BizConstant.STORAGE_CLICKHOUSE);
        Assert.assertEquals(globalGroupId, response.getInlongGroupId());

        ClickHouseStorageResponse kafkaStorageResponse = (ClickHouseStorageResponse) response;
        kafkaStorageResponse.setEnableCreateResource(BizConstant.ENABLE_CREATE_RESOURCE);

        ClickHouseStorageRequest request = CommonBeanUtils
                .copyProperties(kafkaStorageResponse, ClickHouseStorageRequest::new);
        boolean result = storageService.update(request, globalOperator);
        Assert.assertTrue(result);
    }

}
