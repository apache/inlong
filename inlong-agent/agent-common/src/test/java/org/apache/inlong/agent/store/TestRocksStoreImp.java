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

package org.apache.inlong.agent.store;

import org.apache.inlong.agent.AgentBaseTestsHelper;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class TestRocksStoreImp {

    private static RocksStoreImp db;
    private static AgentBaseTestsHelper helper;

    @BeforeClass
    public static void setup() throws Exception {
        helper = new AgentBaseTestsHelper(TestRocksStoreImp.class.getName()).setupAgentHome();
        db = new RocksStoreImp("/localdb");
    }

    @AfterClass
    public static void teardown() throws IOException {
        db.close();
        helper.teardownAgentHome();
    }

    @Test
    public void testKeyValueDB() {
        KeyValueEntity entity = new KeyValueEntity("test1", "testA", "test");
        db.put(entity);
        KeyValueEntity ret = db.get("test1");
        Assert.assertEquals("test1", ret.getKey());
        Assert.assertEquals("testA", ret.getJsonValue());

        db.remove("test1");
        ret = db.get("test1");
        Assert.assertNull(ret);

        StateSearchKey keys = StateSearchKey.SUCCESS;
        KeyValueEntity entity1 = new KeyValueEntity("test2", "testA", "test");
        entity.setStateSearchKey(keys);
        entity1.setStateSearchKey(keys);

        entity.setJsonValue("testC");
        db.put(entity);
        KeyValueEntity newEntity = db.get("test1");
        Assert.assertEquals("testC", newEntity.getJsonValue());
    }

    @Test
    public void testDeleteEntity() {
        KeyValueEntity entity = new KeyValueEntity("searchKey1", "searchResult1", "test");
        db.put(entity);
        db.remove("searchKey1");
        KeyValueEntity entityResult = db.get("searchKey1");
        Assert.assertNull(entityResult);
    }
}
