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

package org.apache.inlong.agent.db;

import java.util.List;
import org.apache.inlong.agent.AgentBaseTestsHelper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBerkeleyDBImp {

    private static BerkeleyDbImp db;
    private static AgentBaseTestsHelper helper;

    @BeforeClass
    public static void setup() throws Exception {
        helper = new AgentBaseTestsHelper(TestBerkeleyDBImp.class.getName()).setupAgentHome();
        db = new BerkeleyDbImp();
    }

    @AfterClass
    public static void teardown() throws Exception {
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

        db.set(entity);
        db.set(entity1);

        List<KeyValueEntity> entityList = db.search(keys);
        for (KeyValueEntity keyValueEntity : entityList) {
            Assert.assertEquals(StateSearchKey.SUCCESS, keyValueEntity.getStateSearchKey());
        }
        Assert.assertEquals(2, entityList.size());

        entity.setJsonValue("testC");
        KeyValueEntity oldEntity = db.put(entity);
        Assert.assertEquals("testA", oldEntity.getJsonValue());

        KeyValueEntity newEntity = db.get("test1");
        Assert.assertEquals("testC", newEntity.getJsonValue());

    }

    @Test
    public void testCommandDb() {
        CommandEntity commandEntity = new CommandEntity("1", 0, false, "1", "");
        db.putCommand(commandEntity);
        CommandEntity command = db.getCommand("1");
        Assert.assertEquals("1", command.getId());
        List<CommandEntity> commandEntities = db.searchCommands(false);
        Assert.assertEquals("1", commandEntities.get(0).getId());
    }

    @Test
    public void testSecondaryIndex() {
        KeyValueEntity entity = new KeyValueEntity("searchKey1", "searchResult1", "test");
        db.put(entity);
        KeyValueEntity entity1 = new KeyValueEntity("searchKey2", "searchResult2", "test");
        db.put(entity1);
        KeyValueEntity entityResult = db.searchOne(StateSearchKey.ACCEPTED);
        Assert.assertEquals("searchKey1", entityResult.getKey());

        entityResult = db.searchOne(StateSearchKey.ACCEPTED);
        Assert.assertEquals("searchKey1", entityResult.getKey());

        entityResult.setStateSearchKey(StateSearchKey.RUNNING);
        db.put(entityResult);

        entityResult = db.searchOne(StateSearchKey.ACCEPTED);
        Assert.assertEquals("searchKey2", entityResult.getKey());

        List<KeyValueEntity> entityList = db.search(StateSearchKey.ACCEPTED);
        Assert.assertEquals(1, entityList.size());

        entityList = db.search(StateSearchKey.FAILED);
        Assert.assertEquals(0, entityList.size());
    }

    @Test
    public void testDeleteItem() {
        KeyValueEntity entity = new KeyValueEntity("searchKey1", "searchResult1", "test");
        db.put(entity);
        KeyValueEntity entityResult1 = db.remove("searchKey1");
        KeyValueEntity entityResult = db.searchOne(StateSearchKey.ACCEPTED);
        Assert.assertEquals("searchKey1", entityResult1.getKey());
        Assert.assertNull(entityResult);
    }

    @Test
    public void testFileNameSearch() {
        KeyValueEntity entity = new KeyValueEntity("searchKey1", "searchResult1", "test");
        db.put(entity);
        KeyValueEntity entityResult = db.searchOne("test");
        Assert.assertEquals("searchKey1", entityResult.getKey());
    }

}
