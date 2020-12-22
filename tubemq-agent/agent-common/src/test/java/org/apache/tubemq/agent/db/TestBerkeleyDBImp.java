/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tubemq.agent.db;

import java.util.List;
import org.apache.tubemq.agent.AgentBaseTestsHelper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestBerkeleyDBImp {

    private static BerkeleyDBImp db;
    private static final String className = TestBerkeleyDBImp.class.getName();

    @BeforeClass
    public static void setup() throws Exception {
        AgentBaseTestsHelper.setupAgentHome(className);
        db = new BerkeleyDBImp();
    }

    @AfterClass
    public static void teardown() throws Exception {
        db.close();
        AgentBaseTestsHelper.teardownAgentHome(className);
    }

    @Test
    public void testDB() {
        KeyValueEntity entity = new KeyValueEntity("test1", "testA");
        db.put(entity);
        KeyValueEntity ret = db.get("test1");
        Assert.assertEquals("test1", ret.getKey());
        Assert.assertEquals("testA", ret.getJsonValue());

        db.remove("test1");
        ret = db.get("test1");
        Assert.assertNull(ret);

        StateSearchKey keys = StateSearchKey.SUCCESS;
        KeyValueEntity entity1 = new KeyValueEntity("test2", "testA");
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
    public void testSecondaryIndex() {
        KeyValueEntity entity = new KeyValueEntity("searchKey1", "searchResult1");
        db.put(entity);
        KeyValueEntity entity1 = new KeyValueEntity("searchKey2", "searchResult2");
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

}
