package org.apache.inlong.agent.db;

import java.io.IOException;
import java.util.List;
import org.apache.inlong.agent.AgentBaseTestsHelper;
import org.apache.inlong.common.db.CommandEntity;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRocksDbImp {

    private static RocksDbImp db;
    private static AgentBaseTestsHelper helper;

    @BeforeClass
    public static void setup() throws Exception {
        helper = new AgentBaseTestsHelper(TestRocksDbImp.class.getName()).setupAgentHome();
        db = new RocksDbImp();
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
        db.put(entity);
        KeyValueEntity newEntity = db.get("test1");
        Assert.assertEquals("testC", newEntity.getJsonValue());

    }

    @Test
    public void testCommandDb() {
        CommandEntity commandEntity = new CommandEntity("1", 0, false, 1, "1");
        db.putCommand(commandEntity);
        CommandEntity command = db.getCommand("1");
        Assert.assertEquals("1", command.getId());
        List<CommandEntity> commandEntities = db.searchCommands(false);
        Assert.assertEquals("1", commandEntities.get(0).getId());
    }

    @Test
    public void testDeleteEntity() {
        KeyValueEntity entity = new KeyValueEntity("searchKey1", "searchResult1", "test");
        db.put(entity);
        db.remove("searchKey1");
        KeyValueEntity entityResult = db.searchOne(StateSearchKey.ACCEPTED);
        Assert.assertNull(entityResult);
    }

    @Test
    public void testFileNameSearch() {
        KeyValueEntity entity = new KeyValueEntity("searchKey1", "searchResult1", "test");
        db.put(entity);
        KeyValueEntity entityResult = db.searchOne(StateSearchKey.ACCEPTED);
        Assert.assertEquals("searchKey1", entityResult.getKey());
    }

    @AfterClass
    public static void teardown() throws IOException {
        db.close();
        helper.teardownAgentHome();
    }

}
