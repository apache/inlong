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

package org.apache.inlong.manager.dao.mapper;

import org.apache.inlong.manager.dao.DaoBaseTest;
import org.apache.inlong.manager.dao.entity.AgentHeartbeatLogEntity;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Date;

public class AgentHeartbeatLogEntityMapperTest extends DaoBaseTest {

    @Autowired
    private AgentHeartbeatLogEntityMapper heartbeatLogEntityMapper;

    @Test
    public void deleteByPrimaryKey() {
        AgentHeartbeatLogEntity entity = createAgentHeartBeatLogEntity();
        heartbeatLogEntityMapper.insert(entity);
        heartbeatLogEntityMapper.deleteByPrimaryKey(entity.getIp());
        Assert.assertNull(heartbeatLogEntityMapper.selectByPrimaryKey(entity.getIp()));
    }

    @Test
    public void insert() {
        AgentHeartbeatLogEntity entity = createAgentHeartBeatLogEntity();
        heartbeatLogEntityMapper.insert(entity);
        Assert.assertEquals(entity, heartbeatLogEntityMapper.selectByPrimaryKey(entity.getIp()));
    }

    @Test
    public void insertSelective() {
        AgentHeartbeatLogEntity entity = createAgentHeartBeatLogEntity();
        entity.setHeartbeatMsg(null);
        heartbeatLogEntityMapper.insertSelective(entity);
        AgentHeartbeatLogEntity queryResult = heartbeatLogEntityMapper.selectByPrimaryKey(entity.getIp());
        Assert.assertNull(queryResult.getHeartbeatMsg());
    }

    @Test
    public void selectByPrimaryKey() {
        AgentHeartbeatLogEntity entity = createAgentHeartBeatLogEntity();
        heartbeatLogEntityMapper.insert(entity);
        Assert.assertEquals(entity, heartbeatLogEntityMapper.selectByPrimaryKey(entity.getIp()));
    }

    @Test
    public void updateByPrimaryKeySelective() {
        AgentHeartbeatLogEntity entity = createAgentHeartBeatLogEntity();
        heartbeatLogEntityMapper.insert(entity);
        entity.setVersion(null);
        heartbeatLogEntityMapper.updateByPrimaryKeySelective(entity);
        Assert.assertNotNull(heartbeatLogEntityMapper.selectByPrimaryKey(entity.getIp()).getVersion());
    }

    @Test
    public void updateByPrimaryKey() {
        AgentHeartbeatLogEntity entity = createAgentHeartBeatLogEntity();
        heartbeatLogEntityMapper.insert(entity);
        entity.setVersion(null);
        heartbeatLogEntityMapper.updateByPrimaryKey(entity);
        Assert.assertEquals(entity, heartbeatLogEntityMapper.selectByPrimaryKey(entity.getIp()));
    }

    @Test
    public void replace() {
        AgentHeartbeatLogEntity entity = createAgentHeartBeatLogEntity();
        heartbeatLogEntityMapper.insert(entity);

        entity.setVersion(null);
        heartbeatLogEntityMapper.replace(entity);
        Assert.assertEquals(entity, heartbeatLogEntityMapper.selectByPrimaryKey(entity.getIp()));
    }

    private AgentHeartbeatLogEntity createAgentHeartBeatLogEntity() {
        AgentHeartbeatLogEntity entity = new AgentHeartbeatLogEntity();
        entity.setIp("127.0.0.1");
        entity.setVersion("0.0.1");
        entity.setModifyTime(new Date());
        entity.setHeartbeatMsg("test message");
        return entity;
    }
}