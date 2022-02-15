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

import java.util.Date;
import org.apache.inlong.manager.dao.DaoBaseTest;
import org.apache.inlong.manager.dao.entity.AgentHeartBeatLogEntity;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

public class AgentHeartBeatLogEntityMapperTest extends DaoBaseTest {

    @Autowired
    private AgentHeartBeatLogEntityMapper agentHeartBeatLogEntityMapper;

    @Test
    public void deleteByPrimaryKey() {
        AgentHeartBeatLogEntity entity = createAgentHeartBeatLogEntity();
        agentHeartBeatLogEntityMapper.insert(entity);
        agentHeartBeatLogEntityMapper.deleteByPrimaryKey(entity.getIp());
        Assert.assertNull(agentHeartBeatLogEntityMapper.selectByPrimaryKey(entity.getIp()));

    }

    @Test
    public void insert() {
        AgentHeartBeatLogEntity entity = createAgentHeartBeatLogEntity();
        agentHeartBeatLogEntityMapper.insert(entity);
        Assert.assertEquals(entity, agentHeartBeatLogEntityMapper.selectByPrimaryKey(entity.getIp()));
    }

    @Test
    public void insertSelective() {
        AgentHeartBeatLogEntity entity = createAgentHeartBeatLogEntity();
        entity.setHeartbeatMsg(null);
        agentHeartBeatLogEntityMapper.insertSelective(entity);
        AgentHeartBeatLogEntity queryResult = agentHeartBeatLogEntityMapper.selectByPrimaryKey(entity.getIp());
        Assert.assertNull(queryResult.getHeartbeatMsg());
    }

    @Test
    public void selectByPrimaryKey() {
        AgentHeartBeatLogEntity entity = createAgentHeartBeatLogEntity();
        agentHeartBeatLogEntityMapper.insert(entity);
        Assert.assertEquals(entity, agentHeartBeatLogEntityMapper.selectByPrimaryKey(entity.getIp()));
    }

    @Test
    public void updateByPrimaryKeySelective() {
        AgentHeartBeatLogEntity entity = createAgentHeartBeatLogEntity();
        agentHeartBeatLogEntityMapper.insert(entity);
        entity.setVersion(null);
        agentHeartBeatLogEntityMapper.updateByPrimaryKeySelective(entity);
        Assert.assertNotNull(agentHeartBeatLogEntityMapper.selectByPrimaryKey(entity.getIp()).getVersion());
    }

    @Test
    public void updateByPrimaryKey() {
        AgentHeartBeatLogEntity entity = createAgentHeartBeatLogEntity();
        agentHeartBeatLogEntityMapper.insert(entity);
        entity.setVersion(null);
        agentHeartBeatLogEntityMapper.updateByPrimaryKey(entity);
        Assert.assertEquals(entity, agentHeartBeatLogEntityMapper.selectByPrimaryKey(entity.getIp()));
    }

    @Test
    public void replace() {
        AgentHeartBeatLogEntity entity = createAgentHeartBeatLogEntity();
        agentHeartBeatLogEntityMapper.insert(entity);

        entity.setVersion(null);
        agentHeartBeatLogEntityMapper.replace(entity);
        Assert.assertEquals(entity, agentHeartBeatLogEntityMapper.selectByPrimaryKey(entity.getIp()));
    }

    private AgentHeartBeatLogEntity createAgentHeartBeatLogEntity() {
        AgentHeartBeatLogEntity entity = new AgentHeartBeatLogEntity();
        entity.setIp("127.0.0.1");
        entity.setVersion("0.0.1");
        entity.setModifyTime(new Date());
        entity.setHeartbeatMsg("test message");
        return entity;
    }
}