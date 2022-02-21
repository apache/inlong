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
import org.apache.inlong.manager.dao.entity.AgentHeartbeatEntity;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Date;

public class AgentHeartbeatEntityMapperTest extends DaoBaseTest {

    @Autowired
    private AgentHeartbeatEntityMapper heartbeatEntityMapper;

    @Test
    public void deleteByPrimaryKey() {
        AgentHeartbeatEntity entity = createHeartbeatEntity();
        heartbeatEntityMapper.insert(entity);
        heartbeatEntityMapper.deleteByPrimaryKey(entity.getIp());
        Assert.assertNull(heartbeatEntityMapper.selectByPrimaryKey(entity.getIp()));
    }

    @Test
    public void insert() {
        AgentHeartbeatEntity entity = createHeartbeatEntity();
        heartbeatEntityMapper.insert(entity);
        Assert.assertEquals(entity, heartbeatEntityMapper.selectByPrimaryKey(entity.getIp()));
    }

    @Test
    public void insertSelective() {
        AgentHeartbeatEntity entity = createHeartbeatEntity();
        entity.setHeartbeatMsg(null);
        heartbeatEntityMapper.insertSelective(entity);
        AgentHeartbeatEntity queryResult = heartbeatEntityMapper.selectByPrimaryKey(entity.getIp());
        Assert.assertNull(queryResult.getHeartbeatMsg());
    }

    @Test
    public void selectByPrimaryKey() {
        AgentHeartbeatEntity entity = createHeartbeatEntity();
        heartbeatEntityMapper.insert(entity);
        Assert.assertEquals(entity, heartbeatEntityMapper.selectByPrimaryKey(entity.getIp()));
    }

    @Test
    public void updateByPrimaryKeySelective() {
        AgentHeartbeatEntity entity = createHeartbeatEntity();
        heartbeatEntityMapper.insert(entity);
        entity.setVersion(null);
        heartbeatEntityMapper.updateByPrimaryKeySelective(entity);
        Assert.assertNotNull(heartbeatEntityMapper.selectByPrimaryKey(entity.getIp()).getVersion());
    }

    @Test
    public void updateByPrimaryKey() {
        AgentHeartbeatEntity entity = createHeartbeatEntity();
        heartbeatEntityMapper.insert(entity);
        entity.setVersion(null);
        heartbeatEntityMapper.updateByPrimaryKey(entity);
        Assert.assertEquals(entity, heartbeatEntityMapper.selectByPrimaryKey(entity.getIp()));
    }

    @Test
    public void replace() {
        AgentHeartbeatEntity entity = createHeartbeatEntity();
        heartbeatEntityMapper.insert(entity);

        entity.setVersion(null);
        heartbeatEntityMapper.replace(entity);
        Assert.assertEquals(entity, heartbeatEntityMapper.selectByPrimaryKey(entity.getIp()));
    }

    private AgentHeartbeatEntity createHeartbeatEntity() {
        AgentHeartbeatEntity entity = new AgentHeartbeatEntity();
        entity.setIp("127.0.0.1");
        entity.setVersion("0.0.1");
        entity.setModifyTime(new Date());
        entity.setHeartbeatMsg("test message");
        return entity;
    }

}