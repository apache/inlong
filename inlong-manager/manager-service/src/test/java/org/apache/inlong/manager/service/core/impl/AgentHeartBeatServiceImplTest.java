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

package org.apache.inlong.manager.service.core.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;

import org.apache.inlong.manager.common.pojo.agent.AgentHeartbeatRequest;
import org.apache.inlong.manager.common.pojo.agent.HeartbeatMessage;
import org.apache.inlong.manager.dao.mapper.AgentHeartBeatLogEntityMapper;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.junit.Before;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

class AgentHeartBeatServiceImplTest extends ServiceBaseTest {

    @InjectMocks
    AgentHeartBeatServiceImpl agentHeartBeatService;

    @Mock
    AgentHeartBeatLogEntityMapper agentHeartBeatLogEntityMapper;

    @Before
    public void before() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void heartbeat() {
        Mockito.when(agentHeartBeatLogEntityMapper.replace(any())).thenReturn(1);
        AgentHeartbeatRequest vo = new AgentHeartbeatRequest();
        vo.setHeart(new HeartbeatMessage());
        String ans = agentHeartBeatService.heartbeat(vo);
        assertEquals("success", ans);
    }
}