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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.inlong.manager.common.pojo.agent.AgentHeartbeatRequest;
import org.apache.inlong.manager.dao.entity.AgentHeartbeatEntity;
import org.apache.inlong.manager.dao.mapper.AgentHeartbeatEntityMapper;
import org.apache.inlong.manager.service.core.AgentHeartbeatService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;

@Service
public class AgentHeartbeatServiceImpl implements AgentHeartbeatService {

    @Autowired
    private AgentHeartbeatEntityMapper heartbeatEntityMapper;

    @Override
    public String heartbeat(AgentHeartbeatRequest info) {
        Gson gson = new GsonBuilder().create();

        AgentHeartbeatEntity record = new AgentHeartbeatEntity();
        record.setIp(info.getAgentIp());
        record.setVersion(info.getHeart().getVersion());
        record.setModifyTime(new Date());
        record.setHeartbeatMsg(gson.toJson(info.getHeart()));

        int success = heartbeatEntityMapper.replace(record);
        if (success > 0) {
            return "success";
        } else {
            throw new IllegalArgumentException("insert heartbeat into database failed");
        }
    }

}
