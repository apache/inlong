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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.inlong.manager.common.pojo.agent.AgentSysConfig;
import org.apache.inlong.manager.common.pojo.agent.AgentSysconfRequest;
import org.apache.inlong.manager.common.util.AgentConstants;
import org.apache.inlong.manager.dao.entity.AgentSysConfigEntity;
import org.apache.inlong.manager.dao.mapper.AgentSysConfigEntityMapper;
import org.apache.inlong.manager.service.core.AgentSysConfigService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class AgentSysConfigServiceImpl implements AgentSysConfigService {

    private static final Logger LOGGER = LoggerFactory.getLogger(AgentSysConfigServiceImpl.class);

    @Autowired
    private AgentSysConfigEntityMapper agentSysConfigEntityMapper;

    @Override
    public AgentSysConfig getAgentSysConfig(AgentSysconfRequest request) {
        AgentSysConfigEntity entity = agentSysConfigEntityMapper.selectByPrimaryKey(request.getAgentIp());
        if (entity != null) {
            AgentSysConfig agentSysConfig = AgentSysConfig.builder()
                    .msgQueueSize(entity.getMsgQueueSize())
                    .clearDayOffset(entity.getClearDayOffset())
                    .bufferSize(entity.getBuffersize())
                    .flushEventTimeoutMillSec(entity.getFlushEventTimeoutMillSec())
                    .threadManagerSleepInterval(entity.getThreadManagerSleepInterval())
                    .sendRunnableSize(entity.getSendRunnableSize())
                    .minRetryThreads(entity.getMinRetryThreads())
                    .clearIntervalSec(entity.getClearIntervalSec())
                    .maxReaderCnt(entity.getMaxReaderCnt())
                    .sendTimeoutMillSec(entity.getSendTimeoutMillSec())
                    .bufferSizeInBytes(entity.getBufferSizeInBytes())
                    .maxRetryThreads(entity.getMaxRetryThreads())
                    .dbPath(entity.getDbPath())
                    .scanIntervalSec(entity.getScanIntervalSec())
                    // TODO Need to confirm
                    .compress(entity.getCompress() == 1)
                    // TODO Need to confirm
                    .isCalcMd5(entity.getIsCalmd5() == 1)
                    .ip(entity.getIp())
                    .flowSize(entity.getFlowSize())
                    .onelineSize(entity.getOnelineSize())
                    .eventCheckInterval(entity.getEventCheckInterval())
                    .confRefreshIntervalSecs(entity.getConfRefreshIntervalSecs())
                    .statIntervalSec(entity.getStatIntervalSec())
                    .msgSize(entity.getMsgSize())
                    .batchSize(entity.getBatchSize())
                    .agentRpcReconnectTime(entity.getAgentRpcReconnectTime())
                    .md5(null)
                    .build();
            return agentSysConfig;
        } else {
            throw new IllegalArgumentException("do not get config from database");
        }
    }

}
