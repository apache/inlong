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

    public String calcMd5(AgentSysConfigEntity entity) {
        Map<String, String> propertiesMap = new TreeMap<>();
        propertiesMap.put(AgentConstants.NAME, "inlong_agent");
        propertiesMap.put(AgentConstants.IP, entity.getIp());
        propertiesMap.put(AgentConstants.DB_PATH, entity.getDbPath());
        propertiesMap.put(AgentConstants.SCAN_INTERVAL_SEC, "" + entity.getScanIntervalSec());
        propertiesMap.put(AgentConstants.BATCH_SIZE, "" + entity.getBatchSize());
        propertiesMap.put(AgentConstants.MSG_SIZE, "" + entity.getMsgSize());
        propertiesMap.put(AgentConstants.SEND_RUNNABLEC_SIZE, "" + entity.getSendRunnableSize());
        propertiesMap.put(AgentConstants.MSG_QUEUE_SIZE, "" + entity.getMsgQueueSize());
        propertiesMap.put(AgentConstants.ONELINE_SIZE, "" + entity.getOnelineSize());
        propertiesMap.put(AgentConstants.STAT_INTERVAL_SEC, "" + entity.getStatIntervalSec());

        propertiesMap.put(AgentConstants.MAX_READER_CNT, "" + entity.getMaxReaderCnt());
        // TODO
        propertiesMap.put(AgentConstants.COMPRESS, "" + (entity.getCompress() == 1));
        // TODO
        propertiesMap.put(AgentConstants.IS_SEND, "" + AgentConstants.DEFAULT_IS_SEND);
        // TODO
        propertiesMap.put(AgentConstants.IS_PACKAGE, "" + AgentConstants.DEFAULT_BPACKAGE);
        propertiesMap.put(AgentConstants.BUFFER_SIZE, "" + entity.getBuffersize());
        // TODO
        propertiesMap.put(AgentConstants.IS_WRITE, "" + AgentConstants.DEFAULT_IS_WRITE);
        // TODO
        propertiesMap.put(AgentConstants.IS_CALC_MD5, "" + (entity.getIsCalmd5() == 1));
        propertiesMap.put(AgentConstants.SEND_TIMEOUT_MILL_SEC,
                "" + entity.getSendTimeoutMillSec());
        propertiesMap.put(AgentConstants.FLUSH_EVENT_TIMEOUT_MILL_SEC,
                "" + entity.getFlushEventTimeoutMillSec());
        propertiesMap.put(AgentConstants.RPC_CLIENT_RECONNECT,
                "" + entity.getAgentRpcReconnectTime());
        propertiesMap.put(AgentConstants.FLOW_SIZE, "" + entity.getFlowSize());
        propertiesMap.put(AgentConstants.CLEAR_INTERVAL_SEC, "" + entity.getClearIntervalSec());
        propertiesMap.put(AgentConstants.CLEAR_DAY_OFFSET, "" + entity.getClearDayOffset());
        propertiesMap.put(AgentConstants.MIN_RETRY_THREADS, "" + entity.getMinRetryThreads());
        propertiesMap.put(AgentConstants.MAX_RETRY_THREADS, "" + entity.getMaxRetryThreads());
        propertiesMap.put(AgentConstants.EVENT_CHECK_INTERVAL,
                "" + entity.getEventCheckInterval());
        propertiesMap.put(AgentConstants.THREAD_MANAGER_SLEEP_INTERVAL,
                "" + entity.getThreadManagerSleepInterval());
        propertiesMap
                .put(AgentConstants.BUFFER_SIZE_IN_BYTES, "" + entity.getBufferSizeInBytes());
        propertiesMap.put(AgentConstants.CONF_REFRESH_INTERVAL_SECS, "" + entity.getConfRefreshIntervalSecs());
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        for (Map.Entry<String, String> entry : propertiesMap.entrySet()) {
            md.update(entry.getKey().getBytes(), 0, entry.getKey().length());
            md.update(entry.getValue().getBytes(), 0, entry.getValue().length());
        }

        /* Calculate md5. */
        byte[] mdbytes = md.digest();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < mdbytes.length; i++) {
            sb.append(Integer.toString((mdbytes[i] & 0xff) + 0x100, 16)
                    .substring(1));
        }
        //LOGGER.debug("conf md5 {}",sb.toString());
        return sb.toString();
    }
}
