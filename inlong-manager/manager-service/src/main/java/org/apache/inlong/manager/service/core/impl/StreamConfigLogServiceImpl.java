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

import java.util.List;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamConfigLogRequest;
import org.apache.inlong.manager.dao.entity.StreamConfigLogEntity;
import org.apache.inlong.manager.dao.mapper.StreamConfigLogEntityMapper;
import org.apache.inlong.manager.service.core.StreamConfigLogService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class StreamConfigLogServiceImpl extends AbstractService<StreamConfigLogEntity>
        implements StreamConfigLogService {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamConfigLogServiceImpl.class);

    @Autowired
    private StreamConfigLogEntityMapper streamConfigLogEntityMapper;

    @Override
    public String reportConfigLog(InlongStreamConfigLogRequest request) {
        if (putData(convertData(request))) {
            return "Receive success";
        } else {
            LOGGER.warn("Receive Queue is full, data will be discarded !");
            return "Receive Queue is full, data will be discarded !";
        }
    }

    public boolean batchInsertEntities(List<StreamConfigLogEntity> entryList) {
        streamConfigLogEntityMapper.insertList(entryList);
        return true;
    }

    public StreamConfigLogEntity convertData(InlongStreamConfigLogRequest request) {
        StreamConfigLogEntity entity = new StreamConfigLogEntity();
        entity.setComponentName(request.getComponentName());
        entity.setInlongGroupId(request.getInlongGroupId());
        entity.setInlongStreamId(request.getInlongStreamId());
        entity.setConfigName(request.getConfigName());
        entity.setReportTime(request.getReportTime());
        entity.setVersion(request.getVersion());
        entity.setLogInfo(request.getLogInfo());
        entity.setLogType(request.getLogType());
        entity.setIp(request.getIp());
        return entity;
    }
}
