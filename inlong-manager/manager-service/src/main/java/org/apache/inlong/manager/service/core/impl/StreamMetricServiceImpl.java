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
import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamMetricRequest;
import org.apache.inlong.manager.dao.entity.StreamMetricEntity;
import org.apache.inlong.manager.dao.mapper.StreamMetricEntityMapper;
import org.apache.inlong.manager.service.core.StreamMetricService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class StreamMetricServiceImpl extends AbstractService<StreamMetricEntity>
        implements StreamMetricService {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamMetricServiceImpl.class);

    @Autowired
    private StreamMetricEntityMapper streamMetricEntityMapper;

    @Override
    public String reportMetric(InlongStreamMetricRequest entity) {
        if (putData(convertData(entity))) {
            return "Receive success";
        } else {
            LOGGER.warn("Receive Queue is full, data will be discarded !");
            return "Receive Queue is full, data will be discarded !";
        }
    }

    public boolean batchInsertEntities(List<StreamMetricEntity> entryList) {
        streamMetricEntityMapper.insertList(entryList);
        return false;
    }

    public StreamMetricEntity convertData(InlongStreamMetricRequest request) {
        StreamMetricEntity entity = new StreamMetricEntity();
        entity.setComponentName(request.getComponentName());
        entity.setInlongGroupId(request.getInlongGroupId());
        entity.setInlongStreamId(request.getInlongStreamId());
        entity.setMetricInfo(request.getMetricInfo());
        entity.setReportTime(request.getReportTime());
        entity.setVersion(request.getVersion());
        entity.setMetricName(request.getMetricName());
        return entity;
    }
}
