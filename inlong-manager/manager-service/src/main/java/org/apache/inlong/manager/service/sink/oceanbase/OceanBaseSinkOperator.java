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

package org.apache.inlong.manager.service.sink.oceanbase;

import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.pojo.node.DataNodeInfo;
import org.apache.inlong.manager.pojo.node.oceanbase.OceanBaseDataNodeDTO;
import org.apache.inlong.manager.pojo.sink.SinkField;
import org.apache.inlong.manager.pojo.sink.SinkRequest;
import org.apache.inlong.manager.pojo.sink.StreamSink;
import org.apache.inlong.manager.pojo.sink.oceanbase.OceanBaseSink;
import org.apache.inlong.manager.pojo.sink.oceanbase.OceanBaseSinkDTO;
import org.apache.inlong.manager.pojo.sink.oceanbase.OceanBaseSinkRequest;
import org.apache.inlong.manager.service.sink.AbstractSinkOperator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * OceanBase sink operator
 */
@Service
public class OceanBaseSinkOperator extends AbstractSinkOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(OceanBaseSinkOperator.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public Boolean accept(String sinkType) {
        return SinkType.OCEANBASE.equals(sinkType);
    }

    @Override
    protected String getSinkType() {
        return SinkType.OCEANBASE;
    }

    @Override
    protected void setTargetEntity(SinkRequest request, StreamSinkEntity targetEntity) {
        if (!this.getSinkType().equals(request.getSinkType())) {
            throw new BusinessException(ErrorCodeEnum.SINK_TYPE_NOT_SUPPORT,
                    ErrorCodeEnum.SINK_TYPE_NOT_SUPPORT.getMessage() + ": " + getSinkType());
        }
        OceanBaseSinkRequest sinkRequest = (OceanBaseSinkRequest) request;
        try {
            OceanBaseSinkDTO dto = OceanBaseSinkDTO.getFromRequest(sinkRequest, targetEntity.getExtParams());
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SINK_SAVE_FAILED,
                    String.format("serialize extParams of OceanBase SinkDTO failure: %s", e.getMessage()));
        }
    }

    @Override
    public StreamSink getFromEntity(StreamSinkEntity entity) {
        OceanBaseSink sink = new OceanBaseSink();
        if (entity == null) {
            return sink;
        }

        OceanBaseSinkDTO dto = OceanBaseSinkDTO.getFromJson(entity.getExtParams());
        if (StringUtils.isBlank(dto.getJdbcUrl())) {
            if (StringUtils.isBlank(entity.getDataNodeName())) {
                throw new BusinessException(ErrorCodeEnum.SINK_INFO_INCORRECT,
                        "OceanBase jdbc url not specified and data node is blank");
            }
            DataNodeInfo dataNodeInfo = dataNodeHelper.getDataNodeInfo(
                    entity.getDataNodeName(), entity.getSinkType());
            CommonBeanUtils.copyProperties(dataNodeInfo, dto, true);
            dto.setJdbcUrl(OceanBaseDataNodeDTO.convertToJdbcurl(dataNodeInfo.getUrl()));
            dto.setPassword(dataNodeInfo.getToken());
        }
        CommonBeanUtils.copyProperties(entity, sink, true);
        CommonBeanUtils.copyProperties(dto, sink, true);
        List<SinkField> sinkFields = super.getSinkFields(entity.getId());
        sink.setSinkFieldList(sinkFields);
        return sink;
    }

}
