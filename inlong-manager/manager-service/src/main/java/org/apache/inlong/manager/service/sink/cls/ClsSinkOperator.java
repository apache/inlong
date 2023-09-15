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

package org.apache.inlong.manager.service.sink.cls;

import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.dao.entity.DataNodeEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.dao.mapper.DataNodeEntityMapper;
import org.apache.inlong.manager.pojo.node.cls.ClsDataNodeDTO;
import org.apache.inlong.manager.pojo.sink.SinkField;
import org.apache.inlong.manager.pojo.sink.SinkRequest;
import org.apache.inlong.manager.pojo.sink.StreamSink;
import org.apache.inlong.manager.pojo.sink.cls.ClsSink;
import org.apache.inlong.manager.pojo.sink.cls.ClsSinkDTO;
import org.apache.inlong.manager.pojo.sink.cls.ClsSinkRequest;
import org.apache.inlong.manager.service.sink.AbstractSinkOperator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * Cloud log service sink operator
 */
@Service
public class ClsSinkOperator extends AbstractSinkOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClsSinkOperator.class);
    private static final String KEY_FIELDS = "fieldNames";
    private static final String SECRET_KEY = "secretKey";
    private static final String SECRET_ID = "secretId";
    private static final String END_POINT = "endpoint";
    private static final String TOPIC_ID = "topicId";

    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private DataNodeEntityMapper dataNodeEntityMapper;

    @Override
    protected void setTargetEntity(SinkRequest request, StreamSinkEntity targetEntity) {
        if (!this.getSinkType().equals(request.getSinkType())) {
            throw new BusinessException(ErrorCodeEnum.SINK_TYPE_NOT_SUPPORT,
                    ErrorCodeEnum.SINK_TYPE_NOT_SUPPORT.getMessage() + ": " + getSinkType());
        }
        ClsSinkRequest sinkRequest = (ClsSinkRequest) request;
        try {
            ClsSinkDTO dto = ClsSinkDTO.getFromRequest(sinkRequest, targetEntity.getExtParams());
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SINK_SAVE_FAILED,
                    String.format("serialize extParams of Doris SinkDTO failure: %s", e.getMessage()));
        }
    }

    @Override
    protected String getSinkType() {
        return SinkType.CLS;
    }

    @Override
    public Boolean accept(String sinkType) {
        return SinkType.CLS.equals(sinkType);
    }

    @Override
    public StreamSink getFromEntity(StreamSinkEntity entity) {
        ClsSink sink = new ClsSink();
        if (entity == null) {
            return sink;
        }

        ClsSinkDTO dto = ClsSinkDTO.getFromJson(entity.getExtParams());
        DataNodeEntity dataNodeEntity = dataNodeEntityMapper.selectByUniqueKey(entity.getDataNodeName(),
                DataNodeType.CLS);
        ClsDataNodeDTO clsDataNodeDTO = JsonUtils.parseObject(dataNodeEntity.getExtParams(),
                ClsDataNodeDTO.class);
        CommonBeanUtils.copyProperties(entity, sink, true);
        CommonBeanUtils.copyProperties(dto, sink, true);
        CommonBeanUtils.copyProperties(clsDataNodeDTO, sink, true);
        List<SinkField> sinkFields = getSinkFields(entity.getId());
        sink.setSinkFieldList(sinkFields);
        return sink;
    }

    @Override
    public Map<String, String> parse2IdParams(StreamSinkEntity streamSink, List<String> fields) {
        Map<String, String> params = super.parse2IdParams(streamSink, fields);
        ClsSinkDTO clsSinkDTO = JsonUtils.parseObject(streamSink.getExtParams(), ClsSinkDTO.class);
        params.put(TOPIC_ID, clsSinkDTO.getTopicId());
        DataNodeEntity dataNodeEntity = dataNodeEntityMapper.selectByUniqueKey(streamSink.getDataNodeName(),
                DataNodeType.CLS);
        ClsDataNodeDTO clsDataNodeDTO = JsonUtils.parseObject(dataNodeEntity.getExtParams(),
                ClsDataNodeDTO.class);
        params.put(SECRET_ID, clsDataNodeDTO.getSendSecretId());
        params.put(SECRET_KEY, clsDataNodeDTO.getSendSecretKey());
        params.put(END_POINT, clsDataNodeDTO.getEndpoint());
        StringBuilder fieldNames = new StringBuilder();
        for (String field : fields) {
            fieldNames.append(field).append(" ");
        }
        params.put(KEY_FIELDS, fieldNames.toString());
        return params;
    }
}
