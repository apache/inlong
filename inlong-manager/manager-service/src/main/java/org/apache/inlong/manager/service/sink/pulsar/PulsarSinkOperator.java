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

package org.apache.inlong.manager.service.sink.pulsar;

import org.apache.inlong.common.pojo.sort.dataflow.field.FieldConfig;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.sink.PulsarSinkConfig;
import org.apache.inlong.common.pojo.sort.dataflow.sink.SinkConfig;
import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.dao.entity.DataNodeEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.dao.mapper.DataNodeEntityMapper;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.node.DataNodeInfo;
import org.apache.inlong.manager.pojo.node.pulsar.PulsarDataNodeDTO;
import org.apache.inlong.manager.pojo.sink.SinkField;
import org.apache.inlong.manager.pojo.sink.SinkRequest;
import org.apache.inlong.manager.pojo.sink.StreamSink;
import org.apache.inlong.manager.pojo.sink.pulsar.PulsarSink;
import org.apache.inlong.manager.pojo.sink.pulsar.PulsarSinkDTO;
import org.apache.inlong.manager.pojo.sink.pulsar.PulsarSinkRequest;
import org.apache.inlong.manager.pojo.sort.util.FieldInfoUtils;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.service.sink.AbstractSinkOperator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.inlong.manager.common.consts.InlongConstants.PULSAR_TOPIC_FORMAT;

/**
 * Pulsar sink operator
 */
@Service
public class PulsarSinkOperator extends AbstractSinkOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarSinkOperator.class);

    private static final String TOPIC = "topic";
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private DataNodeEntityMapper dataNodeEntityMapper;

    @Override
    public Boolean accept(String sinkType) {
        return SinkType.PULSAR.equals(sinkType);
    }

    @Override
    protected String getSinkType() {
        return SinkType.PULSAR;
    }

    @Override
    protected void setTargetEntity(SinkRequest request, StreamSinkEntity targetEntity) {
        if (!this.getSinkType().equals(request.getSinkType())) {
            throw new BusinessException(ErrorCodeEnum.SINK_TYPE_NOT_SUPPORT,
                    ErrorCodeEnum.SINK_TYPE_NOT_SUPPORT.getMessage() + ": " + getSinkType());
        }
        PulsarSinkRequest sinkRequest = (PulsarSinkRequest) request;
        try {
            PulsarSinkDTO dto = PulsarSinkDTO.getFromRequest(sinkRequest, targetEntity.getExtParams());
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SINK_SAVE_FAILED,
                    String.format("serialize extParams of Pulsar SinkDTO failure: %s", e.getMessage()));
        }
    }

    @Override
    public StreamSink getFromEntity(StreamSinkEntity entity) {
        PulsarSink sink = new PulsarSink();
        if (entity == null) {
            return sink;
        }
        DataNodeEntity dataNodeEntity = dataNodeEntityMapper.selectByUniqueKey(entity.getDataNodeName(),
                DataNodeType.PULSAR);
        PulsarDataNodeDTO pulsarDataNodeDTO = JsonUtils.parseObject(dataNodeEntity.getExtParams(),
                PulsarDataNodeDTO.class);
        PulsarSinkDTO dto = PulsarSinkDTO.getFromJson(entity.getExtParams());
        CommonBeanUtils.copyProperties(entity, sink, true);
        CommonBeanUtils.copyProperties(dto, sink, true);
        CommonBeanUtils.copyProperties(pulsarDataNodeDTO, sink, true);
        List<SinkField> sinkFields = super.getSinkFields(entity.getId());
        sink.setSinkFieldList(sinkFields);
        return sink;
    }

    @Override
    public Map<String, String> parse2IdParams(StreamSinkEntity streamSink, List<String> fields,
            DataNodeInfo dataNodeInfo) {

        Map<String, String> params = super.parse2IdParams(streamSink, fields, dataNodeInfo);
        PulsarSinkDTO pulsarSinkDTO;
        try {
            pulsarSinkDTO = objectMapper.readValue(streamSink.getExtParams(), PulsarSinkDTO.class);
        } catch (JsonProcessingException e) {
            LOGGER.error("parse pulsar sink dto error", e);
            return params;
        }
        String fullTopicName = getFullTopicName(pulsarSinkDTO);
        params.put(TOPIC, fullTopicName);
        return params;
    }

    private String getFullTopicName(PulsarSinkDTO pulsarSinkDTO) {
        return String.format(PULSAR_TOPIC_FORMAT, pulsarSinkDTO.getPulsarTenant(), pulsarSinkDTO.getNamespace(),
                pulsarSinkDTO.getTopic());

    }

    @Override
    public SinkConfig getSinkConfig(InlongGroupInfo groupInfo, InlongStreamInfo streamInfo, StreamSink sink) {
        PulsarSink pulsarSink = (PulsarSink) sink;
        PulsarSinkConfig sinkConfig = CommonBeanUtils.copyProperties(pulsarSink, PulsarSinkConfig::new);
        List<FieldConfig> fields = sinkFieldMapper.selectBySinkId(sink.getId()).stream().map(
                v -> {
                    FieldConfig fieldConfig = new FieldConfig();
                    FormatInfo formatInfo = FieldInfoUtils.convertFieldFormat(
                            v.getFieldType().toLowerCase());
                    fieldConfig.setName(v.getFieldName());
                    fieldConfig.setFormatInfo(formatInfo);
                    return fieldConfig;
                }).collect(Collectors.toList());
        sinkConfig.setFieldConfigs(fields);
        return sinkConfig;
    }

}
