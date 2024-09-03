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

package org.apache.inlong.manager.service.sink.http;

import org.apache.inlong.common.pojo.sort.dataflow.field.FieldConfig;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.sink.HttpSinkConfig;
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
import org.apache.inlong.manager.pojo.node.http.HttpDataNodeDTO;
import org.apache.inlong.manager.pojo.sink.SinkField;
import org.apache.inlong.manager.pojo.sink.SinkRequest;
import org.apache.inlong.manager.pojo.sink.StreamSink;
import org.apache.inlong.manager.pojo.sink.http.HttpSink;
import org.apache.inlong.manager.pojo.sink.http.HttpSinkDTO;
import org.apache.inlong.manager.pojo.sink.http.HttpSinkRequest;
import org.apache.inlong.manager.pojo.sort.util.FieldInfoUtils;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.service.sink.AbstractSinkOperator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Http sink operator
 */
@Service
public class HttpSinkOperator extends AbstractSinkOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSinkOperator.class);

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
        HttpSinkRequest sinkRequest = (HttpSinkRequest) request;
        try {
            HttpSinkDTO dto = HttpSinkDTO.getFromRequest(sinkRequest, targetEntity.getExtParams());
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SINK_SAVE_FAILED,
                    String.format("serialize extParams of http SinkDTO failure: %s", e.getMessage()));
        }
    }

    @Override
    protected String getSinkType() {
        return SinkType.HTTP;
    }

    @Override
    public Boolean accept(String sinkType) {
        return SinkType.HTTP.equals(sinkType);
    }

    @Override
    public StreamSink getFromEntity(StreamSinkEntity entity) {
        HttpSink sink = new HttpSink();
        if (entity == null) {
            return sink;
        }

        HttpSinkDTO dto = HttpSinkDTO.getFromJson(entity.getExtParams());
        DataNodeEntity dataNodeEntity = dataNodeEntityMapper.selectByUniqueKey(entity.getDataNodeName(),
                DataNodeType.HTTP);
        HttpDataNodeDTO httpDataNodeDTO = JsonUtils.parseObject(dataNodeEntity.getExtParams(),
                HttpDataNodeDTO.class);
        CommonBeanUtils.copyProperties(entity, sink, true);
        CommonBeanUtils.copyProperties(dto, sink, true);
        CommonBeanUtils.copyProperties(httpDataNodeDTO, sink, true);
        List<SinkField> sinkFields = getSinkFields(entity.getId());
        sink.setSinkFieldList(sinkFields);
        return sink;
    }

    @Override
    public SinkConfig getSinkConfig(InlongGroupInfo groupInfo, InlongStreamInfo streamInfo, StreamSink sink) {
        HttpSink httpSink = (HttpSink) sink;
        HttpSinkConfig sinkConfig = CommonBeanUtils.copyProperties(httpSink, HttpSinkConfig::new);
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
