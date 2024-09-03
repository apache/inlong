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

import org.apache.inlong.common.pojo.sort.dataflow.field.FieldConfig;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.sink.ClsSinkConfig;
import org.apache.inlong.common.pojo.sort.dataflow.sink.SinkConfig;
import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.dao.entity.DataNodeEntity;
import org.apache.inlong.manager.dao.entity.InlongStreamEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.dao.mapper.DataNodeEntityMapper;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.node.DataNodeInfo;
import org.apache.inlong.manager.pojo.node.cls.ClsDataNodeDTO;
import org.apache.inlong.manager.pojo.node.cls.ClsDataNodeInfo;
import org.apache.inlong.manager.pojo.sink.SinkField;
import org.apache.inlong.manager.pojo.sink.SinkRequest;
import org.apache.inlong.manager.pojo.sink.StreamSink;
import org.apache.inlong.manager.pojo.sink.cls.ClsSink;
import org.apache.inlong.manager.pojo.sink.cls.ClsSinkDTO;
import org.apache.inlong.manager.pojo.sink.cls.ClsSinkRequest;
import org.apache.inlong.manager.pojo.sort.util.FieldInfoUtils;
import org.apache.inlong.manager.pojo.stream.InlongStreamExtParam;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.service.sink.AbstractSinkOperator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

            InlongStreamEntity stream = inlongStreamEntityMapper
                    .selectByIdentifier(request.getInlongGroupId(), request.getInlongStreamId());
            dto.setSeparator(String.valueOf((char) (Integer.parseInt(stream.getDataSeparator()))));

            InlongStreamExtParam streamExt =
                    JsonUtils.parseObject(stream.getExtParams(), InlongStreamExtParam.class);
            dto.setFieldOffset(streamExt.getExtendedFieldSize());

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
    public Map<String, String> parse2IdParams(StreamSinkEntity streamSink, List<String> fields,
            DataNodeInfo dataNodeInfo) {
        Map<String, String> params = super.parse2IdParams(streamSink, fields, dataNodeInfo);
        ClsSinkDTO clsSinkDTO = JsonUtils.parseObject(streamSink.getExtParams(), ClsSinkDTO.class);
        params.computeIfAbsent(TOPIC_ID, k -> clsSinkDTO.getTopicId());
        ClsDataNodeInfo clsDataNodeInfo = (ClsDataNodeInfo) dataNodeInfo;
        params.computeIfAbsent(SECRET_ID, k -> clsDataNodeInfo.getSendSecretId());
        params.computeIfAbsent(SECRET_KEY, k -> clsDataNodeInfo.getSendSecretKey());
        params.computeIfAbsent(END_POINT, k -> clsDataNodeInfo.getEndpoint());
        StringBuilder fieldNames = new StringBuilder();
        for (String field : fields) {
            fieldNames.append(field).append(InlongConstants.BLANK);
        }
        params.computeIfAbsent(KEY_FIELDS, k -> fieldNames.toString());
        return params;
    }

    @Override
    public SinkConfig getSinkConfig(InlongGroupInfo groupInfo, InlongStreamInfo streamInfo, StreamSink sink) {
        ClsSink clsSink = (ClsSink) sink;
        ClsSinkConfig sinkConfig = CommonBeanUtils.copyProperties(clsSink, ClsSinkConfig::new);
        sinkConfig.setSeparator(String.valueOf((char) (Integer.parseInt(streamInfo.getDataSeparator()))));
        sinkConfig.setFieldOffset(streamInfo.getExtendedFieldSize());
        sinkConfig.setContentOffset(0);
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
