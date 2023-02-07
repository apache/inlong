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

package org.apache.inlong.manager.service.sink.kudu;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.pojo.sink.SinkField;
import org.apache.inlong.manager.pojo.sink.SinkRequest;
import org.apache.inlong.manager.pojo.sink.StreamSink;
import org.apache.inlong.manager.pojo.sink.kudu.KuduSink;
import org.apache.inlong.manager.pojo.sink.kudu.KuduSinkDTO;
import org.apache.inlong.manager.pojo.sink.kudu.KuduSinkRequest;
import org.apache.inlong.manager.service.sink.AbstractSinkOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Kudu sink operator, such as save or update Kudu field, etc.
 */
@Service
public class KuduSinkOperator extends AbstractSinkOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(KuduSinkOperator.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public Boolean accept(String sinkType) {
        return SinkType.KUDU.equals(sinkType);
    }

    @Override
    protected String getSinkType() {
        return SinkType.KUDU;
    }

    @Override
    protected void setTargetEntity(SinkRequest request, StreamSinkEntity targetEntity) {
        if (!this.getSinkType().equals(request.getSinkType())) {
            throw new BusinessException(ErrorCodeEnum.SINK_TYPE_NOT_SUPPORT,
                    ErrorCodeEnum.SINK_TYPE_NOT_SUPPORT.getMessage() + ": " + getSinkType());
        }
        KuduSinkRequest sinkRequest = (KuduSinkRequest) request;
        String masters = sinkRequest.getMasters();
        if (StringUtils.isBlank(masters)) {
            throw new BusinessException(ErrorCodeEnum.SINK_SAVE_FAILED, "masters can not be empty!");
        }
        if (masters.contains(InlongConstants.SEMICOLON)) {
            throw new BusinessException(ErrorCodeEnum.SINK_SAVE_FAILED, "masters can not contain comma!");
        }

        String partitionKey = sinkRequest.getPartitionKey();
        boolean partitionKeyExist = StringUtils.isNotBlank(partitionKey);
        if (partitionKeyExist) {
            Set<String> fieldNames = sinkRequest.getSinkFieldList().stream().map(SinkField::getFieldName)
                    .collect(Collectors.toSet());
            List<String> partitionKeys = Arrays.asList(partitionKey.split(InlongConstants.COMMA));
            if (!CollectionUtils.isSubCollection(partitionKeys, fieldNames)) {
                throw new BusinessException(ErrorCodeEnum.SINK_SAVE_FAILED,
                        String.format("The partitionKey(%s) must be included in the sinkFieldList(%s)",
                                partitionKey, fieldNames));
            }
        }

        try {
            KuduSinkDTO dto = KuduSinkDTO.getFromRequest(sinkRequest);
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SINK_SAVE_FAILED,
                    String.format("serialize extParams of Kudu SinkDTO failure: %s", e.getMessage()));
        }

    }

    @Override
    public StreamSink getFromEntity(StreamSinkEntity entity) {
        KuduSink sink = new KuduSink();
        if (entity == null) {
            return sink;
        }

        KuduSinkDTO dto = KuduSinkDTO.getFromJson(entity.getExtParams());

        CommonBeanUtils.copyProperties(entity, sink, true);
        CommonBeanUtils.copyProperties(dto, sink, true);
        List<SinkField> sinkFields = super.getSinkFields(entity.getId());
        sink.setSinkFieldList(sinkFields);
        return sink;
    }

}
