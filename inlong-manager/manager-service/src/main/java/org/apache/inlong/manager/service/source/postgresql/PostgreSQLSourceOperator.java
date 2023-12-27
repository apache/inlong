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

package org.apache.inlong.manager.service.source.postgresql;

import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.SourceType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.dao.entity.StreamSourceEntity;
import org.apache.inlong.manager.pojo.node.postgresql.PostgreSQLDataNodeInfo;
import org.apache.inlong.manager.pojo.source.SourceRequest;
import org.apache.inlong.manager.pojo.source.StreamSource;
import org.apache.inlong.manager.pojo.source.postgresql.PostgreSQLSource;
import org.apache.inlong.manager.pojo.source.postgresql.PostgreSQLSourceDTO;
import org.apache.inlong.manager.pojo.source.postgresql.PostgreSQLSourceRequest;
import org.apache.inlong.manager.pojo.stream.StreamField;
import org.apache.inlong.manager.service.source.AbstractSourceOperator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * PostgreSQL stream source operator
 */
@Service
public class PostgreSQLSourceOperator extends AbstractSourceOperator {

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public Boolean accept(String sourceType) {
        return SourceType.POSTGRESQL.equals(sourceType);
    }

    @Override
    protected String getSourceType() {
        return SourceType.POSTGRESQL;
    }

    @Override
    public String getExtParams(StreamSourceEntity sourceEntity) {
        PostgreSQLSourceDTO postgreSQLSourceDTO = JsonUtils.parseObject(sourceEntity.getExtParams(),
                PostgreSQLSourceDTO.class);
        if (java.util.Objects.nonNull(postgreSQLSourceDTO) && StringUtils.isBlank(postgreSQLSourceDTO.getHostname())) {
            PostgreSQLDataNodeInfo dataNodeInfo = (PostgreSQLDataNodeInfo) dataNodeService.get(
                    sourceEntity.getDataNodeName(), DataNodeType.POSTGRESQL);
            CommonBeanUtils.copyProperties(dataNodeInfo, postgreSQLSourceDTO, true);
            postgreSQLSourceDTO.setHostname(dataNodeInfo.getUrl().split(InlongConstants.COLON)[0]);
            postgreSQLSourceDTO.setPort(Integer.valueOf(dataNodeInfo.getUrl().split(InlongConstants.COLON)[1]));
            postgreSQLSourceDTO.setPassword(dataNodeInfo.getToken());
            return JsonUtils.toJsonString(postgreSQLSourceDTO);
        }
        return sourceEntity.getExtParams();
    }

    @Override
    protected void setTargetEntity(SourceRequest request, StreamSourceEntity targetEntity) {
        PostgreSQLSourceRequest sourceRequest = (PostgreSQLSourceRequest) request;
        CommonBeanUtils.copyProperties(sourceRequest, targetEntity, true);
        try {
            PostgreSQLSourceDTO dto = PostgreSQLSourceDTO.getFromRequest(sourceRequest, targetEntity.getExtParams());
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT,
                    String.format("serialize extParams of PostgreSQL SourceDTO failure: %s", e.getMessage()));
        }
    }

    @Override
    public StreamSource getFromEntity(StreamSourceEntity entity) {
        PostgreSQLSource source = new PostgreSQLSource();
        if (entity == null) {
            return source;
        }

        PostgreSQLSourceDTO dto = PostgreSQLSourceDTO.getFromJson(entity.getExtParams());
        if (StringUtils.isBlank(dto.getHostname())) {
            if (StringUtils.isBlank(entity.getDataNodeName())) {
                throw new BusinessException(ErrorCodeEnum.SINK_INFO_INCORRECT,
                        "postgreSQl hostname unspecified and data node is blank");
            }
            PostgreSQLDataNodeInfo dataNodeInfo = (PostgreSQLDataNodeInfo) dataNodeService.get(
                    entity.getDataNodeName(), DataNodeType.POSTGRESQL);
            CommonBeanUtils.copyProperties(dataNodeInfo, dto, true);
            dto.setHostname(dataNodeInfo.getUrl().split(InlongConstants.COLON)[0]);
            dto.setPort(Integer.valueOf(dataNodeInfo.getUrl().split(InlongConstants.COLON)[1]));
            dto.setPassword(dataNodeInfo.getToken());
        }
        CommonBeanUtils.copyProperties(entity, source, true);
        CommonBeanUtils.copyProperties(dto, source, true);

        List<StreamField> sourceFields = super.getSourceFields(entity.getId());
        source.setFieldList(sourceFields);
        return source;
    }

}
