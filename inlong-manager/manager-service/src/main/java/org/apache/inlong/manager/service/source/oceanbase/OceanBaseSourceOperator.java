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

package org.apache.inlong.manager.service.source.oceanbase;

import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.SourceType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.dao.entity.StreamSourceEntity;
import org.apache.inlong.manager.pojo.node.oceanbase.OceanBaseDataNodeInfo;
import org.apache.inlong.manager.pojo.source.SourceRequest;
import org.apache.inlong.manager.pojo.source.StreamSource;
import org.apache.inlong.manager.pojo.source.oceanbase.OceanBaseBinlogSource;
import org.apache.inlong.manager.pojo.source.oceanbase.OceanBaseBinlogSourceDTO;
import org.apache.inlong.manager.pojo.source.oceanbase.OceanBaseBinlogSourceRequest;
import org.apache.inlong.manager.pojo.stream.StreamField;
import org.apache.inlong.manager.service.source.AbstractSourceOperator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;

/**
 * Binlog source operator
 */
@Service
public class OceanBaseSourceOperator extends AbstractSourceOperator {

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public Boolean accept(String sourceType) {
        return SourceType.OCEANBASE.equals(sourceType);
    }

    @Override
    protected String getSourceType() {
        return SourceType.OCEANBASE;
    }

    @Override
    public String getExtParams(StreamSourceEntity sourceEntity) {
        OceanBaseBinlogSourceDTO OceanBaseBinlogSourceDTO = JsonUtils.parseObject(sourceEntity.getExtParams(),
                OceanBaseBinlogSourceDTO.class);
        if (Objects.nonNull(OceanBaseBinlogSourceDTO) && StringUtils.isBlank(OceanBaseBinlogSourceDTO.getHostname())) {
            OceanBaseDataNodeInfo dataNodeInfo = (OceanBaseDataNodeInfo) dataNodeService.get(
                    sourceEntity.getDataNodeName(), DataNodeType.OCEANBASE);
            CommonBeanUtils.copyProperties(dataNodeInfo, OceanBaseBinlogSourceDTO, true);
            OceanBaseBinlogSourceDTO.setPassword(dataNodeInfo.getToken());
            OceanBaseBinlogSourceDTO.setHostname(dataNodeInfo.getUrl().split(InlongConstants.COLON)[0]);
            OceanBaseBinlogSourceDTO.setPort(Integer.valueOf(dataNodeInfo.getUrl().split(InlongConstants.COLON)[1]));
            return JsonUtils.toJsonString(OceanBaseBinlogSourceDTO);
        }
        return sourceEntity.getExtParams();
    }

    @Override
    protected void setTargetEntity(SourceRequest request, StreamSourceEntity targetEntity) {
        OceanBaseBinlogSourceRequest sourceRequest = (OceanBaseBinlogSourceRequest) request;
        CommonBeanUtils.copyProperties(sourceRequest, targetEntity, true);
        try {
            OceanBaseBinlogSourceDTO dto =
                    OceanBaseBinlogSourceDTO.getFromRequest(sourceRequest, targetEntity.getExtParams());
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT,
                    String.format("serialize extParams of OceanBaseBinlog SourceDTO failure: %s", e.getMessage()));
        }
    }

    @Override
    public StreamSource getFromEntity(StreamSourceEntity entity) {
        OceanBaseBinlogSource source = new OceanBaseBinlogSource();
        if (entity == null) {
            return source;
        }

        OceanBaseBinlogSourceDTO dto = OceanBaseBinlogSourceDTO.getFromJson(entity.getExtParams());
        if (StringUtils.isBlank(dto.getHostname())) {
            if (StringUtils.isBlank(entity.getDataNodeName())) {
                throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT,
                        "OceanBase url and data node is blank");
            }
            OceanBaseDataNodeInfo dataNodeInfo = (OceanBaseDataNodeInfo) dataNodeService.get(
                    entity.getDataNodeName(), DataNodeType.OCEANBASE);
            CommonBeanUtils.copyProperties(dataNodeInfo, dto, true);
            dto.setPassword(dataNodeInfo.getToken());
            dto.setHostname(dataNodeInfo.getUrl().split(InlongConstants.COLON)[0]);
            dto.setPort(Integer.valueOf(dataNodeInfo.getUrl().split(InlongConstants.COLON)[1]));
        }
        CommonBeanUtils.copyProperties(entity, source, true);
        CommonBeanUtils.copyProperties(dto, source, true);

        List<StreamField> sourceFields = super.getSourceFields(entity.getId());
        source.setFieldList(sourceFields);
        return source;
    }

}
