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

package org.apache.inlong.manager.service.source.binlog;

import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.SourceType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.dao.entity.StreamSourceEntity;
import org.apache.inlong.manager.pojo.node.mysql.MySQLDataNodeInfo;
import org.apache.inlong.manager.pojo.source.SourceRequest;
import org.apache.inlong.manager.pojo.source.StreamSource;
import org.apache.inlong.manager.pojo.source.mysql.MySQLBinlogSource;
import org.apache.inlong.manager.pojo.source.mysql.MySQLBinlogSourceDTO;
import org.apache.inlong.manager.pojo.source.mysql.MySQLBinlogSourceRequest;
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
public class BinlogSourceOperator extends AbstractSourceOperator {

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public Boolean accept(String sourceType) {
        return SourceType.MYSQL_BINLOG.equals(sourceType);
    }

    @Override
    protected String getSourceType() {
        return SourceType.MYSQL_BINLOG;
    }

    @Override
    public String getExtParams(StreamSourceEntity sourceEntity) {
        MySQLBinlogSourceDTO mySQLBinlogSourceDTO = JsonUtils.parseObject(sourceEntity.getExtParams(),
                MySQLBinlogSourceDTO.class);
        if (Objects.nonNull(mySQLBinlogSourceDTO) && StringUtils.isBlank(mySQLBinlogSourceDTO.getHostname())) {
            MySQLDataNodeInfo dataNodeInfo = (MySQLDataNodeInfo) dataNodeService.get(
                    sourceEntity.getDataNodeName(), DataNodeType.MYSQL);
            CommonBeanUtils.copyProperties(dataNodeInfo, mySQLBinlogSourceDTO, true);
            mySQLBinlogSourceDTO.setUser(dataNodeInfo.getUsername());
            mySQLBinlogSourceDTO.setPassword(dataNodeInfo.getToken());
            mySQLBinlogSourceDTO.setHostname(dataNodeInfo.getUrl().split(InlongConstants.COLON)[0]);
            mySQLBinlogSourceDTO.setPort(Integer.valueOf(dataNodeInfo.getUrl().split(InlongConstants.COLON)[1]));
            return JsonUtils.toJsonString(mySQLBinlogSourceDTO);
        }
        return sourceEntity.getExtParams();
    }

    @Override
    protected void setTargetEntity(SourceRequest request, StreamSourceEntity targetEntity) {
        MySQLBinlogSourceRequest sourceRequest = (MySQLBinlogSourceRequest) request;
        CommonBeanUtils.copyProperties(sourceRequest, targetEntity, true);
        try {
            MySQLBinlogSourceDTO dto = MySQLBinlogSourceDTO.getFromRequest(sourceRequest, targetEntity.getExtParams());
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT,
                    String.format("serialize extParams of MySQLBinlog SourceDTO failure: %s", e.getMessage()));
        }
    }

    @Override
    public StreamSource getFromEntity(StreamSourceEntity entity) {
        MySQLBinlogSource source = new MySQLBinlogSource();
        if (entity == null) {
            return source;
        }

        MySQLBinlogSourceDTO dto = MySQLBinlogSourceDTO.getFromJson(entity.getExtParams());
        if (StringUtils.isBlank(dto.getHostname())) {
            if (StringUtils.isBlank(entity.getDataNodeName())) {
                throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT,
                        "mysql url and data node is blank");
            }
            MySQLDataNodeInfo dataNodeInfo = (MySQLDataNodeInfo) dataNodeService.get(
                    entity.getDataNodeName(), DataNodeType.MYSQL);
            CommonBeanUtils.copyProperties(dataNodeInfo, dto, true);
            dto.setUser(dataNodeInfo.getUsername());
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
