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

package org.apache.inlong.manager.service.source.iceberg;

import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.SourceType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.dao.entity.InlongStreamFieldEntity;
import org.apache.inlong.manager.dao.entity.StreamSourceEntity;
import org.apache.inlong.manager.pojo.node.iceberg.IcebergDataNodeInfo;
import org.apache.inlong.manager.pojo.sink.iceberg.IcebergColumnInfo;
import org.apache.inlong.manager.pojo.sort.util.FieldInfoUtils;
import org.apache.inlong.manager.pojo.source.SourceRequest;
import org.apache.inlong.manager.pojo.source.StreamSource;
import org.apache.inlong.manager.pojo.source.iceberg.IcebergSource;
import org.apache.inlong.manager.pojo.source.iceberg.IcebergSourceDTO;
import org.apache.inlong.manager.pojo.source.iceberg.IcebergSourceRequest;
import org.apache.inlong.manager.pojo.stream.StreamField;
import org.apache.inlong.manager.service.resource.sink.iceberg.IcebergCatalogUtils;
import org.apache.inlong.manager.service.source.AbstractSourceOperator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Iceberg stream source operator
 */
@Service
public class IcebergSourceOperator extends AbstractSourceOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(IcebergSourceOperator.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public Boolean accept(String sourceType) {
        return SourceType.ICEBERG.equals(sourceType);
    }

    @Override
    protected String getSourceType() {
        return SourceType.ICEBERG;
    }

    @Override
    public String getExtParams(StreamSourceEntity sourceEntity) {
        IcebergSourceDTO icebergSourceDTO = JsonUtils.parseObject(sourceEntity.getExtParams(),
                IcebergSourceDTO.class);
        if (Objects.nonNull(icebergSourceDTO) && StringUtils.isBlank(icebergSourceDTO.getUri())) {
            IcebergDataNodeInfo dataNodeInfo = (IcebergDataNodeInfo) dataNodeService.get(
                    sourceEntity.getDataNodeName(), DataNodeType.ICEBERG);
            CommonBeanUtils.copyProperties(dataNodeInfo, icebergSourceDTO, true);
            icebergSourceDTO.setUri(dataNodeInfo.getUrl());
            return JsonUtils.toJsonString(icebergSourceDTO);
        }
        return sourceEntity.getExtParams();
    }

    @Override
    protected void setTargetEntity(SourceRequest request, StreamSourceEntity targetEntity) {
        IcebergSourceRequest sourceRequest = (IcebergSourceRequest) request;
        CommonBeanUtils.copyProperties(sourceRequest, targetEntity, true);
        try {
            IcebergSourceDTO dto = IcebergSourceDTO.getFromRequest(sourceRequest, targetEntity.getExtParams());
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT,
                    String.format("serialize extParams of Kafka SourceDTO failure: %s", e.getMessage()));
        }
    }

    @Override
    public StreamSource getFromEntity(StreamSourceEntity entity) {
        IcebergSource source = new IcebergSource();
        if (entity == null) {
            return source;
        }

        IcebergSourceDTO dto = IcebergSourceDTO.getFromJson(entity.getExtParams());
        if (StringUtils.isBlank(dto.getUri())) {
            if (StringUtils.isBlank(entity.getDataNodeName())) {
                throw new BusinessException(ErrorCodeEnum.SINK_INFO_INCORRECT,
                        "iceberg catalog uri unspecified and data node is blank");
            }
            IcebergDataNodeInfo dataNodeInfo = (IcebergDataNodeInfo) dataNodeService.get(
                    entity.getDataNodeName(), DataNodeType.ICEBERG);
            CommonBeanUtils.copyProperties(dataNodeInfo, dto, true);
            dto.setUri(dataNodeInfo.getUrl());
        }
        CommonBeanUtils.copyProperties(entity, source, true);
        CommonBeanUtils.copyProperties(dto, source, true);

        List<StreamField> sourceFields = super.getSourceFields(entity.getId());
        source.setFieldList(sourceFields);
        return source;
    }

    @Override
    @Transactional(rollbackFor = Throwable.class, isolation = Isolation.REPEATABLE_READ)
    public void syncSourceFieldInfo(SourceRequest request, String operator) {
        IcebergSourceRequest sourceRequest = (IcebergSourceRequest) request;

        LOGGER.info("sync source field for iceberg {}", sourceRequest);
        String metastoreUri = sourceRequest.getUri();
        String dbName = sourceRequest.getDatabase();
        String tableName = sourceRequest.getTableName();
        boolean tableExists = IcebergCatalogUtils.tableExists(metastoreUri, dbName, tableName);
        List<StreamField> streamFields = new ArrayList<>();
        if (tableExists) {
            List<IcebergColumnInfo> existColumns = IcebergCatalogUtils.getColumns(metastoreUri, dbName, tableName);
            for (IcebergColumnInfo columnInfo : existColumns) {
                StreamField streamField = new StreamField();
                streamField.setFieldName(columnInfo.getFieldName());
                streamField.setFieldType(FieldInfoUtils.sqlTypeToJavaTypeStr(columnInfo.getFieldType()));
                streamField.setFieldComment(columnInfo.getFieldComment());
                streamFields.add(streamField);
            }
            updateField(sourceRequest.getInlongGroupId(), sourceRequest.getInlongStreamId(), streamFields);
        }
    }

    public void updateField(String groupId, String streamId, List<StreamField> fieldList) {
        LOGGER.debug("begin to update inlong stream field, groupId={}, streamId={}, field={}", groupId, streamId,
                fieldList);
        try {
            streamFieldMapper.deleteAllByIdentifier(groupId, streamId);
            if (CollectionUtils.isEmpty(fieldList)) {
                return;
            }
            fieldList.forEach(streamField -> streamField.setId(null));
            List<InlongStreamFieldEntity> list = CommonBeanUtils.copyListProperties(fieldList,
                    InlongStreamFieldEntity::new);
            for (InlongStreamFieldEntity entity : list) {
                entity.setInlongGroupId(groupId);
                entity.setInlongStreamId(streamId);
                entity.setIsDeleted(InlongConstants.UN_DELETED);
            }
            streamFieldMapper.insertAll(list);
            LOGGER.info("success to update inlong stream field for groupId={}", groupId);
        } catch (Exception e) {
            LOGGER.error("failed to update inlong stream field: ", e);
            throw new BusinessException(ErrorCodeEnum.STREAM_FIELD_SAVE_FAILED);
        }
    }
}
