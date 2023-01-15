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

package org.apache.inlong.manager.service.sink.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.FieldType;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.pojo.sink.SinkField;
import org.apache.inlong.manager.pojo.sink.SinkRequest;
import org.apache.inlong.manager.pojo.sink.StreamSink;
import org.apache.inlong.manager.pojo.sink.redis.RedisClusterMode;
import org.apache.inlong.manager.pojo.sink.redis.RedisColumnInfo;
import org.apache.inlong.manager.pojo.sink.redis.RedisDataType;
import org.apache.inlong.manager.pojo.sink.redis.RedisSchemaMapMode;
import org.apache.inlong.manager.pojo.sink.redis.RedisSink;
import org.apache.inlong.manager.pojo.sink.redis.RedisSinkDTO;
import org.apache.inlong.manager.pojo.sink.redis.RedisSinkRequest;
import org.apache.inlong.manager.service.sink.AbstractSinkOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

/**
 * Hudi sink operator, such as save or update hudi field, etc.
 */
@Service
public class RedisSinkOperator extends AbstractSinkOperator {

    private static final String HOODIE_PRIMARY_KEY_FIELD = "hoodie.datasource.write.recordkey.field";

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisSinkOperator.class);

    private static final String CATALOG_TYPE_HIVE = "HIVE";

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public Boolean accept(String sinkType) {
        return SinkType.REDIS.equals(sinkType);
    }

    @Override
    protected String getSinkType() {
        return SinkType.REDIS;
    }

    @Override
    protected void setTargetEntity(SinkRequest request, StreamSinkEntity targetEntity) {
        if (!this.getSinkType().equals(request.getSinkType())) {
            throw new BusinessException(ErrorCodeEnum.SINK_TYPE_NOT_SUPPORT,
                    ErrorCodeEnum.SINK_TYPE_NOT_SUPPORT.getMessage() + ": " + getSinkType());
        }
        RedisSinkRequest sinkRequest = (RedisSinkRequest) request;

        String clusterMode = sinkRequest.getClusterMode();
        RedisClusterMode redisClusterMode = RedisClusterMode.of(clusterMode);
        if (redisClusterMode == null) {
            throw new BusinessException(ErrorCodeEnum.SINK_SAVE_FAILED,
                    "Redis ClusterMode must in [" + Arrays.toString(RedisClusterMode.values()) + "] !");
        }

        switch (redisClusterMode) {
            case CLUSTER:
                String clusterNodes = sinkRequest.getClusterNodes();

            case SENTINEL:
                String sentinelMasterName = sinkRequest.getSentinelMasterName();
                String sentinelsInfo = sinkRequest.getSentinelsInfo();
            case STANDALONE:
                String host = sinkRequest.getHost();
                Integer port = sinkRequest.getPort();
        }
        RedisDataType dataType = RedisDataType.valueOf(sinkRequest.getDataType());
        if (dataType == null) {
            throw new BusinessException(ErrorCodeEnum.SINK_SAVE_FAILED,
                    "Redis DataType must not null");
        }
        RedisSchemaMapMode mapMode = RedisSchemaMapMode.valueOf(sinkRequest.getSchemaMapMode());
        if (!dataType.getMapModes().contains(mapMode)) {
            throw new BusinessException(ErrorCodeEnum.SINK_SAVE_FAILED,
                    "Redis schemaMapMode '" + mapMode + "' is not supported in '" + dataType + "'");
        }

        try {
            RedisSinkDTO dto = RedisSinkDTO.getFromRequest(sinkRequest);
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SINK_SAVE_FAILED,
                    String.format("serialize extParams of Redis SinkDTO failure: %s", e.getMessage()));
        }
    }

    @Override
    public StreamSink getFromEntity(StreamSinkEntity entity) {
        RedisSink sink = new RedisSink();
        if (entity == null) {
            return sink;
        }

        RedisSinkDTO dto = RedisSinkDTO.getFromJson(entity.getExtParams());

        CommonBeanUtils.copyProperties(entity, sink, true);
        CommonBeanUtils.copyProperties(dto, sink, true);
        List<SinkField> sinkFields = super.getSinkFields(entity.getId());
        sink.setSinkFieldList(sinkFields);
        return sink;
    }

    @Override
    protected void checkFieldInfo(SinkField field) {
        if (FieldType.forName(field.getFieldType()) == FieldType.DECIMAL) {
            RedisColumnInfo info = RedisColumnInfo.getFromJson(field.getExtParams());

        }
    }

}
