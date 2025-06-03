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

package org.apache.inlong.manager.service.datatype;

import org.apache.inlong.common.enums.DataTypeEnum;
import org.apache.inlong.common.pojo.sort.dataflow.dataType.DataTypeConfig;
import org.apache.inlong.common.pojo.sort.dataflow.dataType.KvConfig;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.dao.entity.InlongStreamEntity;
import org.apache.inlong.manager.pojo.consume.BriefMQMessage.FieldInfo;
import org.apache.inlong.manager.pojo.sink.SinkField;
import org.apache.inlong.manager.pojo.stream.BaseInlongStream;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.sdk.transform.decode.KvUtils;
import org.apache.inlong.sdk.transform.decode.SourceDecoderFactory;
import org.apache.inlong.sdk.transform.encode.SinkEncoderFactory;
import org.apache.inlong.sdk.transform.pojo.KvSourceInfo;
import org.apache.inlong.sdk.transform.pojo.MapSinkInfo;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;
import org.apache.inlong.sdk.transform.process.TransformProcessor;
import org.apache.inlong.sdk.transform.process.converter.TypeConverter;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.inlong.manager.pojo.stream.InlongStreamExtParam.unpackExtParams;

@Slf4j
@Service
public class KvDataTypeOperator implements DataTypeOperator {

    @Override
    public boolean accept(DataTypeEnum type) {
        return DataTypeEnum.KV.equals(type);
    }

    @Override
    public List<FieldInfo> parseFields(String str, InlongStreamInfo streamInfo) throws Exception {
        List<FieldInfo> fields = CommonBeanUtils.copyListProperties(streamInfo.getFieldList(), FieldInfo::new);
        try {
            char separator = 0;
            if (StringUtils.isNotBlank(streamInfo.getDataSeparator())) {
                separator = (char) Integer.parseInt(streamInfo.getDataSeparator());
            }
            char kvSeparator = '=';
            if (StringUtils.isNotBlank(streamInfo.getKvSeparator())) {
                kvSeparator = (char) Integer.parseInt(streamInfo.getKvSeparator());
            }
            Character escapeChar = null;
            if (StringUtils.isNotBlank(streamInfo.getDataEscapeChar())) {
                escapeChar = streamInfo.getDataEscapeChar().charAt(0);
            }
            Character lineSeparator = null;
            if (StringUtils.isNotBlank(streamInfo.getLineSeparator())) {
                lineSeparator = (char) Integer.parseInt(streamInfo.getLineSeparator());
            }
            List<Map<String, String>> rowValues =
                    KvUtils.splitKv(str, separator, kvSeparator, escapeChar, null, lineSeparator);
            for (Map<String, String> row : rowValues) {
                for (FieldInfo fieldInfo : fields) {
                    fieldInfo.setFieldValue(row.get(fieldInfo.getFieldName()));
                }
            }
        } catch (Exception e) {
            log.warn("parse fields failed for groupId = {}, streamId = {}", streamInfo.getInlongGroupId(),
                    streamInfo.getInlongStreamId(), e);
        }
        return fields;
    }

    @Override
    public DataTypeConfig getDataTypeConfig(InlongStreamInfo streamInfo) {
        char separator = 0;
        if (StringUtils.isNotBlank(streamInfo.getDataSeparator())) {
            separator = (char) Integer.parseInt(streamInfo.getDataSeparator());
        }
        Character escape = null;
        if (StringUtils.isNotBlank(streamInfo.getDataEscapeChar())) {
            escape = streamInfo.getDataEscapeChar().charAt(0);
        }
        KvConfig kvConfig = new KvConfig();
        char kvSeparator = '=';
        Character lineSeparator = null;
        if (StringUtils.isNotBlank(streamInfo.getKvSeparator())) {
            kvSeparator = (char) Integer.parseInt(streamInfo.getKvSeparator());
        }
        // row separator, which must be a field separator in the data flow
        if (StringUtils.isNotBlank(streamInfo.getLineSeparator())) {
            lineSeparator = (char) Integer.parseInt(streamInfo.getLineSeparator());
        }
        kvConfig.setLineSeparator(lineSeparator);
        kvConfig.setKvSplitter(kvSeparator);
        kvConfig.setEntrySplitter(separator);
        kvConfig.setEscapeChar(escape);
        return kvConfig;
    }

    @Override
    public Map<String, Object> parseTransform(InlongStreamEntity streamEntity, List<SinkField> fieldList,
            String transformSql,
            String data) {
        try {
            List<org.apache.inlong.sdk.transform.pojo.FieldInfo> srcFields = new ArrayList<>();
            List<org.apache.inlong.sdk.transform.pojo.FieldInfo> dstFields = new ArrayList<>();
            for (SinkField sinkField : fieldList) {
                String sourceFieldName = sinkField.getSourceFieldName();;
                String targetFieldName = sinkField.getFieldName();
                if (StringUtils.isNotBlank(sourceFieldName)) {
                    srcFields.add(
                            new org.apache.inlong.sdk.transform.pojo.FieldInfo(sourceFieldName,
                                    TypeConverter.DefaultTypeConverter()));
                }
                if (StringUtils.isNotBlank(targetFieldName)) {
                    dstFields.add(new org.apache.inlong.sdk.transform.pojo.FieldInfo(targetFieldName));
                }
            }
            char separator = '&';
            if (StringUtils.isNotBlank(streamEntity.getDataSeparator())) {
                separator = (char) Integer.parseInt(streamEntity.getDataSeparator());
            }
            Character escape = null;
            if (StringUtils.isNotBlank(streamEntity.getDataEscapeChar())) {
                escape = streamEntity.getDataEscapeChar().charAt(0);
            }
            char kvSeparator = '=';
            BaseInlongStream baseInlongStream = new BaseInlongStream();
            unpackExtParams(streamEntity.getExtParams(), baseInlongStream);
            if (StringUtils.isNotBlank(baseInlongStream.getKvSeparator())) {
                kvSeparator = (char) Integer.parseInt(baseInlongStream.getKvSeparator());
            }
            KvSourceInfo kvSourceInfo = new KvSourceInfo(streamEntity.getDataEncoding(), srcFields);
            kvSourceInfo.setEscapeChar(escape);
            kvSourceInfo.setEntryDelimiter(separator);
            kvSourceInfo.setKvDelimiter(kvSeparator);
            MapSinkInfo mapSinkInfo = new MapSinkInfo(streamEntity.getDataEncoding(), dstFields);
            TransformConfig config = new TransformConfig(transformSql);
            TransformProcessor<String, Map<String, Object>> processor = TransformProcessor
                    .create(config, SourceDecoderFactory.createKvDecoder(kvSourceInfo),
                            SinkEncoderFactory.createMapEncoder(mapSinkInfo));
            List<Map<String, Object>> result = processor.transform(data);
            log.info("success parse transform sql result={}", result);
            return result.get(0);
        } catch (Exception e) {
            log.error("parse transform sql failed", e);
            throw new BusinessException("parse transform sql failed" + e.getMessage());
        }
    }
}
