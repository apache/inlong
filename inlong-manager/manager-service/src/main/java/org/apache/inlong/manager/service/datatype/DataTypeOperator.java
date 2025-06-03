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
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.dao.entity.InlongStreamEntity;
import org.apache.inlong.manager.pojo.consume.BriefMQMessage.FieldInfo;
import org.apache.inlong.manager.pojo.sink.SinkField;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;

import java.util.List;
import java.util.Map;

/**
 * Data type operator
 */
public interface DataTypeOperator {

    /**
     * Determines whether the current instance matches the specified type.
     */
    boolean accept(DataTypeEnum type);

    /**
     * Parse fields from message
     *
     * @param streamInfo inlong stream info
     * @return list of field info
     */
    default List<FieldInfo> parseFields(String message, InlongStreamInfo streamInfo) throws Exception {
        return CommonBeanUtils.copyListProperties(streamInfo.getFieldList(), FieldInfo::new);
    }

    default DataTypeConfig getDataTypeConfig(InlongStreamInfo streamInfo) {
        throw new BusinessException(
                String.format("current type is not support for data type=%s", streamInfo.getDataType()));
    }

    default Map<String, Object> parseTransform(InlongStreamEntity streamEntity, List<SinkField> fieldList,
            String transformSql, String data) {
        throw new BusinessException(
                String.format("current type is not support for data type=%s", streamEntity.getDataType()));
    }

}
