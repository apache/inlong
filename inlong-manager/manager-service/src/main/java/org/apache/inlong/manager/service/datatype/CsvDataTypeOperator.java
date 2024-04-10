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
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.pojo.stream.StreamField;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class CsvDataTypeOperator implements DataTypeOperator {

    @Override
    public boolean accept(DataTypeEnum type) {
        return DataTypeEnum.CSV.equals(type);
    }

    @Override
    public List<StreamField> parseFields(String str, InlongStreamInfo streamInfo) throws Exception {
        List<StreamField> streamFields = CommonBeanUtils.copyListProperties(streamInfo.getFieldList(),
                StreamField::new);
        try {
            char separator = '|';
            if (StringUtils.isNotBlank(streamInfo.getDataSeparator())) {
                separator = (char) Integer.parseInt(streamInfo.getDataSeparator());
            }
            String[] bodys = StringUtils.split(str, separator);
            if (bodys.length != streamFields.size()) {
                return streamFields;
            }
            for (int i = 0; i < bodys.length; i++) {
                streamFields.get(i).setFieldValue(bodys[i]);
            }
            return streamFields;
        } catch (Exception e) {
            log.warn("parse fields failed for groupId = {}, streamId = {}", streamInfo.getInlongGroupId(),
                    streamInfo.getInlongStreamId(), e);
        }
        return streamFields;
    }
}
